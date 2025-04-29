# Import packages
from googleapiclient.discovery import build
from datetime import datetime
from tqdm import tqdm
from utils.module import convert_timezone, rmv_milisec

"""CLASS YOUTUBE"""


class Youtube:
    def __init__(
        self,
        API_KEY,
        USERNAME,
        YOUTUBE_API_SERVICE_NAME,
        YOUTUBE_API_VERSION,
        SATKER_ID,
    ):
        self.API_KEY = API_KEY
        self.USERNAME = USERNAME
        self.YOUTUBE_API_SERVICE_NAME = YOUTUBE_API_SERVICE_NAME
        self.YOUTUBE_API_VERSION = YOUTUBE_API_VERSION
        self.SATKER_ID = SATKER_ID
        self.build()
        self.get_channel_id()
        self.get_channel_info()

    # Method: Initialize
    def build(self):
        self.youtube = build(
            self.YOUTUBE_API_SERVICE_NAME,
            self.YOUTUBE_API_VERSION,
            developerKey=self.API_KEY,
        )

    # Method 1: Get channelId using the channel's username
    def get_channel_id(self):
        channel_response = (
            self.youtube.channels()
            .list(part="id,snippet,statistics", forHandle=self.USERNAME)
            .execute()
        )
        if channel_response["items"]:
            self.snippet = channel_response["items"][0]["snippet"]
            print(self.snippet)
            self.user_id = channel_response["items"][0]["id"]
        else:
            self.snippet = None
            self.user_id = None

    def get_channel_info(self):
        channel_response = (
            self.youtube.channels().list(part="statistics", id=self.user_id).execute()
        )
        if channel_response["items"]:
            self.subscribers = int(
                channel_response["items"][0]["statistics"]["subscriberCount"]
            )
        else:
            self.subscribers = None

    # Method 2: Get All Videos Id from Playlist
    def get_video_from_playlist(self):
        self.video_ids = []
        self.ids = []

        # Get All Playlist
        print("Get All Videos Id from Playlist..")
        request_playlist = self.youtube.playlists().list(
            part="snippet", channelId=self.user_id, maxResults=50
        )
        while request_playlist:
            response_playlist = request_playlist.execute()

            for playlist in tqdm(response_playlist["items"]):

                if "youtube#playlist" in playlist["kind"]:
                    playlist_id = playlist["id"]
                    # Get All Videos from current PlaylistId
                    result = (
                        self.youtube.playlistItems()
                        .list(part="snippet", playlistId=playlist_id, maxResults=50)
                        .execute()
                    )
                    while result:
                        for item in result["items"]:
                            video_id = item["snippet"]["resourceId"]["videoId"]
                            self.ids.append(video_id)
                            self.video_ids.append(
                                {
                                    "videoId": video_id,
                                    "playlistId": playlist_id,
                                    "playlistTitle": playlist["snippet"]["title"],
                                }
                            )
                        # Check if there is a next page of videos on playlistid and get the next set of results
                        if result.get("nextpageToken"):
                            result = self.youtube.playlistItems().list(
                                part="snippet",
                                playlistId=playlist_id,
                                maxResults=50,
                                pageToken=result.get("nextPageToken"),
                            )
                        else:
                            break

            # Check if there is a next page and get the next set of results
            if response_playlist.get("nextPageToken"):
                request_playlist = self.youtube.search().list(
                    part="snippet",
                    channelId=self.user_id,
                    maxResults=50,
                    order="date",
                    pageToken=response_playlist.get(
                        "nextPageToken"
                    ),  # Get next page token if it exists
                )
            else:
                break

    # Method 3: Get the list of video IDs from the channel
    def get_video_from_user(self):
        self.video_ids2 = []

        print("Get the list of video IDs from the channel...")
        request = self.youtube.search().list(
            part="snippet",
            channelId=self.user_id,
            maxResults=50,  # You can get up to 50 results per request
            order="date",  # Sort by upload date
        )
        while request:
            response = request.execute()  # trigger
            for item in response["items"]:
                if "youtube#video" in item["id"]["kind"]:
                    video_id = item["id"]["videoId"]
                    if video_id not in self.video_ids:
                        self.video_ids2.append(video_id)
            # Check if there is a next page and get the next set of results
            if response.get("nextPageToken"):
                request = self.youtube.search().list(
                    part="snippet",
                    channelId=self.user_id,
                    maxResults=50,
                    order="date",
                    pageToken=response.get(
                        "nextPageToken"
                    ),  # Get next page token if it exists
                )
            else:
                break
        print("Done.")

    # Additional Functions

    # Method 4: Get statistics (views, likes, comments) for each video
    def get_video_stats(self, video_id):

        def to_int(prop):
            return int(prop) if prop else None

        def getPublishedAt(prop):
            if prop:
                publishedAt_utc = datetime.strptime(
                    rmv_milisec(prop), "%Y-%m-%dT%H:%M:%SZ"
                )
                return convert_timezone(publishedAt_utc)
            else:
                return None

        # print(video_id)
        if type(video_id) == dict:
            videoId = video_id["videoId"]
            playlistId = video_id["playlistId"]
            playlistTitle = video_id["playlistTitle"]

        elif type(video_id) == str:
            videoId = video_id
            playlistId = None
            playlistTitle = None

        # Exeute
        vd = self.youtube.videos().list(part="snippet,statistics", id=videoId).execute()

        if vd["items"]:
            channelId = vd["items"][0]["snippet"].get("channelId")
            if channelId == self.user_id:
                return {
                    "satker_id": self.SATKER_ID,
                    "videoId": videoId,
                    "playlistId": playlistId,
                    "playlistTitle": playlistTitle,
                    "title": vd["items"][0]["snippet"].get("title"),
                    "publishedAt": getPublishedAt(
                        vd["items"][0]["snippet"].get("publishedAt")
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    "viewCount": to_int(vd["items"][0]["statistics"].get("viewCount")),
                    "likeCount": to_int(vd["items"][0]["statistics"].get("likeCount")),
                    "favoriteCount": to_int(
                        vd["items"][0]["statistics"].get("favoriteCount")
                    ),
                    "commentCount": to_int(
                        vd["items"][0]["statistics"].get("commentCount")
                    ),
                    "scrapedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

    # Method : Run!
    def run_statistics(self):

        print("Get statistics (views, likes, comments) for each video..")
        details = []

        # Run Method 2:
        self.get_video_from_playlist()
        for id in tqdm(self.video_ids):  # update-here!
            # print(id)
            stat = self.get_video_stats(id)
            if stat:
                details.append(stat)

        # Run Method 3:
        self.get_video_from_user()
        for id2 in tqdm(self.video_ids2):  # update-here!
            if id2 not in self.ids:
                stat2 = self.get_video_stats(id2)
                if stat2:
                    details.append(stat2)

        return [details, self.user_id, self.subscribers, self.snippet]
