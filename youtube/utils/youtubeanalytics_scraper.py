# Import packages
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import time
import dateutil.parser as parser

""" CLASS YOUTUBE ANALYTICS"""

class Youtubean:
    def __init__(
        self,
        YOUTUBE_APIAN_SERVICE_NAME,
        YOUTUBE_APIAN_VERSION,
        YOUTUBE_APIAN_START_DATE,
        YOUTUBE_APIAN_END_DATE,
        SATKER_ID,
        credentials,
    ):
        self.YOUTUBE_APIAN_SERVICE_NAME = YOUTUBE_APIAN_SERVICE_NAME
        self.YOUTUBE_APIAN_VERSION = YOUTUBE_APIAN_VERSION
        self.YOUTUBE_APIAN_START_DATE = YOUTUBE_APIAN_START_DATE
        self.YOUTUBE_APIAN_END_DATE = YOUTUBE_APIAN_END_DATE
        self.SATKER_ID = SATKER_ID
        self.credentials = credentials
        self.buildAPI()

    def buildAPI(self):
        self.youtubeanalytics = build(
            self.YOUTUBE_APIAN_SERVICE_NAME,
            self.YOUTUBE_APIAN_VERSION,
            credentials=self.credentials,
        )

    def jsontoPandas(self, json):
        if json["rows"]:
            datas = [row for row in json["rows"]]
            headers = [header["name"] for header in json["columnHeaders"]]
            df = pd.DataFrame(datas)
            df.columns = headers
            return df
        else:
            return False

    def execute_with_retry(self, request, retries):
        for i in range(retries):
            try:
                return request.execute()
            except Exception as e:
                print("Error: ", e.args[0])
                if e.args[0] in [403, 429, 10060]:  # Rate limit error
                    wait_time = 2**i  # Tunggu 2, 4, 8 detik sebelum mencoba lagi
                    print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                    tqdm(time.sleep(wait_time))
                else:
                    raise  # Error lain, langsung hentikan

    def get_ya(self):
        res_yacontent = self.youtubeanalytics.reports().query(
            ids="channel==MINE",
            startDate=self.YOUTUBE_APIAN_START_DATE,
            endDate=self.YOUTUBE_APIAN_END_DATE,
            metrics="views,shares,likes,comments,estimatedMinutesWatched,averageViewDuration,subscribersGained",  # penayangan,total menit ditonton,rata2 durasi per penayangan(detik), subscriberyg diperoleh | estimatedMinutesWatched = (views * averageViewDuration) / 60
            dimensions="video",
            sort="-views",
            maxResults=10,
        )
        res_yadaycontent = self.youtubeanalytics.reports().query(
            ids="channel==MINE",
            startDate=self.YOUTUBE_APIAN_START_DATE,
            endDate=self.YOUTUBE_APIAN_END_DATE,
            metrics="views,shares,likes,comments,estimatedMinutesWatched,averageViewDuration,subscribersGained",
            dimensions="day",
        )
        res_yatraffic = self.youtubeanalytics.reports().query(
            ids="channel==MINE",
            startDate=self.YOUTUBE_APIAN_START_DATE,
            endDate=self.YOUTUBE_APIAN_END_DATE,
            dimensions="insightTrafficSourceType",
            metrics="views,estimatedMinutesWatched,averageViewDuration",
            sort="-views",
        )

        res_ya = {
            "res_yacontent": res_yacontent,
            "res_yadaycontent": res_yadaycontent,
            "res_yatraffic": res_yatraffic,
        }
        return res_ya

    def run_statistics(self):
        result = {}
        for key, r in self.get_ya().items():
            result[key] = self.jsontoPandas(self.execute_with_retry(r, 100))
        return result


######## ADDITIONAL FUNCTIONS #####
def get_refreshed_creds(SATKER_YATOKEN):
    print("me-refreshed credentials user..")
    creds = Credentials(
        token=SATKER_YATOKEN.get("access_token"),
        refresh_token=SATKER_YATOKEN.get("refresh_token"),
        token_uri=SATKER_YATOKEN.get("token_uri"),
        client_id=SATKER_YATOKEN.get("client_id"),
        client_secret=SATKER_YATOKEN.get("client_secret"),
        scopes=SATKER_YATOKEN.get("scopes"),
        universe_domain=SATKER_YATOKEN.get("universe_domain"),
        account=SATKER_YATOKEN.get("account"),
        expiry=(
            parser.parse(SATKER_YATOKEN.get("expiry"))
            if SATKER_YATOKEN.get("expiry")
            else None
        ),
    )
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        print("refreshed!")
    return creds


def get_creds_dict(creds):
    creds_dict = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
        "universe_domain": creds.universe_domain,
        "account": creds.account,
        "expiry": creds.expiry.isoformat() if creds.expiry else None,
    }
    return creds_dict


def df_sql(df, satker_id):
    df1 = pd.DataFrame({"satker_id": [satker_id for i in range(len(df))]})
    df2 = pd.DataFrame(
        {
            "scrapedAt": [
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for i in range(len(df))
            ]
        }
    )
    return pd.concat([df1, df, df2], axis=1).to_dict(orient="records")