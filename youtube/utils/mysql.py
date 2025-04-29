# Import packages
from datetime import datetime
import mysql.connector

""" " MYSQL DB CONNECTION"""

class MySQLDB:
    def __init__(self, HOST, PORT, USER, PASSWORD, DATABASE):
        self.HOST = HOST
        self.PORT = PORT
        self.USER = USER
        self.PASSWORD = PASSWORD
        self.DATABASE = DATABASE
        self.build()

    # Method: Initialize
    def build(self):
        self.MYDB = mysql.connector.connect(
            host=self.HOST,
            port=self.PORT,
            user=self.USER,
            password=self.PASSWORD,
            database=self.DATABASE,
            charset="utf8mb4",
        )
        print(self.MYDB)
        self.MYCURSOR = self.MYDB.cursor()

    # Get Last Unscraped Satker
    def get_satkers(self):
        self.MYCURSOR.execute(
            """
                SELECT satkers.satker_id, satkers.username_youtube, satkers.yatoken, A.scrapedAt FROM satkers
                LEFT JOIN (
                SELECT satker_id, MAX(DATE_FORMAT(yacontents.scrapedAt, '%M %d %Y %H:%i:%S')) as scrapedAt FROM yacontents
                GROUP BY yacontents.satker_id
                ) A
                ON A.satker_id = satkers.satker_id
                ORDER BY A.scrapedAt ASC
            """
        )
        return self.MYCURSOR.fetchall()

    # ============================== YOUTUBES GENERAL METHOD ========================================
    def get_apis(self):
        self.MYCURSOR.execute(
            """
                SELECT apiyoutubes.apikey, DATE_FORMAT(apiyoutubes.last_used, '%M %d %Y %H:%i:%S') as last_used
                FROM apiyoutubes
                ORDER BY last_used ASC
            """
        )
        return self.MYCURSOR.fetchall()

    # update channel_id and subscribers
    def update_channelId_subscribers(self, satker_id, channel_id, subscribers):
        sql = "UPDATE satkers SET channel_id=%s, subscribers=%s WHERE satker_id=%s"
        self.MYCURSOR.execute(sql, (channel_id, subscribers, satker_id))
        self.MYDB.commit()

    def update_snippet(self, satker_id, snippet_json):
        sql = "UPDATE satkers SET snippet=%s WHERE satker_id=%s"
        self.MYCURSOR.execute(sql, (snippet_json, satker_id))
        self.MYDB.commit()

    # If success, then delete data
    def delete_last_scraped(self, satker_id):
        sql = "DELETE FROM youtubes WHERE satker_id = %s"
        self.MYCURSOR.execute(sql, (satker_id,))
        self.MYDB.commit()

    # Then, insert new data
    def insert_new_scraped(self, row):
        sql = """INSERT INTO youtubes (`satker_id`,`videoId`, `playlistId`, 
                    `playlistTitle`, `title`, `publishedAt`, 
                    `viewCount`, `likeCount`, `favoriteCount`,
                    `commentCount`, `scrapedAt`)
                    VALUES (%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,
                    %s,%s)
                """
        self.MYCURSOR.execute(
            sql,
            (
                row["satker_id"],
                row["videoId"],
                row["playlistId"],
                row["playlistTitle"],
                row["title"],
                row["publishedAt"],
                (row["viewCount"] if row["viewCount"] else 0),
                (row["likeCount"] if row["likeCount"] else 0),
                (row["favoriteCount"] if row["favoriteCount"] else 0),
                (row["commentCount"] if row["commentCount"] else 0),
                row["scrapedAt"],
            ),
        )
        self.MYDB.commit()

    # then, update the api last_used
    def update_api_last_used(self, apikey):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "UPDATE apiyoutubes SET last_used=%s WHERE apikey=%s"
        self.MYCURSOR.execute(sql, (now, apikey))
        self.MYDB.commit()

    # ============================== END YOUTUBES GENERAL METHOD ========================================

    # ============================== YOUTUBES ANALYTICS METHOD========================================
    # refresh satkers' token
    def refresh_satkers_token(self, satker_id, creds_json):
        sql = "UPDATE satkers SET yatoken=%s WHERE satker_id=%s"
        self.MYCURSOR.execute(sql, (creds_json, satker_id))
        self.MYDB.commit()

    # YACONTENTS
    # If success, then delete data
    def delete_last_an_yacontents_scraped(self, satker_id):
        sql = "DELETE FROM yacontents WHERE satker_id = %s"
        self.MYCURSOR.execute(sql, (satker_id,))
        self.MYDB.commit()

    # Then, insert new data
    def insert_new_an_yacontents_scraped(self, row):
        self.MYCURSOR.execute(
            """
                    INSERT INTO yacontents (`satker_id`, `video`, `views`,`shares`,
                                            `likes`, `comments`,`estimatedMinutesWatched`,
                                            `averageViewDuration`,`subscribersGained`,`scrapedAt`)
                    VALUES (%s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s)
                """,
            (
                row["satker_id"],
                row["video"],
                (row["views"] if row["views"] else 0),
                (row["shares"] if row["shares"] else 0),
                (row["likes"] if row["likes"] else 0),
                (row["comments"] if row["comments"] else 0),
                (
                    row["estimatedMinutesWatched"]
                    if row["estimatedMinutesWatched"]
                    else 0
                ),
                (row["averageViewDuration"] if row["averageViewDuration"] else 0),
                (row["subscribersGained"] if row["subscribersGained"] else 0),
                row["scrapedAt"],
            ),
        )
        self.MYDB.commit()

    # YACONTENTDAYS
    # If success, then delete data
    def delete_last_an_yadaycontents_scraped(self, satker_id):
        sql = "DELETE FROM yadaycontents WHERE satker_id = %s"
        self.MYCURSOR.execute(sql, (satker_id,))
        self.MYDB.commit()

    # Then, insert new data
    def insert_new_an_yadaycontents_scraped(self, row):
        self.MYCURSOR.execute(
            """
                    INSERT INTO yadaycontents (`satker_id`, `day`, `views`,`shares`,
                                            `likes`, `comments`,`estimatedMinutesWatched`,
                                            `averageViewDuration`,`subscribersGained`,`scrapedAt`)
                    VALUES (%s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s)
                """,
            (
                row["satker_id"],
                row["day"],
                (row["views"] if row["views"] else 0),
                (row["shares"] if row["shares"] else 0),
                (row["likes"] if row["likes"] else 0),
                (row["comments"] if row["comments"] else 0),
                (
                    row["estimatedMinutesWatched"]
                    if row["estimatedMinutesWatched"]
                    else 0
                ),
                (row["averageViewDuration"] if row["averageViewDuration"] else 0),
                (row["subscribersGained"] if row["subscribersGained"] else 0),
                row["scrapedAt"],
            ),
        )
        self.MYDB.commit()

    # YATRAFFICS
    # If success, then delete data
    def delete_last_an_yatraffics_scraped(self, satker_id):
        sql = "DELETE FROM yatraffics WHERE satker_id = %s"
        self.MYCURSOR.execute(sql, (satker_id,))
        self.MYDB.commit()

    # Then, insert new data
    def insert_new_an_yatraffics_scraped(self, row):
        sql = """
                    INSERT INTO yatraffics (`satker_id`, `insightTrafficSourceType`, `views`,`estimatedMinutesWatched`,
                                            `averageViewDuration`,`scrapedAt`)
                    VALUES (%s,%s,%s,%s,
                    %s,%s)
                """
        self.MYCURSOR.execute(
            sql,
            (
                row["satker_id"],
                row["insightTrafficSourceType"],
                (row["views"] if row["views"] else 0),
                (
                    row["estimatedMinutesWatched"]
                    if row["estimatedMinutesWatched"]
                    else 0
                ),
                (row["averageViewDuration"] if row["averageViewDuration"] else 0),
                row["scrapedAt"],
            ),
        )
        self.MYDB.commit()