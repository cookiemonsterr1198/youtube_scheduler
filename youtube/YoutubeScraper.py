# Import packages
from datetime import datetime
import json
from config import settings
from utils.mysql import MySQLDB
from utils.youtube_scraper import Youtube
from utils.module import *
from utils.youtubeanalytics_scraper import Youtubean, get_refreshed_creds, get_creds_dict, df_sql

""" PARAMETERS """
HOST = settings.HOST
PORT = settings.PORT
USER = settings.USER
PASSWORD = settings.PASSWORD
DATABASE = settings.DATABASE

YOUTUBE_API_SERVICE_NAME = settings.YOUTUBE_API_SERVICE_NAME
YOUTUBE_API_VERSION = settings.YOUTUBE_API_VERSION

YOUTUBE_APIAN_HOST = settings.YOUTUBE_APIAN_HOST
YOUTUBE_APIAN_PORT = settings.YOUTUBE_APIAN_PORT
YOUTUBE_APIAN_SERVICE_NAME = settings.YOUTUBE_APIAN_SERVICE_NAME
YOUTUBE_APIAN_VERSION = settings.YOUTUBE_APIAN_VERSION

SCOPES = settings.SCOPES


""" "!! RUN !!"""

mysqldb = MySQLDB(HOST, PORT, USER, PASSWORD, DATABASE)
satkers = mysqldb.get_satkers()
apiyoutubes = mysqldb.get_apis()
# apianyoutubes = mysqldb.get_apians()

for satker in satkers:  # hanya bpssumut testing
    SATKER_ID = satker[0]
    SATKER_USERNAME = satker[1]
    SATKER_YATOKEN = satker[2]
    SUCCESS = False
    FIRST_UPLOADED = None
    LAST_UPLOADED = None

    ##### GENERAL SECTION
    if SATKER_USERNAME and SATKER_USERNAME != "":
        print(
            f"Get ready scraping satker :{SATKER_ID} with username :{SATKER_USERNAME}..."
        )
        print(f"Gathering general information: {SATKER_USERNAME}..")
        for apiyoutube in apiyoutubes:
            API_KEY = apiyoutube[0]
            try:
                # Initialize Youtube
                youtube = Youtube(
                    API_KEY,
                    SATKER_USERNAME,
                    YOUTUBE_API_SERVICE_NAME,
                    YOUTUBE_API_VERSION,
                    SATKER_ID,
                )

                data = youtube.run_statistics()
                print(data)
                FIRST_UPLOADED = convert_timezone(
                    datetime.strptime(rmv_milisec(data[3]["publishedAt"]), "%Y-%m-%dT%H:%M:%SZ")
                ).strftime("%Y-%m-%d")
                LAST_UPLOADED = datetime.now().strftime("%Y-%m-%d")
                ### SAVE DATA GENERAL
                # save channel_id & subscribers
                mysqldb.update_channelId_subscribers(SATKER_ID, data[1], data[2])
                mysqldb.update_snippet(SATKER_ID, json.dumps(data[3]))
                # If success, then Delete data
                mysqldb.delete_last_scraped(satker_id=SATKER_ID)
                # Then, insert new data and update the last_used API
                for row in data[0]:
                    mysqldb.insert_new_scraped(row)
                # update api key last_used apiyoutube
                mysqldb.update_api_last_used(apikey=API_KEY)
                ### END SAVE DATA GENERAL
                print("Updating data general success.\n Process Finished.")

                SUCCESS = True

                break
            except Exception as e:
                print(f"Error!! :{e}\n Trying again with another api..")

        if SUCCESS:
            #### ANALYTICS SECTION
            print(f"Gathering analytics information: {SATKER_USERNAME}..")
            # for apianyoutube in apianyoutubes:
            if SATKER_YATOKEN and SATKER_YATOKEN != "":
                SATKER_YATOKEN = json.loads(SATKER_YATOKEN)
                creds = get_refreshed_creds(SATKER_YATOKEN=SATKER_YATOKEN)
                # After retrieving creds, try scraping analytics:
                try:
                    # Initialize Youtube Analytics
                    youtubean = Youtubean(
                        YOUTUBE_APIAN_SERVICE_NAME,
                        YOUTUBE_APIAN_VERSION,
                        FIRST_UPLOADED,
                        LAST_UPLOADED,
                        SATKER_ID,
                        creds,
                    )
                    data_an = youtubean.run_statistics()
                    print(data_an)

                    # Convert credentials to dictionary
                    creds_dict = get_creds_dict(creds=creds)
                    ### SAVE DATA ANALYTICS
                    mysqldb.refresh_satkers_token(
                        satker_id=SATKER_ID, creds_json=json.dumps(creds_dict)
                    )
                    print("token saved.")
                    # If success, then Delete data
                    # Then, insert new data and update the last_used APIAN
                    print(SATKER_ID)
                    for key, res_an in data_an.items():
                        df_an = df_sql(df=res_an, satker_id=SATKER_ID)

                        if key == "res_yacontent":
                            # Membaca row tabel df_an
                            mysqldb.delete_last_an_yacontents_scraped(
                                satker_id=SATKER_ID
                            )
                            for row in df_an:
                                mysqldb.insert_new_an_yacontents_scraped(row=row)
                        elif key == "res_yadaycontent":
                            mysqldb.delete_last_an_yadaycontents_scraped(
                                satker_id=SATKER_ID
                            )
                            for row in df_an:
                                mysqldb.insert_new_an_yadaycontents_scraped(row=row)
                        elif key == "res_yatraffic":
                            mysqldb.delete_last_an_yatraffics_scraped(
                                satker_id=SATKER_ID
                            )
                            for row in df_an:
                                mysqldb.insert_new_an_yatraffics_scraped(row=row)
                    ### END SAVE DATA ANALYTICS

                    print("Updating data analytis success.\nProcess Finished.")
                except Exception as ex:
                    print(f"Error Analytics!! :{ex}\nTrying again with another api..")
    else:
        print(
            f"Username for satker :{SATKER_ID} is unspecified. Trying another satker.."
        )
print("DONE.")

