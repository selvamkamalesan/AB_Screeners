import logging

import pandas as pd

from modules.processors.parser import CMOTS_API_Request
from modules.processors.database import AB_Screener_DB
from modules.libraries import *
from modules.utilities.redis import save_redis

logging.basicConfig(filename=Props.APP_LOG + datetime.now().strftime("%d-%m-%Y") + '.log', filemode='a',format='%(asctime)s -%(name)s - %(levelname)s - %(message)s', datefmt='%d-%m-%Y %H:%M:%S',level=logging.INFO)

def contract_download():
    file_date = datetime.now().strftime("%d%b%Y").upper()
    # yesterday_date = datetime.now() - timedelta(days=Props.BHAVCOPY_DAYS)
    yesterday_date = datetime.strptime("2024-04-28","%Y-%m-%d")
    if yesterday_date.strftime("%A") == "Saturday":
        yesterday_date = yesterday_date - timedelta(days=1)

    if yesterday_date.strftime("%A") == "Sunday":
        yesterday_date = yesterday_date - timedelta(days=2)

    yesterday_file_date = yesterday_date.strftime("%d%b%Y").upper()
    yesterday_file_month = yesterday_date.strftime("%b").upper()
    yesterday_file_year = yesterday_date.strftime('%Y')

    path_to_file_contract = Props.CONTRACT_SAVE_PATH
    path_to_file_bhav = Props.BHAVCOPY_SAVE_PATH + '/zip/%s_NFO_BHAV_COPY.csv.zip' % yesterday_file_date

    cm_url = 'https://v2api.aliceblueonline.com/restpy/static/contract_master/'
    nfo_bhav_url = 'https://archives.nseindia.com/content/historical/DERIVATIVES/'

    try:
        CM_Base_list = ['NFO','MCX']
        for Base_list in CM_Base_list:
            file_name = Base_list
            path_contract = Path(path_to_file_contract + '%s_%s_FO.csv' % (file_date, Base_list))
            print('Contract Master --> %s_%s_FO.csv' % (file_date, Base_list))
            print(path_contract)
            if path_contract.is_file():
                print("                |-> File Already Exist")
                pass
            else:
                url = cm_url + '%s.csv' % file_name
                CM_files = requests.get(url, allow_redirects=True, timeout=60)
                if CM_files.status_code == 200:
                    with open(Props.CONTRACT_SAVE_PATH + '%s_%s_FO.csv' % (file_date, Base_list), 'wb') as f:
                        f.write(CM_files.content)
                    df = pd.read_csv(Props.CONTRACT_SAVE_PATH + '%s_%s_FO.csv' % (file_date, Base_list))
                    df = df.rename(
                        columns={'Exch': 'exch', 'Exchange Segment': 'exchange_segment', 'Symbol': 'symbol',
                                 'Token': 'token',
                                 'Instrument Type': 'instrument_type', 'Option Type': 'option_type',
                                 'Strike Price': 'strike_price',
                                 'Instrument Name': 'instrument_name', 'Formatted Ins Name': 'formatted_ins_name',
                                 'Trading Symbol': 'trading_symbol', 'Expiry Date': 'expiry_date',
                                 'Lot Size': 'lot_size',
                                 'Tick Size': 'tick_size'})
                    df.to_csv(Props.CONTRACT_SAVE_PATH + '%s_%s_FO.csv' % (file_date, Base_list))
                    print(
                        "                |->  File download process completed")
                    time.sleep(1)
                    logging.info("Contract Download Successfully")
                else:
                    print("                |->  Error in file download " + Base_list + " - " + str(CM_files.status_code) + " - " + CM_files.reason)
                    logging.error("Error in file download " + Base_list + " - " + str(CM_files.status_code) + " - " + CM_files.reason)
        print(
            "------------------------------------- Contract Download Process Completed "
            "-----------------------------------"
        )
    except Exception as e:
        print("Contract Master Download Error : %s" % e)
        logging.error("Contract Master Download Error : %s"%str(e))
        pass

    try:
        print("NFO_BHAV_FILE --> fo%sbhav.csv.zip" % yesterday_file_date)
        if Path(path_to_file_bhav).is_file():
            print("              |-> File Already Exist")
            pass
        else:
            url = nfo_bhav_url + '%s/%s/fo%sbhav.csv.zip' % (yesterday_file_year, yesterday_file_month, yesterday_file_date)
            r = requests.get(url, allow_redirects=True, timeout=60)
            open(path_to_file_bhav, 'wb').write(r.content)

            with zipfile.ZipFile(path_to_file_bhav, 'r') as zip_ref:
                zip_ref.extractall(Props.BHAVCOPY_SAVE_PATH)
            print(
                "              |->  File download process completed")
            logging.info("Bhav Download Successfully")
        print(
            "------------------------------------ NFO Bhav File Download Process Completed "
            "-----------------------------------")
    except Exception as e:
        print("Bhav Download Error : %s" % e)
        logging.error("Bhav Download Error : %s"%str(e))
        pass

def Highest_OI_FUT():
    # Get CMOTS API Request
    response_data  = CMOTS_API_Request(JWT=True,Endpoint=Props.CMOTS_ENDPOINT_HIGHEST_OI_FUT)
    try:
        if response_data["stat"] == 'ok':

            HOI_FUT = response_data["data"]["data"]
            DF_HOI_FUT = pd.DataFrame(HOI_FUT)
            DF_HOI_FUT["symbol"] = DF_HOI_FUT["symbol"].str.strip()
            DF_HOI_FUT["expdate"] = DF_HOI_FUT["expdate"].astype('datetime64[ns]')
            DF_HOI_FUT["updtime"] = DF_HOI_FUT["updtime"].astype('datetime64[ns]')

            DF_HOI_FUT["script_identifier"] = DF_HOI_FUT["symbol"]+DF_HOI_FUT["expdate"].dt.strftime('%y%b%d').str.upper()+"FUT"
            DF_HOI_FUT["created_on"] = datetime.now()
            # DB Insert Process
            AB_Screener_DB().insert_query(df=DF_HOI_FUT,table_name=Props.DB_TBL_HIGH_OI_FUT)

            redis_key = "HIGHEST:OI:FUT"
            save_redis(redis_key, DF_HOI_FUT.to_json(orient='records'))
            print("Completed Highest_OI_FUT -",datetime.now())
            logging.info("Completed Highest_OI_FUT")
    except:
        logging.error("HIGHEST OI FUT :"+str(response_data))

def Highest_OI_OPT():
    # Get CMOTS API Request
    response_data  = CMOTS_API_Request(JWT=True,Endpoint=Props.CMOTS_ENDPOINT_HIGHEST_OI_OPT)
    try:
        if response_data["stat"] == 'ok':
            HOI_OPT = response_data["data"]["data"]
            DF_HOI_OPT = pd.DataFrame(HOI_OPT)

            DF_HOI_OPT["symbol"] = DF_HOI_OPT["symbol"].str.strip()
            DF_HOI_OPT["expdate"] = DF_HOI_OPT["expdate"].astype('datetime64[ns]')
            DF_HOI_OPT["updtime"] = DF_HOI_OPT["updtime"].astype('datetime64[ns]')
            DF_HOI_OPT["strikeprice"] = DF_HOI_OPT["strikeprice"].astype(int).astype(str)

            DF_HOI_OPT["script_identifier"] = DF_HOI_OPT["symbol"]+DF_HOI_OPT["expdate"].astype('datetime64[ns]').dt.strftime('%y%b%d').str.upper()+DF_HOI_OPT["opttype"]+DF_HOI_OPT["strikeprice"]
            DF_HOI_OPT["created_on"] = datetime.now()
            # DB Insert Process
            AB_Screener_DB().insert_query(df=DF_HOI_OPT,table_name=Props.DB_TBL_HIGH_OI_OPT)

            redis_key = "HIGHEST:OI:OPT"
            save_redis(redis_key, DF_HOI_OPT.to_json(orient='records'))
            print("Completed Highest_OI_OPT -", datetime.now())
            logging.info("Completed Highest_OI_OPT")
    except:
        logging.error("HIGHEST OI OPT :"+str(response_data))

def Gainer_OI_FUT():
    # Get CMOTS API Request
    response_data  = CMOTS_API_Request(JWT=True,Endpoint=Props.CMOTS_ENDPOINT_GAINER_OI_FUT)
    try:
        if response_data["stat"] == 'ok':
            GOI_FUT = response_data["data"]["data"]
            DF_GOI_FUT = pd.DataFrame(GOI_FUT)
            DF_GOI_FUT["symbol"] = DF_GOI_FUT["symbol"].str.strip()
            DF_GOI_FUT["expdate"] = DF_GOI_FUT["expdate"].astype('datetime64[ns]')
            DF_GOI_FUT["updtime"] = DF_GOI_FUT["updtime"].astype('datetime64[ns]')

            DF_GOI_FUT["script_identifier"] = DF_GOI_FUT["symbol"]+DF_GOI_FUT["expdate"].dt.strftime('%y%b%d').str.upper()+"FUT"
            DF_GOI_FUT["created_on"] = datetime.now()
            # DB Insert Process
            AB_Screener_DB().insert_query(df=DF_GOI_FUT,table_name=Props.DB_TBL_GAIN_OI_FUT)

            redis_key = "GAINER:OI:FUT"
            save_redis(redis_key, DF_GOI_FUT.to_json(orient='records'))
            print("Completed Gainer_OI_FUT -", datetime.now())
            logging.info("Completed Gainer_OI_FUT")
    except:
        logging.error("GAINER OI FUT :"+str(response_data))

def Gainer_OI_OPT():
    # Get CMOTS API Request
    response_data  = CMOTS_API_Request(JWT=True,Endpoint=Props.CMOTS_ENDPOINT_GAINER_OI_OPT)
    try:
        if response_data["stat"] == 'ok':
            GOI_OPT = response_data["data"]["data"]
            DF_GOI_OPT = pd.DataFrame(GOI_OPT)

            DF_GOI_OPT["symbol"] = DF_GOI_OPT["symbol"].str.strip()
            DF_GOI_OPT["expdate"] = DF_GOI_OPT["expdate"].astype('datetime64[ns]')
            DF_GOI_OPT["updtime"] = DF_GOI_OPT["updtime"].astype('datetime64[ns]')
            DF_GOI_OPT["strikeprice"] = DF_GOI_OPT["strikeprice"].astype(int).astype(str)

            DF_GOI_OPT["script_identifier"] = DF_GOI_OPT["symbol"]+DF_GOI_OPT["expdate"].astype('datetime64[ns]').dt.strftime('%y%b%d').str.upper()+DF_GOI_OPT["opttype"]+DF_GOI_OPT["strikeprice"]
            DF_GOI_OPT["created_on"] = datetime.now()
            # DB Insert Process
            AB_Screener_DB().insert_query(df=DF_GOI_OPT,table_name=Props.DB_TBL_GAIN_OI_OPT)

            redis_key = "GAINER:OI:OPT"
            save_redis(redis_key, DF_GOI_OPT.to_json(orient='records'))
            print("Completed Gainer_OI_OPT -", datetime.now())
            logging.info("Completed Gainer_OI_OPT")
    except:
        logging.error("GAINER OI OPT :"+str(response_data))

def Looser_OI_FUT():
    # Get CMOTS API Request
    response_data = CMOTS_API_Request(JWT=True,Endpoint=Props.CMOTS_ENDPOINT_LOOSER_OI_FUT)
    try:
        if response_data["stat"] == 'ok':
            LOI_FUT = response_data["data"]["data"]
            DF_LOI_FUT = pd.DataFrame(LOI_FUT)

            DF_LOI_FUT["symbol"] = DF_LOI_FUT["symbol"].str.strip()
            DF_LOI_FUT["expdate"] = DF_LOI_FUT["expdate"].astype('datetime64[ns]')
            DF_LOI_FUT["updtime"] = DF_LOI_FUT["updtime"].astype('datetime64[ns]')

            DF_LOI_FUT["script_identifier"] = DF_LOI_FUT["symbol"]+DF_LOI_FUT["expdate"].dt.strftime('%y%b%d').str.upper()+"FUT"
            DF_LOI_FUT["created_on"] = datetime.now()
            # DB Insert Process
            AB_Screener_DB().insert_query(df=DF_LOI_FUT,table_name=Props.DB_TBL_LOSS_OI_FUT)

            redis_key = "LOOSER:OI:FUT"
            save_redis(redis_key, DF_LOI_FUT.to_json(orient='records'))
            print("Completed Looser_OI_FUT -", datetime.now())
            logging.info("Completed Looser_OI_FUT")
    except:
        logging.error("LOOSER OI FUT :"+str(response_data))

def Looser_OI_OPT():
    # Get CMOTS API Request
    response_data  = CMOTS_API_Request(JWT=True,Endpoint=Props.CMOTS_ENDPOINT_LOOSER_OI_OPT)
    try:
        if response_data["stat"] == 'ok':
            LOI_OPT = response_data["data"]["data"]
            DF_LOI_OPT = pd.DataFrame(LOI_OPT)

            DF_LOI_OPT["symbol"] = DF_LOI_OPT["symbol"].str.strip()
            DF_LOI_OPT["expdate"] = DF_LOI_OPT["expdate"].astype('datetime64[ns]')
            DF_LOI_OPT["updtime"] = DF_LOI_OPT["updtime"].astype('datetime64[ns]')
            DF_LOI_OPT["strikeprice"] = DF_LOI_OPT["strikeprice"].astype(int).astype(str)

            DF_LOI_OPT["script_identifier"] = DF_LOI_OPT["symbol"]+DF_LOI_OPT["expdate"].astype('datetime64[ns]').dt.strftime('%y%b%d').str.upper()+DF_LOI_OPT["opttype"]+DF_LOI_OPT["strikeprice"]
            DF_LOI_OPT["created_on"] = datetime.now()
            # DB Insert Process
            AB_Screener_DB().insert_query(df=DF_LOI_OPT,table_name=Props.DB_TBL_LOSS_OI_OPT)

            redis_key = "LOOSER:OI:OPT"
            save_redis(redis_key, DF_LOI_OPT.to_json(orient='records'))
            print("Completed Looser_OI_OPT -", datetime.now())
            logging.info("Completed Looser_OI_OPT")
    except:
        logging.error("LOOSER OI OPT :"+str(response_data))

def Highest_Volume_FUT():
    # Get CMOTS API Request
    response_data_idx = CMOTS_API_Request(JWT=True,Endpoint=Props.CMOTS_ENDPOINT_HIGHER_VOLUME+"/FUTIDX/100")
    response_data_stk = CMOTS_API_Request(JWT=True, Endpoint=Props.CMOTS_ENDPOINT_HIGHER_VOLUME+"/FUTSTK/100")
    try:
        if response_data_idx["stat"] == 'ok' and response_data_stk["stat"] == 'ok' :

            HOV_FUT_IDX = response_data_idx["data"]["data"]
            HOV_FUT_STK = response_data_stk["data"]["data"]

            DF_HOV_FUT_IDX = pd.DataFrame(HOV_FUT_IDX)
            DF_HOV_FUT_STK = pd.DataFrame(HOV_FUT_STK)

            frame = [DF_HOV_FUT_IDX,DF_HOV_FUT_STK]
            DF_HOV_FUT = pd.concat(frame)

            DF_HOV_FUT["symbol"] = DF_HOV_FUT["symbol"].str.strip()
            DF_HOV_FUT["expdate"] = DF_HOV_FUT["expdate"].astype('datetime64[ns]')
            DF_HOV_FUT["updtime"] = DF_HOV_FUT["updtime"].astype('datetime64[ns]')

            DF_HOV_FUT["script_identifier"] = DF_HOV_FUT["symbol"]+DF_HOV_FUT["expdate"].dt.strftime('%y%b%d').str.upper()+"FUT"
            DF_HOV_FUT["created_on"] = datetime.now()
            DF_HOV_FUT.rename(columns = {'faochg':'faochange','oi':'openinterest','oichg':'chgopenint'}, inplace = True)
            # DB Insert Process
            AB_Screener_DB().insert_query(df=DF_HOV_FUT,table_name=Props.DB_TBL_HIGH_VOL_FUT)

            redis_key = "HIGHEST:VOL:FUT"
            save_redis(redis_key, DF_HOV_FUT.to_json(orient='records'))
            print("Completed Highest_Volume_FUT -",datetime.now())
            logging.info("Completed Highest_Volume_FUT")
    except:
        print("Exception Error!!!")
        logging.error("Highest_Volume_FUT IDX :" + str(response_data_idx))
        logging.error("Highest_Volume_FUT STK :" + str(response_data_stk))

def Highest_Volume_OPT():
    # Get CMOTS API Request
    response_data_idx = CMOTS_API_Request(JWT=True,Endpoint=Props.CMOTS_ENDPOINT_HIGHER_VOLUME+"/OPTIDX/1000")
    response_data_stk = CMOTS_API_Request(JWT=True, Endpoint=Props.CMOTS_ENDPOINT_HIGHER_VOLUME+"/OPTSTK/1000")
    try:
        if response_data_idx["stat"] == 'ok' and response_data_stk["stat"] == 'ok' :
            HOV_OPT_IDX = response_data_idx["data"]["data"]
            HOV_OPT_STK = response_data_stk["data"]["data"]

            DF_HOV_OPT_IDX = pd.DataFrame(HOV_OPT_IDX)
            DF_HOV_OPT_STK = pd.DataFrame(HOV_OPT_STK)

            frame = [DF_HOV_OPT_IDX,DF_HOV_OPT_STK]
            DF_HOV_OPT = pd.concat(frame)


            DF_HOV_OPT["symbol"] = DF_HOV_OPT["symbol"].str.strip()
            DF_HOV_OPT["expdate"] = DF_HOV_OPT["expdate"].astype('datetime64[ns]')
            DF_HOV_OPT["updtime"] = DF_HOV_OPT["updtime"].astype('datetime64[ns]')
            DF_HOV_OPT["strikeprice"] = DF_HOV_OPT["strikeprice"].astype(int).astype(str)

            DF_HOV_OPT["script_identifier"] = DF_HOV_OPT["symbol"]+DF_HOV_OPT["expdate"].astype('datetime64[ns]').dt.strftime('%y%b%d').str.upper()+DF_HOV_OPT["opttype"]+DF_HOV_OPT["strikeprice"]
            DF_HOV_OPT["created_on"] = datetime.now()
            # DB Insert Process
            DF_HOV_OPT.rename(columns={'faochg': 'faochange', 'oi': 'openinterest', 'oichg': 'chgopenint'}, inplace=True)
            AB_Screener_DB().insert_query(df=DF_HOV_OPT,table_name=Props.DB_TBL_HIGH_VOL_OPT)

            redis_key = "HIGHEST:VOL:OPT"
            save_redis(redis_key, DF_HOV_OPT.to_json(orient='records'))
            print("Completed Highest_OI_OPT -", datetime.now())
            logging.info("Completed Highest_OI_OPT")
    except:
        print("Exception Error!!!")
        logging.error("Highest_Volume_OPT IDX :" + str(response_data_idx))
        logging.error("Highest_Volume_OPT STK :" + str(response_data_stk))

def Highest_Volume_COM():
    response_data = CMOTS_API_Request(JWT=True,Endpoint=Props.CMOTS_ENDPOINT_HIGHER_VOLUME+"/mcx/topvol/-/20/-")
    try:
        if response_data["stat"] == 'ok' and response_data["stat"] == 'ok':
            HOV_COM = response_data["data"]["data"]
            DF_HOV_COM = pd.DataFrame(HOV_COM)

            pd.set_option('display.max_columns', None)
            print(DF_HOV_COM)
    except:
        print("Exception Error!!!")
        logging.error("Highest_Volume_COM :" + str(response_data))

def Gainer_MCX():
    # Get CMOTS API Request
    response_data  = CMOTS_API_Request(JWT=True,Endpoint=Props.CMOTS_ENDPOINT_MCX_GAINER)

    if response_data["stat"] == 'ok':

        HOI_FUT = response_data["data"]["data"]
        DF_HOI_FUT = pd.DataFrame(HOI_FUT)
        DF_HOI_FUT["symbol"] = DF_HOI_FUT["symbol"].str.strip()
        DF_HOI_FUT["exp_date"] = DF_HOI_FUT["exp_date"].astype('datetime64[ns]')
        DF_HOI_FUT["trd_date"] = DF_HOI_FUT["trd_date"].astype('datetime64[ns]')

        DF_HOI_FUT["script_identifier"] = DF_HOI_FUT["symbol"]+DF_HOI_FUT["exp_date"].dt.strftime('%y%b%d').str.upper()+"FUT"
        DF_HOI_FUT["created_on"] = datetime.now()
        # print(DF_HOI_FUT)
        # DB Insert Process
        AB_Screener_DB().insert_query_mcx(df=DF_HOI_FUT,table_name=Props.DB_TBL_GAINER_MCX)

        redis_key = "HIGHEST:GAIN:MCX"
        save_redis(redis_key, DF_HOI_FUT.to_json(orient='records'))
        print("Completed Highest_OI_FUT -",datetime.now())
        logging.info("Completed Highest_OI_FUT")

def Loser_MCX():
    # Get CMOTS API Request
    response_data  = CMOTS_API_Request(JWT=True,Endpoint=Props.CMOTS_ENDPOINT_MCX_LOSER)

    if response_data["stat"] == 'ok':

        HOI_FUT = response_data["data"]["data"]
        DF_HOI_FUT = pd.DataFrame(HOI_FUT)
        DF_HOI_FUT["symbol"] = DF_HOI_FUT["symbol"].str.strip()
        DF_HOI_FUT["exp_date"] = DF_HOI_FUT["exp_date"].astype('datetime64[ns]')
        DF_HOI_FUT["trd_date"] = DF_HOI_FUT["trd_date"].astype('datetime64[ns]')

        DF_HOI_FUT["script_identifier"] = DF_HOI_FUT["symbol"]+DF_HOI_FUT["exp_date"].dt.strftime('%y%b%d').str.upper()+"FUT"
        DF_HOI_FUT["created_on"] = datetime.now()
        # print(DF_HOI_FUT)
        # DB Insert Process
        AB_Screener_DB().insert_query_mcx(df=DF_HOI_FUT,table_name=Props.DB_TBL_LOSER_MCX)

        redis_key = "HIGHEST:LOSS:MCX"
        save_redis(redis_key, DF_HOI_FUT.to_json(orient='records'))
        print("Completed Highest_OI_FUT -",datetime.now())
        logging.info("Completed Highest_OI_FUT")

def Volume_MCX():
    # Get CMOTS API Request
    response_data  = CMOTS_API_Request(JWT=True,Endpoint=Props.CMOTS_ENDPOINT_MCX_VOLUME)

    if response_data["stat"] == 'ok':

        HOI_FUT = response_data["data"]["data"]
        DF_HOI_FUT = pd.DataFrame(HOI_FUT)
        DF_HOI_FUT["symbol"] = DF_HOI_FUT["symbol"].str.strip()
        DF_HOI_FUT["exp_date"] = DF_HOI_FUT["exp_date"].astype('datetime64[ns]')
        DF_HOI_FUT["trd_date"] = DF_HOI_FUT["trd_date"].astype('datetime64[ns]')

        DF_HOI_FUT["script_identifier"] = DF_HOI_FUT["symbol"]+DF_HOI_FUT["exp_date"].dt.strftime('%y%b%d').str.upper()+"FUT"
        DF_HOI_FUT["created_on"] = datetime.now()
        # print(DF_HOI_FUT)
        # DB Insert Process
        AB_Screener_DB().insert_query_mcx(df=DF_HOI_FUT,table_name=Props.DB_TBL_VOLUME_MCX)

        redis_key = "HIGHEST:VOL:MCX"
        save_redis(redis_key, DF_HOI_FUT.to_json(orient='records'))
        print("Completed Highest_OI_FUT -",datetime.now())
        logging.info("Completed Highest_OI_FUT")

contract_download()
# Volume_MCX()

def start_scheduler(start_time, stop_time, interval_minutes):
    # Convert start and stop time strings to datetime objects
    start_datetime = datetime.strptime(start_time, "%H:%M")
    stop_datetime = datetime.strptime(stop_time, "%H:%M")
    schedule.every().day.at("08:00").do(contract_download)
    # Create a schedule job that runs every 'interval_minutes' minutes
    schedule.every(interval_minutes).minutes.do(Highest_OI_FUT)
    schedule.every(interval_minutes).minutes.do(Highest_OI_OPT)
    schedule.every(interval_minutes).minutes.do(Gainer_OI_FUT)
    schedule.every(interval_minutes).minutes.do(Gainer_OI_OPT)
    schedule.every(interval_minutes).minutes.do(Looser_OI_FUT)
    schedule.every(interval_minutes).minutes.do(Looser_OI_OPT)
    schedule.every(interval_minutes).minutes.do(Highest_Volume_FUT)
    schedule.every(interval_minutes).minutes.do(Highest_Volume_OPT)
    schedule.every(interval_minutes).minutes.do(Gainer_MCX)
    schedule.every(interval_minutes).minutes.do(Loser_MCX)
    schedule.every(interval_minutes).minutes.do(Volume_MCX)


    while True:
        # Check if the current time is within the specified start and stop time
        now = datetime.now().time()
        if start_datetime.time() <= now <= stop_datetime.time():
            schedule.run_pending()

        # Sleep for 1 minute before checking again
        time.sleep(60)

if __name__ == "__main__":
    # Specify start and stop time in HH:MM format
    start_time = "09:00"
    stop_time = "22:30"

    # Set the interval in minutes
    interval_minutes = 1

    start_scheduler(start_time, stop_time, interval_minutes)

