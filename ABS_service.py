import time
import random
import pandas as pd

from modules.utilities.redis import *
from modules.libraries import *
from modules.processors.database import AB_Screener_DB, fii_dii_DB
from modules.processors.parser import CMOTS_API_Request

app = Flask(__name__)

def request_quality_checker(request):
    # if len(request.data) == 0:
    #     return {
    #         "stat":"Not_ok",
    #         "emsg":"Payload is empty"
    #     }
    if request.data.decode("ascii"):
        data = request.get_json()

        # if "exch" not in data or "underlying" not in data or "expiry" not in data:
        #     return {
        #         "stat": "Not_ok",
        #         "emsg": "Payload Input Key is Missing"
        #     }

        if "instrument_type" in data and data["instrument_type"] not in ["OPTSTK","OPTIDX","FUTSTK","FUTIDX"]:
            return {
                "stat": "Not_ok",
                "emsg": "Invalid Input of Instrument Type"
            }

        return {
            "stat":"ok",
            "data":data
        }
    else:
        return {
            "stat":"ok",
            "data":{
            }
        }


@app.route("/ab_screeners/highestOIOpt", methods=['POST'])
def highestOIOpt():
    """
            To Record the User IP and User Agent
            Author : Vigneshwar K
    """
    ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    User_agent = request.headers.get('User-Agent')

    """
            Quality checker of request given and share the proper error message
            Author: Vigneshwar K
    """
    request_quality = request_quality_checker(request)
    if request_quality["stat"] == "Not_ok":
        return request_quality

    input_data = request_quality["data"]

    input_exch = input_data["exch"] if "exch" in input_data else None
    input_symbol = input_data["underlying"] if "underlying" in input_data else None
    try:
        input_expiry = datetime.strptime(input_data["expiry"], "%d%b%y").date() if "expiry" in input_data else None
    except Exception as err:
        return {
            "stat":"Not_ok",
            "emsg":str(err)
        }
    input_instype = input_data["instrument_type"] if "instrument_type" in input_data else None

    cache_data = get_redis_cache("HIGHEST:OI:OPT")
    if cache_data["stat"] == "Not_ok":
        DB_Data = AB_Screener_DB().select_query(Props.DB_TBL_HIGH_OI_OPT)
        HOI_DF  = pd.DataFrame(DB_Data["data"],columns=DB_Data["column"])
        HOI_DF.columns = HOI_DF.columns.str.lower()
        HOI_DF["expdate"] = pd.to_datetime(HOI_DF["expdate"]).apply(lambda x: x.date())
        HOI_DF["updtime"] = pd.to_datetime(HOI_DF["updtime"])
    else:
        HOI_DF = pd.DataFrame(cache_data["data"])
        HOI_DF["expdate"] = pd.to_datetime(HOI_DF["expdate"],unit="ms").apply(lambda x: x.date())
        HOI_DF["updtime"] = pd.to_datetime(HOI_DF["updtime"], unit="ms")

    HOI_DF = HOI_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint']]


    if input_symbol and input_expiry:
        output_DF = HOI_DF[(HOI_DF["symbol"]==input_symbol)&(HOI_DF["expdate"]==input_expiry)].reset_index(drop=True)
    elif input_symbol:
        output_DF = HOI_DF[(HOI_DF["symbol"]==input_symbol)].sort_values("expdate").reset_index(drop=True)
    elif input_instype:
        output_DF = HOI_DF[(HOI_DF["instname"] == input_instype)].reset_index(drop=True)
    else:
        output_DF = HOI_DF

    # output_DF = output_DF.sort_values(by="openinterest", ascending=False).reset_index(drop=True)[0:20]
    output_DF["expdate"] = pd.to_datetime(output_DF["expdate"])

    output_DF["strikeprice"] = output_DF["strikeprice"].astype(str)
    bhav_df = pd.read_csv(Props.BHAVCOPY_SAVE_PATH + '/zip/%s_NFO_BHAV_COPY.csv.zip' % (datetime.now() - timedelta(days=Props.BHAVCOPY_DAYS)).strftime("%d%b%Y").upper())

    contract_master = pd.read_csv(Props.CONTRACT_SAVE_PATH+'%s_%s_FO.csv' % (datetime.now().strftime("%d%b%Y").upper(), "NFO"))
    contract_master = contract_master[(contract_master["instrument_type"] == "OPTSTK")|(contract_master["instrument_type"] == "OPTIDX")]

    # contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token"]]
    contract_master["strike_price"] = contract_master["strike_price"].astype(float).astype(int).astype(str)
    contract_master["expiry_date"] = pd.to_datetime(contract_master["expiry_date"])

    bhav_df.rename(columns={'SYMBOL': 'symbol', 'INSTRUMENT': 'instrument_type', 'EXPIRY_DT': 'expiry_date',
                            'OPTION_TYP': 'option_type', 'STRIKE_PR': 'strike_price', 'CLOSE':'pdc',"OPEN_INT":"pdoi"}, inplace=True)
    bhav_df["expiry_date"] = pd.to_datetime(bhav_df["expiry_date"])
    bhav_df["strike_price"] = bhav_df["strike_price"].astype(int).astype(str)

    # Filter Required Column in Dataframe
    contract_master = pd.merge(contract_master, bhav_df,
                            on=['instrument_type', 'symbol', 'expiry_date', 'strike_price', 'option_type'],
                            how='left')

    contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token","pdc","pdoi","lot_size","formatted_ins_name"]]


    output_DF = pd.merge(output_DF, contract_master, left_on=['symbol', 'strikeprice', 'opttype','expdate'], right_on=['symbol','strike_price','option_type','expiry_date'], how='left')
    output_DF["mktlot"] = output_DF["lot_size"]
    output_DF["instname"] = output_DF["formatted_ins_name"]
    output_DF = output_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint','token',"pdc","pdoi","mktlot"]]

    output_DF["openinterest"] = output_DF["openinterest"].astype(float)/output_DF["mktlot"].astype(float)
    output_DF["openinterest"] = output_DF["openinterest"].fillna(0)
    output_DF["openinterest"] = output_DF["openinterest"].astype(int)

    output_DF = output_DF.sort_values("openinterest",ascending=False).reset_index(drop=True)[0:20]

    output_DF["expdate"] = output_DF["expdate"].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_DF["updtime"] = output_DF["updtime"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    output_DF["exch"] = "nse_fo"
    output_DF["token"] = output_DF["token"].astype(int).astype(str)
    output_data = output_DF.to_json(orient='records')

    output_response ={
        "stat": "ok",
        "data": json.loads(output_data)
    }

    return output_response

@app.route("/ab_screeners/highestOIFut", methods=['POST'])
def highestOIFut():
    """
            To Record the User IP and User Agent
            Author : Vigneshwar K
    """
    ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    User_agent = request.headers.get('User-Agent')

    """
            Quality checker of request given and share the proper error message
            Author: Vigneshwar K
    """
    request_quality = request_quality_checker(request)
    if request_quality["stat"] == "Not_ok":
        return request_quality

    input_data = request_quality["data"]

    input_exch = input_data["exch"] if "exch" in input_data else None
    input_symbol = input_data["underlying"] if "underlying" in input_data else None
    try:
        input_expiry = datetime.strptime(input_data["expiry"], "%d%b%y").date() if "expiry" in input_data else None
    except Exception as err:
        return {
            "stat":"Not_ok",
            "emsg":str(err)
        }
    input_instype = input_data["instrument_type"] if "instrument_type" in input_data else None

    cache_data = get_redis_cache("HIGHEST:OI:FUT")
    if cache_data["stat"] == "Not_ok":
        DB_Data = AB_Screener_DB().select_query(Props.DB_TBL_HIGH_OI_FUT)
        HOI_DF  = pd.DataFrame(DB_Data["data"],columns=DB_Data["column"])
        HOI_DF.columns = HOI_DF.columns.str.lower()
        HOI_DF["expdate"] = pd.to_datetime(HOI_DF["expdate"]).apply(lambda x: x.date())
        HOI_DF["updtime"] = pd.to_datetime(HOI_DF["updtime"])
    else:
        HOI_DF = pd.DataFrame(cache_data["data"])

        HOI_DF["expdate"] = pd.to_datetime(HOI_DF["expdate"],unit="ms").apply(lambda x: x.date())
        HOI_DF["updtime"] = pd.to_datetime(HOI_DF["updtime"], unit="ms")

    HOI_DF = HOI_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint']]

    if input_symbol and input_expiry:
        output_DF = HOI_DF[(HOI_DF["symbol"] == input_symbol) & (HOI_DF["expdate"] == input_expiry)].reset_index(drop=True)
    elif input_symbol:
        output_DF = HOI_DF[(HOI_DF["symbol"] == input_symbol)].sort_values("expdate").reset_index(drop=True)
    elif input_instype:
        output_DF = HOI_DF[(HOI_DF["instname"] == input_instype)].reset_index(drop=True)
    else:
        output_DF = HOI_DF

    # output_DF = output_DF.sort_values(by="openinterest", ascending=False).reset_index(drop=True)[0:20]

    output_DF["expdate"] = pd.to_datetime(output_DF["expdate"])
    output_DF["strikeprice"] = output_DF["strikeprice"].astype(str)

    bhav_df = pd.read_csv(Props.BHAVCOPY_SAVE_PATH + '/zip/%s_NFO_BHAV_COPY.csv.zip' % (datetime.now() - timedelta(days=Props.BHAVCOPY_DAYS)).strftime("%d%b%Y").upper())

    contract_master = pd.read_csv(Props.CONTRACT_SAVE_PATH+'%s_%s_FO.csv' % (datetime.now().strftime("%d%b%Y").upper(), "NFO"))
    contract_master = contract_master[(contract_master["instrument_type"] == "FUTSTK")|(contract_master["instrument_type"] == "FUTIDX")]


    contract_master["strike_price"] = contract_master["strike_price"].astype(float).astype(int).astype(str)
    contract_master["expiry_date"] = pd.to_datetime(contract_master["expiry_date"])

    bhav_df.rename(columns={'SYMBOL': 'symbol', 'INSTRUMENT': 'instrument_type', 'EXPIRY_DT': 'expiry_date',
                            'OPTION_TYP': 'option_type', 'CLOSE':'pdc',"OPEN_INT":"pdoi"}, inplace=True)
    bhav_df["expiry_date"] = pd.to_datetime(bhav_df["expiry_date"])
    # bhav_df["strike_price"] = bhav_df["strike_price"].astype(int).astype(str)

    # Filter Required Column in Dataframe
    contract_master = pd.merge(contract_master, bhav_df,
                            on=['instrument_type', 'symbol', 'expiry_date', 'option_type'],
                            how='left')

    contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token","pdc","pdoi","lot_size","formatted_ins_name"]]

    output_DF = pd.merge(output_DF, contract_master, left_on=['symbol','expdate'], right_on=['symbol','expiry_date'], how='left')
    output_DF["mktlot"] = output_DF["lot_size"]
    output_DF["instname"] = output_DF["formatted_ins_name"]
    output_DF = output_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint','token','pdc','pdoi',"mktlot"]]

    output_DF["exch"] = "nse_fo"

    output_DF["openinterest"] = output_DF["openinterest"].astype(float) / output_DF["mktlot"].astype(float)
    output_DF["openinterest"] = output_DF["openinterest"].fillna(0)
    output_DF["openinterest"] = output_DF["openinterest"].astype(int)

    output_DF = output_DF.sort_values("openinterest", ascending=False).reset_index(drop=True)[0:20]

    output_DF["token"] = output_DF["token"].astype(int).astype(str)

    output_DF["expdate"] = output_DF["expdate"].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_DF["updtime"] = output_DF["updtime"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))


    output_data = output_DF.to_json(orient='records')

    output_response ={
        "stat": "ok",
        "data": json.loads(output_data)
    }

    return output_response

@app.route("/ab_screeners/gainerOIFut", methods=['POST'])
def gainerOIFut():
    """
            To Record the User IP and User Agent
            Author : Vigneshwar K
    """
    ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    User_agent = request.headers.get('User-Agent')

    """
            Quality checker of request given and share the proper error message
            Author: Vigneshwar K
    """
    request_quality = request_quality_checker(request)
    if request_quality["stat"] == "Not_ok":
        return request_quality

    input_data = request_quality["data"]

    input_exch = input_data["exch"] if "exch" in input_data else None
    input_symbol = input_data["underlying"] if "underlying" in input_data else None
    try:
        input_expiry = datetime.strptime(input_data["expiry"], "%d%b%y").date() if "expiry" in input_data else None
    except Exception as err:
        return {
            "stat":"Not_ok",
            "emsg":str(err)
        }
    input_instype = input_data["instrument_type"] if "instrument_type" in input_data else None


    cache_data = get_redis_cache("GAINER:OI:FUT")
    if cache_data["stat"] == "Not_ok":
        DB_Data = AB_Screener_DB().select_query(Props.DB_TBL_GAIN_OI_FUT)
        GOI_DF  = pd.DataFrame(DB_Data["data"],columns=DB_Data["column"])
        GOI_DF.columns = GOI_DF.columns.str.lower()
        GOI_DF["expdate"] = pd.to_datetime(GOI_DF["expdate"]).apply(lambda x: x.date())
        GOI_DF["updtime"] = pd.to_datetime(GOI_DF["updtime"])
    else:
        GOI_DF = pd.DataFrame(cache_data["data"])
        GOI_DF["expdate"] = pd.to_datetime(GOI_DF["expdate"],unit="ms").apply(lambda x: x.date())
        GOI_DF["updtime"] = pd.to_datetime(GOI_DF["updtime"], unit="ms")

    GOI_DF = GOI_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint']]

    if input_symbol and input_expiry:
        output_DF = GOI_DF[(GOI_DF["symbol"] == input_symbol) & (GOI_DF["expdate"] == input_expiry)].reset_index(drop=True)
    elif input_symbol:
        output_DF = GOI_DF[(GOI_DF["symbol"] == input_symbol)].sort_values("expdate").reset_index(drop=True)
    elif input_instype:
        output_DF = GOI_DF[(GOI_DF["instname"] == input_instype)].reset_index(drop=True)
    else:
        output_DF = GOI_DF

    # output_DF = output_DF.sort_values(by="openinterest", ascending=False).reset_index(drop=True)[0:20]

    output_DF["expdate"] = pd.to_datetime(output_DF["expdate"])

    output_DF["strikeprice"] = output_DF["strikeprice"].astype(str)

    bhav_df = pd.read_csv(Props.BHAVCOPY_SAVE_PATH + '/zip/%s_NFO_BHAV_COPY.csv.zip' % (datetime.now() - timedelta(days=Props.BHAVCOPY_DAYS)).strftime("%d%b%Y").upper())

    contract_master = pd.read_csv(Props.CONTRACT_SAVE_PATH+'%s_%s_FO.csv' % (datetime.now().strftime("%d%b%Y").upper(), "NFO"))
    contract_master = contract_master[(contract_master["instrument_type"] == "FUTSTK")|(contract_master["instrument_type"] == "FUTIDX")]

    # contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token"]]
    contract_master["strike_price"] = contract_master["strike_price"].astype(float).astype(int).astype(str)
    contract_master["expiry_date"] = pd.to_datetime(contract_master["expiry_date"])

    bhav_df.rename(columns={'SYMBOL': 'symbol', 'INSTRUMENT': 'instrument_type', 'EXPIRY_DT': 'expiry_date',
                            'OPTION_TYP': 'option_type', 'CLOSE':'pdc',"OPEN_INT":"pdoi"}, inplace=True)
    bhav_df["expiry_date"] = pd.to_datetime(bhav_df["expiry_date"])
    # bhav_df["strike_price"] = bhav_df["strike_price"].astype(int).astype(str)

    # Filter Required Column in Dataframe
    contract_master = pd.merge(contract_master, bhav_df,
                            on=['instrument_type', 'symbol', 'expiry_date', 'option_type'],
                            how='left')
    contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token","pdc","pdoi","lot_size","formatted_ins_name"]]

    output_DF = pd.merge(output_DF, contract_master, left_on=['symbol' ,'expdate'], right_on=['symbol' ,'expiry_date'], how='left')
    output_DF["mktlot"] = output_DF["lot_size"]
    output_DF["instname"] = output_DF["formatted_ins_name"]
    output_DF = output_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint','token',"pdc","pdoi","mktlot"]]

    output_DF["openinterest"] = output_DF["openinterest"].astype(float) / output_DF["mktlot"].astype(float)
    output_DF["openinterest"] = output_DF["openinterest"].fillna(0)
    output_DF["openinterest"] = output_DF["openinterest"].astype(int)
    output_DF["chgopenint"] = output_DF["chgopenint"].astype(float)
    output_DF = output_DF.sort_values("chgopenint", ascending=False).reset_index(drop=True)[0:20]

    output_DF["expdate"] = output_DF["expdate"].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_DF["updtime"] = output_DF["updtime"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    output_DF["exch"] = "nse_fo"
    output_DF["token"] = output_DF["token"].astype(int).astype(str)
    output_data = output_DF.to_json(orient='records')

    output_response ={
        "stat": "ok",
        "data": json.loads(output_data)
    }

    return output_response

@app.route("/ab_screeners/gainerOIOpt", methods=['POST'])
def gainerOIOpt():
    """
            To Record the User IP and User Agent
            Author : Vigneshwar K
    """
    ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    User_agent = request.headers.get('User-Agent')

    """
            Quality checker of request given and share the proper error message
            Author: Vigneshwar K
    """
    request_quality = request_quality_checker(request)
    if request_quality["stat"] == "Not_ok":
        return request_quality

    input_data = request_quality["data"]

    input_exch = input_data["exch"] if "exch" in input_data else None
    input_symbol = input_data["underlying"] if "underlying" in input_data else None
    try:
        input_expiry = datetime.strptime(input_data["expiry"], "%d%b%y").date() if "expiry" in input_data else None
    except Exception as err:
        return {
            "stat":"Not_ok",
            "emsg":str(err)
        }
    input_instype = input_data["instrument_type"] if "instrument_type" in input_data else None



    cache_data = get_redis_cache("GAINER:OI:OPT")
    if cache_data["stat"] == "Not_ok":
        DB_Data = AB_Screener_DB().select_query(Props.DB_TBL_GAIN_OI_OPT)
        GOI_DF  = pd.DataFrame(DB_Data["data"],columns=DB_Data["column"])
        GOI_DF.columns = GOI_DF.columns.str.lower()
        GOI_DF["expdate"] = pd.to_datetime(GOI_DF["expdate"]).apply(lambda x: x.date())
        GOI_DF["updtime"] = pd.to_datetime(GOI_DF["updtime"])
    else:
        GOI_DF = pd.DataFrame(cache_data["data"])
        GOI_DF["expdate"] = pd.to_datetime(GOI_DF["expdate"],unit="ms").apply(lambda x: x.date())
        GOI_DF["updtime"] = pd.to_datetime(GOI_DF["updtime"], unit="ms")

    GOI_DF = GOI_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint']]

    if input_symbol and input_expiry:
        output_DF = GOI_DF[(GOI_DF["symbol"] == input_symbol) & (GOI_DF["expdate"] == input_expiry)].reset_index(drop=True)
    elif input_symbol:
        output_DF = GOI_DF[(GOI_DF["symbol"] == input_symbol)].sort_values("expdate").reset_index(drop=True)
    elif input_instype:
        output_DF = GOI_DF[(GOI_DF["instname"] == input_instype)].reset_index(drop=True)
    else:
        output_DF = GOI_DF

    # output_DF = output_DF.sort_values(by="openinterest", ascending=False).reset_index(drop=True)[0:20]

    output_DF["expdate"] = pd.to_datetime(output_DF["expdate"])

    output_DF["strikeprice"] = output_DF["strikeprice"].astype(str)

    bhav_df = pd.read_csv(Props.BHAVCOPY_SAVE_PATH + '/zip/%s_NFO_BHAV_COPY.csv.zip' % (datetime.now() - timedelta(days=Props.BHAVCOPY_DAYS)).strftime("%d%b%Y").upper())

    contract_master = pd.read_csv(Props.CONTRACT_SAVE_PATH+'%s_%s_FO.csv' % (datetime.now().strftime("%d%b%Y").upper(), "NFO"))
    contract_master = contract_master[(contract_master["instrument_type"] == "OPTSTK")|(contract_master["instrument_type"] == "OPTIDX")]

    # contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token"]]
    contract_master["strike_price"] = contract_master["strike_price"].astype(float).astype(int).astype(str)
    contract_master["expiry_date"] = pd.to_datetime(contract_master["expiry_date"])

    bhav_df.rename(columns={'SYMBOL': 'symbol', 'INSTRUMENT': 'instrument_type', 'EXPIRY_DT': 'expiry_date',
                            'OPTION_TYP': 'option_type', 'STRIKE_PR': 'strike_price', 'CLOSE':'pdc',"OPEN_INT":"pdoi"}, inplace=True)
    bhav_df["expiry_date"] = pd.to_datetime(bhav_df["expiry_date"])
    bhav_df["strike_price"] = bhav_df["strike_price"].astype(int).astype(str)

    # Filter Required Column in Dataframe
    contract_master = pd.merge(contract_master, bhav_df,
                            on=['instrument_type', 'symbol', 'expiry_date', 'strike_price', 'option_type'],
                            how='left')
    contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token","pdc","pdoi","lot_size","formatted_ins_name"]]

    output_DF = pd.merge(output_DF, contract_master, left_on=['symbol', 'strikeprice', 'opttype','expdate'], right_on=['symbol','strike_price','option_type','expiry_date'], how='left')
    output_DF["mktlot"] = output_DF["lot_size"]
    output_DF["instname"] = output_DF["formatted_ins_name"]
    output_DF = output_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint','token',"pdc","pdoi","mktlot"]]

    output_DF["openinterest"] = output_DF["openinterest"].astype(float) / output_DF["mktlot"].astype(float)
    output_DF["openinterest"] = output_DF["openinterest"].fillna(0)
    output_DF["openinterest"] = output_DF["openinterest"].astype(int)
    output_DF["chgopenint"] = output_DF["chgopenint"].astype(float)

    output_DF = output_DF.sort_values("chgopenint", ascending=False).reset_index(drop=True)[0:20]

    output_DF["expdate"] = output_DF["expdate"].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_DF["updtime"] = output_DF["updtime"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    output_DF["exch"] = "nse_fo"
    output_DF["token"] = output_DF["token"].astype(int).astype(str)
    output_data = output_DF.to_json(orient='records')

    output_response ={
        "stat": "ok",
        "data": json.loads(output_data)
    }

    return output_response

@app.route("/ab_screeners/looserOIFut", methods=['POST'])
def looserOIFut():
    """
            To Record the User IP and User Agent
            Author : Vigneshwar K
    """
    ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    User_agent = request.headers.get('User-Agent')

    """
            Quality checker of request given and share the proper error message
            Author: Vigneshwar K
    """
    request_quality = request_quality_checker(request)
    if request_quality["stat"] == "Not_ok":
        return request_quality

    input_data = request_quality["data"]

    input_exch = input_data["exch"] if "exch" in input_data else None
    input_symbol = input_data["underlying"] if "underlying" in input_data else None
    try:
        input_expiry = datetime.strptime(input_data["expiry"], "%d%b%y").date() if "expiry" in input_data else None
    except Exception as err:
        return {
            "stat":"Not_ok",
            "emsg":str(err)
        }
    input_instype = input_data["instrument_type"] if "instrument_type" in input_data else None


    cache_data = get_redis_cache("LOOSER:OI:FUT")
    if cache_data["stat"] == "Not_ok":
        DB_Data = AB_Screener_DB().select_query(Props.DB_TBL_LOSS_OI_FUT)
        LOI_DF  = pd.DataFrame(DB_Data["data"],columns=DB_Data["column"])
        LOI_DF.columns = LOI_DF.columns.str.lower()
        LOI_DF["expdate"] = pd.to_datetime(LOI_DF["expdate"]).apply(lambda x: x.date())
        LOI_DF["updtime"] = pd.to_datetime(LOI_DF["updtime"])
    else:
        LOI_DF = pd.DataFrame(cache_data["data"])
        LOI_DF["expdate"] = pd.to_datetime(LOI_DF["expdate"],unit="ms").apply(lambda x: x.date())
        LOI_DF["updtime"] = pd.to_datetime(LOI_DF["updtime"], unit="ms")

    LOI_DF = LOI_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint']]

    if input_symbol and input_expiry:
        output_DF = LOI_DF[(LOI_DF["symbol"] == input_symbol) & (LOI_DF["expdate"] == input_expiry)].reset_index(drop=True)
    elif input_symbol:
        output_DF = LOI_DF[(LOI_DF["symbol"] == input_symbol)].sort_values("expdate").reset_index(drop=True)
    elif input_instype:
        output_DF = LOI_DF[(LOI_DF["instname"] == input_instype)].reset_index(drop=True)
    else:
        output_DF = LOI_DF

    # output_DF = output_DF.sort_values(by="openinterest", ascending=True).reset_index(drop=True)[0:20]

    output_DF["expdate"] = pd.to_datetime(output_DF["expdate"])

    output_DF["strikeprice"] = output_DF["strikeprice"].astype(str)

    bhav_df = pd.read_csv(Props.BHAVCOPY_SAVE_PATH + '/zip/%s_NFO_BHAV_COPY.csv.zip' % (datetime.now() - timedelta(days=Props.BHAVCOPY_DAYS)).strftime("%d%b%Y").upper())

    contract_master = pd.read_csv(Props.CONTRACT_SAVE_PATH+'%s_%s_FO.csv' % (datetime.now().strftime("%d%b%Y").upper(), "NFO"))
    contract_master = contract_master[(contract_master["instrument_type"] == "FUTSTK")|(contract_master["instrument_type"] == "FUTIDX")]

    # contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token"]]
    contract_master["strike_price"] = contract_master["strike_price"].astype(float).astype(int).astype(str)
    contract_master["expiry_date"] = pd.to_datetime(contract_master["expiry_date"])

    bhav_df.rename(columns={'SYMBOL': 'symbol', 'INSTRUMENT': 'instrument_type', 'EXPIRY_DT': 'expiry_date',
                            'OPTION_TYP': 'option_type', 'CLOSE':'pdc',"OPEN_INT":"pdoi"}, inplace=True)
    bhav_df["expiry_date"] = pd.to_datetime(bhav_df["expiry_date"])
    # bhav_df["strike_price"] = bhav_df["strike_price"].astype(int).astype(str)

    # Filter Required Column in Dataframe
    contract_master = pd.merge(contract_master, bhav_df,
                            on=['instrument_type', 'symbol', 'expiry_date', 'option_type'],
                            how='left')
    contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token","pdc","pdoi","lot_size","formatted_ins_name"]]

    output_DF = pd.merge(output_DF, contract_master, left_on=['symbol','expdate'], right_on=['symbol','expiry_date'], how='left')
    output_DF["mktlot"] = output_DF["lot_size"]
    output_DF["instname"] = output_DF["formatted_ins_name"]
    output_DF = output_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint','token',"pdc","pdoi","mktlot"]]

    output_DF["openinterest"] = output_DF["openinterest"].astype(float) / output_DF["mktlot"].astype(float)
    output_DF["openinterest"] = output_DF["openinterest"].fillna(0)
    output_DF["openinterest"] = output_DF["openinterest"].astype(int)
    output_DF["chgopenint"] = output_DF["chgopenint"].astype(float)
    output_DF = output_DF.sort_values("chgopenint", ascending=True).reset_index(drop=True)[0:20]

    output_DF["expdate"] = output_DF["expdate"].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_DF["updtime"] = output_DF["updtime"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    output_DF["exch"] = "nse_fo"
    output_DF["token"] = output_DF["token"].astype(int).astype(str)
    output_data = output_DF.to_json(orient='records')

    output_response ={
        "stat": "ok",
        "data": json.loads(output_data)
    }

    return output_response

@app.route("/ab_screeners/looserOIOpt", methods=['POST'])
def looserOIOpt():
    """
            To Record the User IP and User Agent
            Author : Vigneshwar K
    """
    ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    User_agent = request.headers.get('User-Agent')

    """
            Quality checker of request given and share the proper error message
            Author: Vigneshwar K
    """
    request_quality = request_quality_checker(request)
    if request_quality["stat"] == "Not_ok":
        return request_quality

    input_data = request_quality["data"]

    input_exch = input_data["exch"] if "exch" in input_data else None
    input_symbol = input_data["underlying"] if "underlying" in input_data else None
    try:
        input_expiry = datetime.strptime(input_data["expiry"], "%d%b%y").date() if "expiry" in input_data else None
    except Exception as err:
        return {
            "stat":"Not_ok",
            "emsg":str(err)
        }
    input_instype = input_data["instrument_type"] if "instrument_type" in input_data else None


    cache_data = get_redis_cache("LOOSER:OI:OPT")
    if cache_data["stat"] == "Not_ok":
        DB_Data = AB_Screener_DB().select_query(Props.DB_TBL_LOSS_OI_OPT)
        LOI_DF  = pd.DataFrame(DB_Data["data"],columns=DB_Data["column"])
        LOI_DF.columns = LOI_DF.columns.str.lower()
        LOI_DF["expdate"] = pd.to_datetime(LOI_DF["expdate"]).apply(lambda x: x.date())
        LOI_DF["updtime"] = pd.to_datetime(LOI_DF["updtime"])
    else:
        LOI_DF = pd.DataFrame(cache_data["data"])
        LOI_DF["expdate"] = pd.to_datetime(LOI_DF["expdate"],unit="ms").apply(lambda x: x.date())
        LOI_DF["updtime"] = pd.to_datetime(LOI_DF["updtime"], unit="ms")

    LOI_DF = LOI_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint']]

    if input_symbol and input_expiry:
        output_DF = LOI_DF[(LOI_DF["symbol"] == input_symbol) & (LOI_DF["expdate"] == input_expiry)].reset_index(drop=True)
    elif input_symbol:
        output_DF = LOI_DF[(LOI_DF["symbol"] == input_symbol)].sort_values("expdate").reset_index(drop=True)
    elif input_instype:
        output_DF = LOI_DF[(LOI_DF["instname"] == input_instype)].reset_index(drop=True)
    else:
        output_DF = LOI_DF
    # output_DF = output_DF.sort_values(by="openinterest", ascending=True).reset_index(drop=True)[0:20]

    output_DF["expdate"] = pd.to_datetime(output_DF["expdate"])

    output_DF["strikeprice"] = output_DF["strikeprice"].astype(str)

    bhav_df = pd.read_csv(Props.BHAVCOPY_SAVE_PATH + '/zip/%s_NFO_BHAV_COPY.csv.zip' % (datetime.now() - timedelta(days=Props.BHAVCOPY_DAYS)).strftime("%d%b%Y").upper())

    contract_master = pd.read_csv(Props.CONTRACT_SAVE_PATH+'%s_%s_FO.csv' % (datetime.now().strftime("%d%b%Y").upper(), "NFO"))
    contract_master = contract_master[(contract_master["instrument_type"] == "OPTSTK")|(contract_master["instrument_type"] == "OPTIDX")]

    # contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token"]]
    contract_master["strike_price"] = contract_master["strike_price"].astype(float).astype(int).astype(str)
    contract_master["expiry_date"] = pd.to_datetime(contract_master["expiry_date"])

    bhav_df.rename(columns={'SYMBOL': 'symbol', 'INSTRUMENT': 'instrument_type', 'EXPIRY_DT': 'expiry_date',
                            'OPTION_TYP': 'option_type', 'STRIKE_PR': 'strike_price', 'CLOSE':'pdc',"OPEN_INT":"pdoi"}, inplace=True)
    bhav_df["expiry_date"] = pd.to_datetime(bhav_df["expiry_date"])
    bhav_df["strike_price"] = bhav_df["strike_price"].astype(int).astype(str)

    # Filter Required Column in Dataframe
    contract_master = pd.merge(contract_master, bhav_df,
                            on=['instrument_type', 'symbol', 'expiry_date', 'strike_price', 'option_type'],
                            how='left')
    contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token","pdc","pdoi","lot_size","formatted_ins_name"]]

    output_DF = pd.merge(output_DF, contract_master, left_on=['symbol', 'strikeprice', 'opttype','expdate'], right_on=['symbol','strike_price','option_type','expiry_date'], how='left')
    output_DF["mktlot"] = output_DF["lot_size"]
    output_DF["instname"] = output_DF["formatted_ins_name"]
    output_DF = output_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint','token',"pdc","pdoi","mktlot"]]

    output_DF["openinterest"] = output_DF["openinterest"].astype(float) / output_DF["mktlot"].astype(float)
    output_DF["openinterest"] = output_DF["openinterest"].fillna(0)
    output_DF["openinterest"] = output_DF["openinterest"].astype(int)
    output_DF["chgopenint"] = output_DF["chgopenint"].astype(float)
    output_DF = output_DF.sort_values("chgopenint", ascending=True).reset_index(drop=True)[0:20]

    output_DF["expdate"] = output_DF["expdate"].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_DF["updtime"] = output_DF["updtime"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    output_DF["exch"] = "nse_fo"
    output_DF["token"] = output_DF["token"].fillna("0")
    output_DF["token"] = output_DF["token"].astype(int).astype(str)
    output_DF = output_DF[output_DF["token"]!="0"]
    output_data = output_DF.to_json(orient='records')


    output_response ={
        "stat": "ok",
        "data": json.loads(output_data)
    }

    return output_response

@app.route("/ab_screeners/HighVolumeFut", methods=['POST'])
def HighVolumeFut():
    """
                To Record the User IP and User Agent
                Author : Vigneshwar K
        """
    ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    User_agent = request.headers.get('User-Agent')

    """
            Quality checker of request given and share the proper error message
            Author: Vigneshwar K
    """
    request_quality = request_quality_checker(request)
    if request_quality["stat"] == "Not_ok":
        return request_quality

    input_data = request_quality["data"]

    input_exch = input_data["exch"] if "exch" in input_data else None
    input_symbol = input_data["underlying"] if "underlying" in input_data else None
    try:
        input_expiry = datetime.strptime(input_data["expiry"], "%d%b%y").date() if "expiry" in input_data else None
    except Exception as err:
        return {
            "stat":"Not_ok",
            "emsg":str(err)
        }
    input_instype = input_data["instrument_type"] if "instrument_type" in input_data else None

    cache_data = get_redis_cache("HIGHEST:VOL:FUT")

    if cache_data["stat"] == "Not_ok":
        DB_Data = AB_Screener_DB().select_query(Props.DB_TBL_HIGH_VOL_FUT)
        HOI_DF  = pd.DataFrame(DB_Data["data"],columns=DB_Data["column"])
        HOI_DF.columns = HOI_DF.columns.str.lower()
        HOI_DF["expdate"] = pd.to_datetime(HOI_DF["expdate"]).apply(lambda x: x.date())
        HOI_DF["updtime"] = pd.to_datetime(HOI_DF["updtime"])
    else:
        HOI_DF = pd.DataFrame(cache_data["data"])

        HOI_DF["expdate"] = pd.to_datetime(HOI_DF["expdate"],unit="ms").apply(lambda x: x.date())
        HOI_DF["updtime"] = pd.to_datetime(HOI_DF["updtime"], unit="ms")

    HOI_DF = HOI_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint']]

    if input_symbol and input_expiry:
        output_DF = HOI_DF[(HOI_DF["symbol"] == input_symbol) & (HOI_DF["expdate"] == input_expiry)].reset_index(drop=True)
    elif input_symbol:
        output_DF = HOI_DF[(HOI_DF["symbol"] == input_symbol)].sort_values("expdate").reset_index(drop=True)
    elif input_instype:
        output_DF = HOI_DF[(HOI_DF["instname"] == input_instype)].reset_index(drop=True)
    else:
        output_DF = HOI_DF
    output_DF["qty"] = output_DF["qty"].astype(float).astype(int)
    output_DF = output_DF.sort_values(by="qty", ascending=False).reset_index(drop=True)[0:20]

    output_DF["expdate"] = pd.to_datetime(output_DF["expdate"])
    output_DF["strikeprice"] = output_DF["strikeprice"].astype(str)

    bhav_df = pd.read_csv(Props.BHAVCOPY_SAVE_PATH + '/zip/%s_NFO_BHAV_COPY.csv.zip' % (datetime.now() - timedelta(days=Props.BHAVCOPY_DAYS)).strftime("%d%b%Y").upper())

    contract_master = pd.read_csv(Props.CONTRACT_SAVE_PATH+'%s_%s_FO.csv' % (datetime.now().strftime("%d%b%Y").upper(), "NFO"))
    contract_master = contract_master[(contract_master["instrument_type"] == "FUTSTK")|(contract_master["instrument_type"] == "FUTIDX")]


    contract_master["strike_price"] = contract_master["strike_price"].astype(float).astype(int).astype(str)
    contract_master["expiry_date"] = pd.to_datetime(contract_master["expiry_date"])

    bhav_df.rename(columns={'SYMBOL': 'symbol', 'INSTRUMENT': 'instrument_type', 'EXPIRY_DT': 'expiry_date',
                            'OPTION_TYP': 'option_type', 'CLOSE':'pdc',"OPEN_INT":"pdoi"}, inplace=True)
    bhav_df["expiry_date"] = pd.to_datetime(bhav_df["expiry_date"])
    # bhav_df["strike_price"] = bhav_df["strike_price"].astype(int).astype(str)

    # Filter Required Column in Dataframe
    contract_master = pd.merge(contract_master, bhav_df,
                            on=['instrument_type', 'symbol', 'expiry_date', 'option_type'],
                            how='left')

    contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token","pdc","pdoi","lot_size","formatted_ins_name"]]

    output_DF = pd.merge(output_DF, contract_master, left_on=['symbol','expdate'], right_on=['symbol','expiry_date'], how='left')
    output_DF["mktlot"] = output_DF["lot_size"]
    output_DF["instname"] = output_DF["formatted_ins_name"]
    output_DF["qty"] = output_DF["qty"].astype(str)
    output_DF = output_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint','token','pdc','pdoi',"mktlot"]]

    output_DF["exch"] = "nse_fo"

    output_DF["expdate"] = output_DF["expdate"].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_DF["updtime"] = output_DF["updtime"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    output_DF["token"] = output_DF["token"].astype(int).astype(str)
    output_data = output_DF.to_json(orient='records')

    output_response ={
        "stat": "ok",
        "data": json.loads(output_data)
    }

    return output_response

@app.route("/ab_screeners/HighVolumeOpt", methods=['POST'])
def HighVolumeOpt():
    """
            To Record the User IP and User Agent
            Author : Vigneshwar K
    """
    ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    User_agent = request.headers.get('User-Agent')

    """
            Quality checker of request given and share the proper error message
            Author: Vigneshwar K
    """
    request_quality = request_quality_checker(request)
    if request_quality["stat"] == "Not_ok":
        return request_quality

    input_data = request_quality["data"]

    input_exch = input_data["exch"] if "exch" in input_data else None
    input_symbol = input_data["underlying"] if "underlying" in input_data else None
    try:
        input_expiry = datetime.strptime(input_data["expiry"], "%d%b%y").date() if "expiry" in input_data else None
    except Exception as err:
        return {
            "stat":"Not_ok",
            "emsg":str(err)
        }
    input_instype = input_data["instrument_type"] if "instrument_type" in input_data else None

    cache_data = get_redis_cache("HIGHEST:VOL:OPT")
    if cache_data["stat"] == "Not_ok":
        DB_Data = AB_Screener_DB().select_query(Props.DB_TBL_HIGH_VOL_OPT)
        HOI_DF  = pd.DataFrame(DB_Data["data"],columns=DB_Data["column"])
        HOI_DF.columns = HOI_DF.columns.str.lower()
        HOI_DF["expdate"] = pd.to_datetime(HOI_DF["expdate"]).apply(lambda x: x.date())
        HOI_DF["updtime"] = pd.to_datetime(HOI_DF["updtime"])
    else:
        HOI_DF = pd.DataFrame(cache_data["data"])
        HOI_DF["expdate"] = pd.to_datetime(HOI_DF["expdate"],unit="ms").apply(lambda x: x.date())
        HOI_DF["updtime"] = pd.to_datetime(HOI_DF["updtime"], unit="ms")

    HOI_DF = HOI_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint']]


    if input_symbol and input_expiry:
        output_DF = HOI_DF[(HOI_DF["symbol"]==input_symbol)&(HOI_DF["expdate"]==input_expiry)].reset_index(drop=True)
    elif input_symbol:
        output_DF = HOI_DF[(HOI_DF["symbol"]==input_symbol)].sort_values("expdate").reset_index(drop=True)
    elif input_instype:
        output_DF = HOI_DF[(HOI_DF["instname"] == input_instype)].reset_index(drop=True)
    else:
        output_DF = HOI_DF

    output_DF["qty"] = output_DF["qty"].astype(float).astype(int)
    output_DF = output_DF.sort_values(by="qty", ascending=False).reset_index(drop=True)[0:20]
    output_DF["expdate"] = pd.to_datetime(output_DF["expdate"])

    output_DF["strikeprice"] = output_DF["strikeprice"].astype(str)
    bhav_df = pd.read_csv(Props.BHAVCOPY_SAVE_PATH + '/zip/%s_NFO_BHAV_COPY.csv.zip' % (datetime.now() - timedelta(days=Props.BHAVCOPY_DAYS)).strftime("%d%b%Y").upper())

    contract_master = pd.read_csv(Props.CONTRACT_SAVE_PATH+'%s_%s_FO.csv' % (datetime.now().strftime("%d%b%Y").upper(), "NFO"))
    contract_master = contract_master[(contract_master["instrument_type"] == "OPTSTK")|(contract_master["instrument_type"] == "OPTIDX")]

    # contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token"]]
    contract_master["strike_price"] = contract_master["strike_price"].astype(float).astype(int).astype(str)
    contract_master["expiry_date"] = pd.to_datetime(contract_master["expiry_date"])

    bhav_df.rename(columns={'SYMBOL': 'symbol', 'INSTRUMENT': 'instrument_type', 'EXPIRY_DT': 'expiry_date',
                            'OPTION_TYP': 'option_type', 'STRIKE_PR': 'strike_price', 'CLOSE':'pdc',"OPEN_INT":"pdoi"}, inplace=True)
    bhav_df["expiry_date"] = pd.to_datetime(bhav_df["expiry_date"])
    bhav_df["strike_price"] = bhav_df["strike_price"].astype(int).astype(str)

    # Filter Required Column in Dataframe
    contract_master = pd.merge(contract_master, bhav_df,
                            on=['instrument_type', 'symbol', 'expiry_date', 'strike_price', 'option_type'],
                            how='left')

    contract_master = contract_master[["symbol","strike_price","option_type","expiry_date","token","pdc","pdoi","lot_size","formatted_ins_name"]]


    output_DF = pd.merge(output_DF, contract_master, left_on=['symbol', 'strikeprice', 'opttype','expdate'], right_on=['symbol','strike_price','option_type','expiry_date'], how='left')
    output_DF["mktlot"] = output_DF["lot_size"]
    output_DF["instname"] = output_DF["formatted_ins_name"]
    output_DF = output_DF[['prevltp', 'ltp', 'faodiff', 'faochange', 'instname', 'symbol','expdate', 'strikeprice', 'opttype', 'updtime', 'qty', 'openinterest','chgopenint','token',"pdc","pdoi","mktlot"]]

    output_DF["expdate"] = output_DF["expdate"].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_DF["updtime"] = output_DF["updtime"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    output_DF["exch"] = "nse_fo"
    output_DF["token"] = output_DF["token"].astype(int).astype(str)
    output_data = output_DF.to_json(orient='records')

    output_response ={
        "stat": "ok",
        "data": json.loads(output_data)
    }

    return output_response

@app.route("/ab_screeners/gainerMCX", methods=['POST'])
def gainerMCX():
    """
            To Record the User IP and User Agent
            Author : Vigneshwar K
    """
    ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    User_agent = request.headers.get('User-Agent')

    """
            Quality checker of request given and share the proper error message
            Author: Vigneshwar K
    """
    request_quality = request_quality_checker(request)
    if request_quality["stat"] == "Not_ok":
        return request_quality

    input_data = request_quality["data"]

    input_exch = input_data["exch"] if "exch" in input_data else None
    input_symbol = input_data["underlying"] if "underlying" in input_data else None
    try:
        input_expiry = datetime.strptime(input_data["expiry"], "%d%b%y").date() if "expiry" in input_data else None
    except Exception as err:
        return {
            "stat":"Not_ok",
            "emsg":str(err)
        }
    input_instype = input_data["instrument_type"] if "instrument_type" in input_data else None

    cache_data = get_redis_cache("HIGHEST:GAIN:MCX")
    if cache_data["stat"] == "Not_ok":
        DB_Data = AB_Screener_DB().select_query(Props.DB_TBL_GAINER_MCX)
        HOI_DF  = pd.DataFrame(DB_Data["data"],columns=DB_Data["column"])

        HOI_DF.columns = HOI_DF.columns.str.lower()
        HOI_DF["exp_date"] = pd.to_datetime(HOI_DF["exp_date"]).apply(lambda x: x.date())
        HOI_DF["trd_date"] = pd.to_datetime(HOI_DF["trd_date"])
    else:
        HOI_DF = pd.DataFrame(cache_data["data"])

        HOI_DF["exp_date"] = pd.to_datetime(HOI_DF["exp_date"],unit="ms").apply(lambda x: x.date())
        HOI_DF["trd_date"] = pd.to_datetime(HOI_DF["trd_date"], unit="ms")

    HOI_DF = HOI_DF[['symbol', 'commname', 'category', 'unit', 'trd_date', 'exp_date','openprice', 'hprice', 'lprice', 'closeprice', 'prevclose','prevclosedate', 'volume', 'trdval', 'center', 'oi', 'prevoi', 'oidiff','oichange', 'diff', 'change', 'maxdate', 'market_lot','script_identifier', 'created_on']]


    if input_symbol and input_expiry:
        output_DF = HOI_DF[(HOI_DF["symbol"]==input_symbol)&(HOI_DF["exp_date"]==input_expiry)].reset_index(drop=True)
    elif input_symbol:
        output_DF = HOI_DF[(HOI_DF["symbol"]==input_symbol)].sort_values("exp_date").reset_index(drop=True)
    else:
        output_DF = HOI_DF
    pd.set_option('display.max_columns', None)


    # bhav_df = pd.read_csv(Props.BHAVCOPY_SAVE_PATH + '/zip/%s_NFO_BHAV_COPY.csv.zip' % (datetime.now() - timedelta(days=Props.BHAVCOPY_DAYS)).strftime("%d%b%Y").upper())
    #
    contract_master = pd.read_csv(Props.CONTRACT_SAVE_PATH+'%s_%s_FO.csv' % (datetime.now().strftime("%d%b%Y").upper(), "MCX"))

    contract_master = contract_master[(contract_master["instrument_type"] == "FUTCOM")|(contract_master["instrument_type"] == "FUTIDX")]
    contract_master["expiry_date"] = pd.to_datetime(contract_master["expiry_date"])

    contract_master["script_identifier"] = contract_master["symbol"]+contract_master["expiry_date"].dt.strftime("%y%b%d").str.upper()+"FUT"

    print(contract_master["script_identifier"])
    contract_master = contract_master[["script_identifier","lot_size","formatted_ins_name","token"]]
    output_DF = pd.merge(output_DF, contract_master, left_on=["script_identifier"], right_on=["script_identifier"], how='left')

    print(output_DF)
    output_DF["mktlot"] = output_DF["lot_size"]
    output_DF["instname"] = output_DF["formatted_ins_name"]

    output_DF["expdate"] = output_DF["exp_date"].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_DF["updtime"] = output_DF["trd_date"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    output_DF = output_DF[['symbol', 'commname', 'category', 'unit', 'updtime', 'expdate','openprice', 'hprice', 'lprice', 'closeprice', 'prevclose','prevclosedate', 'volume', 'trdval', 'center', 'oi', 'prevoi', 'oidiff','oichange', 'diff', 'change', 'maxdate', 'market_lot','script_identifier', 'created_on','instname','token','mktlot']]


    output_DF["exch"] = "nse_fo"
    output_DF["token"] = output_DF["token"].astype(int).astype(str)
    output_data = output_DF.to_json(orient='records')

    output_response ={
        "stat": "ok",
        "data": json.loads(output_data)
    }

    return output_response

@app.route("/ab_screeners/loserMCX", methods=['POST'])
def loserMCX():
    """
            To Record the User IP and User Agent
            Author : Vigneshwar K
    """
    ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    User_agent = request.headers.get('User-Agent')

    """
            Quality checker of request given and share the proper error message
            Author: Vigneshwar K
    """
    request_quality = request_quality_checker(request)
    if request_quality["stat"] == "Not_ok":
        return request_quality

    input_data = request_quality["data"]

    input_exch = input_data["exch"] if "exch" in input_data else None
    input_symbol = input_data["underlying"] if "underlying" in input_data else None
    try:
        input_expiry = datetime.strptime(input_data["expiry"], "%d%b%y").date() if "expiry" in input_data else None
    except Exception as err:
        return {
            "stat":"Not_ok",
            "emsg":str(err)
        }
    input_instype = input_data["instrument_type"] if "instrument_type" in input_data else None

    cache_data = get_redis_cache("HIGHEST:LOSS:MCX")
    if cache_data["stat"] == "Not_ok":
        DB_Data = AB_Screener_DB().select_query(Props.DB_TBL_LOSER_MCX)
        HOI_DF  = pd.DataFrame(DB_Data["data"],columns=DB_Data["column"])

        HOI_DF.columns = HOI_DF.columns.str.lower()
        HOI_DF["exp_date"] = pd.to_datetime(HOI_DF["exp_date"]).apply(lambda x: x.date())
        HOI_DF["trd_date"] = pd.to_datetime(HOI_DF["trd_date"])
    else:
        HOI_DF = pd.DataFrame(cache_data["data"])

        HOI_DF["exp_date"] = pd.to_datetime(HOI_DF["exp_date"],unit="ms").apply(lambda x: x.date())
        HOI_DF["trd_date"] = pd.to_datetime(HOI_DF["trd_date"], unit="ms")

    HOI_DF = HOI_DF[['symbol', 'commname', 'category', 'unit', 'trd_date', 'exp_date','openprice', 'hprice', 'lprice', 'closeprice', 'prevclose','prevclosedate', 'volume', 'trdval', 'center', 'oi', 'prevoi', 'oidiff','oichange', 'diff', 'change', 'maxdate', 'market_lot','script_identifier', 'created_on']]


    if input_symbol and input_expiry:
        output_DF = HOI_DF[(HOI_DF["symbol"]==input_symbol)&(HOI_DF["exp_date"]==input_expiry)].reset_index(drop=True)
    elif input_symbol:
        output_DF = HOI_DF[(HOI_DF["symbol"]==input_symbol)].sort_values("exp_date").reset_index(drop=True)
    else:
        output_DF = HOI_DF
    pd.set_option('display.max_columns', None)


    # bhav_df = pd.read_csv(Props.BHAVCOPY_SAVE_PATH + '/zip/%s_NFO_BHAV_COPY.csv.zip' % (datetime.now() - timedelta(days=Props.BHAVCOPY_DAYS)).strftime("%d%b%Y").upper())
    #
    contract_master = pd.read_csv(Props.CONTRACT_SAVE_PATH+'%s_%s_FO.csv' % (datetime.now().strftime("%d%b%Y").upper(), "MCX"))

    contract_master = contract_master[(contract_master["instrument_type"] == "FUTCOM")|(contract_master["instrument_type"] == "FUTIDX")]
    contract_master["expiry_date"] = pd.to_datetime(contract_master["expiry_date"])

    contract_master["script_identifier"] = contract_master["symbol"]+contract_master["expiry_date"].dt.strftime("%y%b%d").str.upper()+"FUT"

    print(contract_master["script_identifier"])
    contract_master = contract_master[["script_identifier","lot_size","formatted_ins_name","token"]]
    output_DF = pd.merge(output_DF, contract_master, left_on=["script_identifier"], right_on=["script_identifier"], how='left')

    print(output_DF)
    output_DF["mktlot"] = output_DF["lot_size"]
    output_DF["instname"] = output_DF["formatted_ins_name"]

    output_DF["expdate"] = output_DF["exp_date"].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_DF["updtime"] = output_DF["trd_date"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    output_DF = output_DF[['symbol', 'commname', 'category', 'unit', 'updtime', 'expdate','openprice', 'hprice', 'lprice', 'closeprice', 'prevclose','prevclosedate', 'volume', 'trdval', 'center', 'oi', 'prevoi', 'oidiff','oichange', 'diff', 'change', 'maxdate', 'market_lot','script_identifier', 'created_on','instname','token','mktlot']]


    output_DF["exch"] = "nse_fo"
    output_DF["token"] = output_DF["token"].astype(int).astype(str)
    output_data = output_DF.to_json(orient='records')

    output_response ={
        "stat": "ok",
        "data": json.loads(output_data)
    }

    return output_response

@app.route("/ab_screeners/volumeMCX", methods=['POST'])
def volumeMCX():
    """
            To Record the User IP and User Agent
            Author : Vigneshwar K
    """
    ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    User_agent = request.headers.get('User-Agent')

    """
            Quality checker of request given and share the proper error message
            Author: Vigneshwar K
    """
    request_quality = request_quality_checker(request)
    if request_quality["stat"] == "Not_ok":
        return request_quality

    input_data = request_quality["data"]

    input_exch = input_data["exch"] if "exch" in input_data else None
    input_symbol = input_data["underlying"] if "underlying" in input_data else None
    try:
        input_expiry = datetime.strptime(input_data["expiry"], "%d%b%y").date() if "expiry" in input_data else None
    except Exception as err:
        return {
            "stat":"Not_ok",
            "emsg":str(err)
        }
    input_instype = input_data["instrument_type"] if "instrument_type" in input_data else None

    cache_data = get_redis_cache("HIGHEST:VOL:MCX")
    if cache_data["stat"] == "Not_ok":
        DB_Data = AB_Screener_DB().select_query(Props.DB_TBL_VOLUME_MCX)
        HOI_DF  = pd.DataFrame(DB_Data["data"],columns=DB_Data["column"])

        HOI_DF.columns = HOI_DF.columns.str.lower()
        HOI_DF["exp_date"] = pd.to_datetime(HOI_DF["exp_date"]).apply(lambda x: x.date())
        HOI_DF["trd_date"] = pd.to_datetime(HOI_DF["trd_date"])
    else:
        HOI_DF = pd.DataFrame(cache_data["data"])

        HOI_DF["exp_date"] = pd.to_datetime(HOI_DF["exp_date"],unit="ms").apply(lambda x: x.date())
        HOI_DF["trd_date"] = pd.to_datetime(HOI_DF["trd_date"], unit="ms")

    HOI_DF = HOI_DF[['symbol', 'commname', 'category', 'unit', 'trd_date', 'exp_date','openprice', 'hprice', 'lprice', 'closeprice', 'prevclose','prevclosedate', 'volume', 'trdval', 'center', 'oi', 'prevoi', 'oidiff','oichange', 'diff', 'change', 'maxdate', 'market_lot','script_identifier', 'created_on']]


    if input_symbol and input_expiry:
        output_DF = HOI_DF[(HOI_DF["symbol"]==input_symbol)&(HOI_DF["exp_date"]==input_expiry)].reset_index(drop=True)
    elif input_symbol:
        output_DF = HOI_DF[(HOI_DF["symbol"]==input_symbol)].sort_values("exp_date").reset_index(drop=True)
    else:
        output_DF = HOI_DF
    pd.set_option('display.max_columns', None)


    # bhav_df = pd.read_csv(Props.BHAVCOPY_SAVE_PATH + '/zip/%s_NFO_BHAV_COPY.csv.zip' % (datetime.now() - timedelta(days=Props.BHAVCOPY_DAYS)).strftime("%d%b%Y").upper())
    #
    contract_master = pd.read_csv(Props.CONTRACT_SAVE_PATH+'%s_%s_FO.csv' % (datetime.now().strftime("%d%b%Y").upper(), "MCX"))

    contract_master = contract_master[(contract_master["instrument_type"] == "FUTCOM")|(contract_master["instrument_type"] == "FUTIDX")]
    contract_master["expiry_date"] = pd.to_datetime(contract_master["expiry_date"])

    contract_master["script_identifier"] = contract_master["symbol"]+contract_master["expiry_date"].dt.strftime("%y%b%d").str.upper()+"FUT"


    contract_master = contract_master[["script_identifier","lot_size","formatted_ins_name","token"]]
    output_DF = pd.merge(output_DF, contract_master, left_on=["script_identifier"], right_on=["script_identifier"], how='left')


    output_DF["mktlot"] = output_DF["lot_size"]
    output_DF["instname"] = output_DF["formatted_ins_name"]

    output_DF["expdate"] = output_DF["exp_date"].apply(lambda x: x.strftime('%Y-%m-%d'))
    output_DF["updtime"] = output_DF["trd_date"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    output_DF = output_DF[['symbol', 'commname', 'category', 'unit', 'updtime', 'expdate','openprice', 'hprice', 'lprice', 'closeprice', 'prevclose','prevclosedate', 'volume', 'trdval', 'center', 'oi', 'prevoi', 'oidiff','oichange', 'diff', 'change', 'maxdate', 'market_lot','script_identifier', 'created_on','instname','token','mktlot']]


    output_DF["exch"] = "nse_fo"
    output_DF["token"] = output_DF["token"].astype(int).astype(str)
    output_data = output_DF.to_json(orient='records')

    output_response ={
        "stat": "ok",
        "data": json.loads(output_data)
    }

    return output_response

# ---------------------------------------------------------------------------------------------------------------------- Opening Bell API

@app.route("/market_bell_info", methods=['POST'])
def market_bell_info():
    try:
        data = request.get_json()
    except:
        return {
            "stat":"Not_ok",
            "emsg":"Please check your input format"
        }

    market = data["market_type"]
    date = datetime.strptime(data["date"], "%Y-%m-%d")

    if market not in ["PRE_MARKET","NOON_MARKET","AFTER_MARKET"]:
        return {
            "stat": "Not_ok",
            "emsg": "Invalid Market Type"
        }

    key = market+"_"+date.strftime("%d%m%y")
    data = redis_connect.get(key)
    data = json.loads(data.decode('utf-8'))
    data["data"] = json.loads(data["data"])
    message_df = pd.read_excel("/home/client_engagement_report/static/message/ClientMessage.xlsx")
    data_checker_df = pd.DataFrame(data["data"])
    pchange = data_checker_df[data_checker_df["symbol"]=="NIFTY 50"].reset_index(drop=True)["pc"][0]
    if pchange >= 0:
        message = random.choice(message_df["Positive"].tolist())
    else:
        message = random.choice(message_df["Negative"].tolist())
    if not data:
        mydb = mysql.connector.connect(
            host=Props.DB_HOST,
            port=Props.DB_PORT,
            user=Props.DB_USER,
            password=Props.DB_PASS,
            database="client_engagement_db"
        )

        mycursor = mydb.cursor()

        mycursor.execute("SELECT * FROM client_engagement_db.tbl_market_bell_data where manage_key='%s';"%(key))
        column_name = [i[0] for i in mycursor.description]
        myresult = mycursor.fetchall()

        data = pd.DataFrame(myresult, columns=column_name)
        data = data[['symbol', 'pc', 'flag', 'market', 'update_date', 'key', 'market_move_tag', 'market_move_symbol', 'market_move_pc']]
        data = data.to_json(orient="records")
    data["msg"] = message
    return data

@app.route("/market_bell_info_fcm", methods=['POST'])
def market_bell_info_fcm():
    try:
        data = request.get_json()
    except:
        return {
            "stat":"Not_ok",
            "emsg":"Please check your input format"
        }

    market = data["market_type"]
    date = datetime.strptime(data["date"], "%Y-%m-%d")

    if market not in ["PRE_MARKET","NOON_MARKET","AFTER_MARKET"]:
        return {
            "stat": "Not_ok",
            "emsg": "Invalid Market Type"
        }

    key = market+"_"+date.strftime("%d%m%y")
    data = redis_connect.get(key)
    # print(key)
    if data:
        # print("From Cache")
        data = json.loads(data.decode('utf-8'))
        data["data"] = json.loads(data["data"])
    else:
        # print("From DB")
        mydb = mysql.connector.connect(
            host=Props.DB_HOST,
            port=Props.DB_PORT,
            user=Props.DB_USER,
            password=Props.DB_PASS,
            database="client_engagement_db"
        )

        mycursor = mydb.cursor()

        # print("SELECT * FROM client_engagement_db.tbl_market_bell_data where manage_key='%s';"%(key))
        mycursor.execute("SELECT * FROM client_engagement_db.tbl_market_bell_data where manage_key='%s';"%(key))
        column_name = [i[0] for i in mycursor.description]
        myresult = mycursor.fetchall()

        data = pd.DataFrame(myresult, columns=column_name)
        data = data[['symbol','price', 'pc', 'flag', 'market', 'update_date', 'manage_key', 'market_move_tag', 'market_move_symbol','market_move_price', 'market_move_pc']].sort_values("update_date",ascending=False).drop_duplicates("symbol").reset_index(drop=True)
        response_data = {
            "data":json.loads(data[['symbol','price' ,'pc', 'flag', 'market', 'update_date']].to_json(orient="records"))
        }
        response_data["market_move_tag"] = data["market_move_tag"][0]
        response_data["market_move_symbol"] = data["market_move_symbol"][0]
        response_data["market_move_price"] = data["market_move_price"][0]
        response_data["market_move_pc"] = data["market_move_pc"][0]
        data = response_data


    message_df = pd.read_excel("/home/client_engagement_report/static/message/ClientMessage.xlsx")
    title_df = pd.read_csv("/home/client_engagement_report/static/message/title.csv")
    data_checker_df = pd.DataFrame(data["data"])
    pchange = data_checker_df[data_checker_df["symbol"] == "NIFTY 50"].reset_index(drop=True)["pc"][0]
    if float(pchange) >= 0:
        message = random.choice(message_df["Positive"].tolist())
        title = "%s : %s"%(market.replace("_"," "),random.choice(title_df["Gloom"].tolist()))
    else:
        message = random.choice(message_df["Negative"].tolist())
        title = "%s : %s" % (market.replace("_", " "), random.choice(title_df["Boom"].tolist()))

    list_data_df = pd.DataFrame(data["data"])
    list_data_df.loc[list_data_df["pc"].astype(float)>=0,"trend"] = "⬆️"
    list_data_df.loc[list_data_df["pc"].astype(float)< 0,"trend"] = "⬇️"
    # print(data)
    NIFTY = list_data_df[list_data_df["symbol"] == "NIFTY 50"].reset_index(drop=True)
    BANKNIFTY = list_data_df[list_data_df["symbol"] == "NIFTY BANK"].reset_index(drop=True)
    SENSEX = list_data_df[list_data_df["symbol"] == "SENSEX"].reset_index(drop=True)
    FINNIFTY = list_data_df[list_data_df["symbol"] == "NIFTY FIN SERVICE"].reset_index(drop=True)
    NIFTYMID = list_data_df[list_data_df["symbol"] == "NIFTY MIDCAP SELECT"].reset_index(drop=True)
    NIFTYSML = list_data_df[list_data_df["symbol"] == "NIFTY SMLCAP 50"].reset_index(drop=True)
    print("------------------------------------------------------------------------------------------------------------")
    text =[]
    if not NIFTY.empty:
        text.append("NIFTY %s (%s%%) %s"%(NIFTY["trend"][0], NIFTY["pc"][0], NIFTY["price"][0]))
    if not BANKNIFTY.empty:
        text.append("BANK NIFTY %s (%s%%) %s"%(BANKNIFTY["trend"][0], BANKNIFTY["pc"][0],BANKNIFTY["price"][0]))
    if not SENSEX.empty:
        text.append("SENSEX %s (%s%%) %s," % (SENSEX["trend"][0], SENSEX["pc"][0], SENSEX["price"][0]))
    if not FINNIFTY.empty:
        text.append("FINNIFTY %s (%s%%) %s," % (FINNIFTY["trend"][0], FINNIFTY["pc"][0], FINNIFTY["price"][0]))
    if not NIFTYMID.empty:
        text.append("NIFTY MIDCAP %s (%s%%) %s," % (NIFTYMID["trend"][0], NIFTYMID["pc"][0], NIFTYMID["price"][0]))
    if not NIFTYSML.empty:
        text.append("NIFTY SMLCAP %s (%s%%) %s" % (NIFTYSML["trend"][0], NIFTYSML["pc"][0], NIFTYSML["price"][0]))
    text.append(message)
    text.append("NIFTY 50's %s - %s %s (%s%%) %s" % (data["market_move_tag"], data["market_move_symbol"], NIFTY["trend"][0], data["market_move_pc"],data["market_move_price"]))

    final_message = text[0]
    for i in range(1,len(text)):
        final_message = final_message+"\n"+text[i]
    print(final_message)
    print("------------------------------------------------------------------------------------------------------------")

    return {
        "title":title,
        "msg":final_message,
        "stat":"ok"
    }

# ---------------------------------------------------------------------------------------------------------------------- FII DII API

@app.route("/fii_dii", methods=['POST'])
def fii_dii():
    data = request.get_json()
    from_Date = data["from"]
    if len(from_Date) == 0:
        DB_Data = fii_dii_DB().select_query(0)
    else:
        from_Date = datetime.strptime(from_Date,"%Y-%m-%d")
        DB_Data = fii_dii_DB().select_query(from_Date)
    DF_Data = pd.DataFrame(DB_Data["data"],columns=DB_Data["column"])
    DF_Data["net"]= DF_Data["net"].str.replace(",","").astype(float).round(2)
    Created_On = DF_Data["created_on"].drop_duplicates()
    if len(Created_On) == 1:
        data = DF_Data[["exch","name","category","segment","gross_purchases","gross_sales","net"]]
        data_json = {
            "stat":"ok",
            "data":json.loads(data.to_json(orient="records")),
            "created_on":Created_On[0].strftime("%Y-%m-%d")
        }
    else:
        data_json ={
            "stat":"Not_ok",
            "data":[],
            "created_on":None
        }
    return data_json


if __name__ == "__main__":
    #     app.run(threaded=True,host="192.168.0.113")
    app.run()