from modules.libraries import *

config = configparser.ConfigParser()
config.read('abs.live.properties')

class Props():

    # -----------------------------------------------------------------------------------------------------------------> CMOTS API
    CMOTS_AUTH_TOKEN = config.get("CMOTS", "cmots.auth_token")

    CMOTS_BASEURL = config.get("CMOTS", "cmots.baseurl")
    CMOTS_JWT_BASEURL = config.get("CMOTS", "cmots_jwt.baseurl")

    CMOTS_ENDPOINT_HIGHEST_OI_FUT = config.get("CMOTS", "cmots_endpoint.highest_oi_fut")
    CMOTS_ENDPOINT_HIGHEST_OI_OPT = config.get("CMOTS", "cmots_endpoint.highest_oi_opt")
    CMOTS_ENDPOINT_GAINER_OI_FUT = config.get("CMOTS", "cmots_endpoint.gainer_oi_fut")
    CMOTS_ENDPOINT_GAINER_OI_OPT = config.get("CMOTS", "cmots_endpoint.gainer_oi_opt")
    CMOTS_ENDPOINT_LOOSER_OI_FUT = config.get("CMOTS", "cmots_endpoint.looser_oi_fut")
    CMOTS_ENDPOINT_LOOSER_OI_OPT = config.get("CMOTS", "cmots_endpoint.looser_oi_opt")
    CMOTS_ENDPOINT_HIGHER_VOLUME = config.get("CMOTS", "cmots_endpoint.higher_volume")
    CMOTS_ENDPOINT_MCX_GAINER    = config.get("CMOTS", "cmots_endpoint.mcx_gainer")
    CMOTS_ENDPOINT_MCX_LOSER     = config.get("CMOTS", "cmots_endpoint.mcx_loser")
    CMOTS_ENDPOINT_MCX_VOLUME    = config.get("CMOTS", "cmots_endpoint.mcx_volume")

    # -----------------------------------------------------------------------------------------------------------------> Database
    DB_HOST = config.get("DATABASE", "db.host")
    DB_PORT = config.getint("DATABASE", "db.port")
    DB_USER = config.get("DATABASE", "db.user")
    DB_PASS = config.get("DATABASE", "db.pass")
    DB_SCHEMA = config.get("DATABASE", "db.schemaName")

    # -----------------------------------------------------------------------------------------------------------------> Chart Database
    DB_HOST_FII_DII = config.get("DATABASE", "db.host_fiidii")
    DB_PORT_FII_DII = config.getint("DATABASE", "db.port_fiidii")
    DB_USER_FII_DII = config.get("DATABASE", "db.user_fiidii")
    DB_PASS_FII_DII = config.get("DATABASE", "db.pass_fiidii")
    DB_SCHEMA_FII_DII = config.get("DATABASE", "db.schemaName_fiidii")

    DB_TBL_HIGH_OI_FUT = config.get("DATABASE", "db.oi_high_fut")
    DB_TBL_HIGH_OI_OPT = config.get("DATABASE", "db.oi_high_opt")
    DB_TBL_GAIN_OI_FUT = config.get("DATABASE", "db.oi_gain_fut")
    DB_TBL_GAIN_OI_OPT = config.get("DATABASE", "db.oi_gain_opt")
    DB_TBL_LOSS_OI_FUT = config.get("DATABASE", "db.oi_loss_fut")
    DB_TBL_LOSS_OI_OPT = config.get("DATABASE", "db.oi_loss_opt")
    DB_TBL_HIGH_VOL_FUT = config.get("DATABASE", "db.vol_high_fut")
    DB_TBL_HIGH_VOL_OPT = config.get("DATABASE", "db.vol_high_opt")
    DB_TBL_GAINER_MCX = config.get("DATABASE", "db.gainer_mcx")
    DB_TBL_LOSER_MCX = config.get("DATABASE", "db.loser_mcx")
    DB_TBL_VOLUME_MCX = config.get("DATABASE", "db.loser_mcx")

    # ------------------------------------------------------------------------------------------------------------------ Redis
    REDIS_HOST = config.get('REDIS', 'redis.host')
    REDIS_PORT = config.get('REDIS', 'redis.port')

    NSE_INDICES_SCRIPTS = config.get('COMMON', 'nse.indices')
    CONTRACT_SAVE_PATH = config.get('COMMON', 'contractfile.savepath')
    BHAVCOPY_SAVE_PATH = config.get('COMMON', 'bhavfile.savepath')
    BHAVCOPY_DAYS = config.getint('COMMON', 'bhavfile.day')

    APP_LOG = config.get('COMMON', 'app.logsavepath')
    SERVICE_LOG = config.get('COMMON', 'service.logsavepath')
