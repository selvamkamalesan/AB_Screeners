from modules.libraries import *

redis_connect = redis.Redis(
    host=Props.REDIS_HOST,
    port=Props.REDIS_PORT)

def save_redis(key,data):
    redis_connect.set(key, data)
    return 'Ok'

def get_redis_cache(key):
    data = redis_connect.get(key)
    if data:
        return {
            "stat":"ok",
            "data":json.loads(data)
        }
    else:
        return {
            "stat":"Not_ok",
            "data":"No Data"
        }