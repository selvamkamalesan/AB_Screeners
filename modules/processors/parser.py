from modules.libraries import *

def CMOTS_API_Request(JWT,Endpoint):
    if JWT:
        url = Props.CMOTS_JWT_BASEURL + Endpoint
    else:
        url = Props.CMOTS_BASEURL + Endpoint
    print(url)
    headers = {
        'Authorization': 'Bearer %s'%Props.CMOTS_AUTH_TOKEN
    }
    try:
        response = requests.request("GET", url, headers=headers)
        if response.status_code == 200:
            return {
                "stat":"ok",
                "data": response.json()
            }
        else:
            return {
                "stat": "Not_ok",
                "msg": "Error Code : %s"%response.status_code,
                "data": response.json()
            }
    except requests.exceptions.RequestException as e:
        return {
            "stat":"Not_ok",
            "msg":e,
            "data":"Exception Error in CMOTS Request"
        }
