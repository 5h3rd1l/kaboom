from kaboom.core import *
from kaboom.localuseragent import *

async def weatherwalay(phone, client, out):
    name = "weatherwalay"
    domain = "weatherwalay"
    frequent_rate_limit=False
    headers = {
        'Authorization': 'Basic eHl3d19BdXRoLSMyMDIzIXo6d2VAdGhlcl9XZWIlMjBQbGFu',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    }

    json_data = {
        'phone': f'0{phone[-10:]}',
    }
    
    response = await client.post('https://app.weatherwalay.com/webapp/otp/send-otp', headers=headers, json=json_data)
    try:
        data=response.json()
        if data["success"] == True and data["msg"] == "OTP has been sent to your number":
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": True, "error": False})
            return None
        elif data["success"] == False and data["msg"] == "Please generate otp after 2 mins":
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": True,"sent": False, "error": False, "reason": data.get("msg")})
            return None
        else:
            reason = data.get("msg") or data.get("message") or "otp request failed"
            reason_text = str(reason).lower()
            is_rate_limited = "otp after" in reason_text or "try again" in reason_text or "wait" in reason_text or "limit" in reason_text
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": is_rate_limited,"sent": False, "error": not is_rate_limited, "reason": reason})
            return None
    except Exception as exc:
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": False, "sent": False, "error": True, "reason": str(exc) or type(exc).__name__})
        return None
