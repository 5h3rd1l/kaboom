from kaboom.core import *
from kaboom.localuseragent import *

async def nayabazaar(phone, client, out):
    name = "nayabazaar"
    domain = "nayabazaar"
    frequent_rate_limit=False

        
    headers = {
        'sec-ch-ua-platform': '"Windows"',
        'Referer': 'https://nayabazar.pk/login?continue=%2Fproduct%2Foriginal-toshiba-aa-size-heavy-duty-cell-medium-P7PI4gNusyvi9hDNveRBHcPvUlUwIe',
        'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        'sec-ch-ua-mobile': '?0',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    }

    data = {
        'validationValue': f"0{phone[-10:]}",
        'validateAcount': 'true',
        'validationKey': 'phone',
    }

    response = await client.post('https://nayabazar.pk/controllers/login-by-phone', headers=headers, data=data)
    try:
        data=response.json()
        if data["status"] == "success" and data["otpSent"] == True:
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": True, "error": False})
            return None
        if data["status"] == "error" and data["data"] == "Aap aik minute mein aik say ziada OTP send nahi kar sakty!":
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": True,"sent": False, "error": False, "reason": data.get("data")})
            return None
        reason = data.get("data") or data.get("message") or "otp request failed"
        reason_text = str(reason).lower()
        is_rate_limited = "too many" in reason_text or "more than" in reason_text or "wait" in reason_text or "minute" in reason_text or "hour" in reason_text
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": is_rate_limited,"sent": False, "error": not is_rate_limited, "reason": reason})
        return None
    except Exception as exc:
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": False, "sent": False, "error": True, "reason": str(exc) or type(exc).__name__})
        return None
