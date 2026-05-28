from kaboom.core import *
from kaboom.localuseragent import *

async def sportsx(phone, client, out):
    name = "sportsx"
    domain = "sportsx"
    frequent_rate_limit=False

    headers = {
        'Content-Type': 'application/json',
        'User-Agent': random.choice(ua["browsers"]["chrome"]),
        'Origin': 'https://sportsx.mobi',
        'Referer': 'https://sportsx.mobi/',
    }

    json_data = {
        'msisdn': f'0{phone[-10:]}',
    }

    response = await client.post('https://server.sportsx.mobi/user/login/', headers=headers, json=json_data)
    try:
        data=response.json()
        if data['message'] == "otp sent!":
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": True, "error": False})
            return None
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": False, "error": True, "reason": data.get("message") or "otp request failed"})
        return None
    except Exception as exc:
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": False, "sent": False, "error": True, "reason": str(exc) or type(exc).__name__})
        return None
