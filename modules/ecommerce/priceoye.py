from kaboom.core import *
from kaboom.localuseragent import *

async def priceoye(phone, client, out):
    name = "priceoye"
    domain = "priceoye"
    frequent_rate_limit=False

    random_srsltid = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=37))
    random_srsltid=f'{random_srsltid}-KO7_brX774PEOe0nyz'
    params = {
        'srsltid': random_srsltid,
    }
    headers = {
        'User-Agent': random.choice(ua["browsers"]["chrome"])
    }

    response = await client.get('https://priceoye.pk/', params=params, headers=headers)
    XSRF_TOKEN = response.cookies.get('XSRF-TOKEN')
    po_session = response.cookies.get('po_session')
    csrf_match = re.search(r'<meta name="csrf-token" content="(.+?)">', response.text)
    if not XSRF_TOKEN or not po_session or not csrf_match:
        out.append({"name": name, "domain": domain, "frequent_rate_limit": frequent_rate_limit, "rateLimit": False, "sent": False, "error": True, "reason": "session token not found"})
        return None
    csrf_token = csrf_match.group(1)
    cookies = {
        'XSRF-TOKEN': XSRF_TOKEN,
        'po_session': po_session
    }


    data = {
        'shopper_phone': f"+92{phone[-10:]}",
        '_token': csrf_token,
    }

    response = await client.post(
        'https://priceoye.pk/shoppers/generate_shopper_otp',
        cookies=cookies,
        headers=headers,
        data=data
    )
    try:
        data=response.json()
        response_message = data.get("response", "")
        if response_message == "OTP send successfully" or (
            data.get("msg") == "ok" and "sent" in response_message.lower()
        ):
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit, "rateLimit": False,"sent": True, "error": False})
            return None
        elif response_message == "OTP already sent, please Resend Code after 5 Minutes.":
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": True, "sent": False, "error": False})
            return None
        elif "too many" in response_message.lower() or "check back after" in response_message.lower():
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": True, "sent": False, "error": False, "reason": response_message})
            return None
        else:
            out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": False, "sent": False, "error": True, "reason": response_message or data.get("msg", "otp request failed")})
            return None
    except Exception:
        out.append({"name": name,"domain":domain,"frequent_rate_limit":frequent_rate_limit,  "rateLimit": False, "sent": False, "error": True, "reason": "invalid response"})
        return None
