import requests
import json

url = "https://annotate.vaidik.ai"
email = "swarup.shekhar@vaidik.com"
password = "Tardigrade@1234"

print(f"Attempting to login to {url} as {email}...")

session = requests.Session()
# 1. Fetch CSRF token
r_get = session.get(f"{url}/user/login/")
csrf_token = session.cookies.get("csrftoken") or ""
print(f"Fetched CSRF: {csrf_token[:10]}...")

# 2. POST login
payload = {
    "email": email,
    "password": password,
    "csrfmiddlewaretoken": csrf_token
}
headers = {
    "Referer": f"{url}/user/login/"
}
r_login = session.post(f"{url}/user/login/", data=payload, headers=headers)
print(f"Login status: {r_login.status_code}")

# 3. Fetch API key from user page
r_api = session.get(f"{url}/api/current-user/whoami")
print(f"Whoami response: {r_api.status_code}")
if r_api.status_code == 200:
    user_info = r_api.json()
    print("User Info:", json.dumps(user_info, indent=2))
    # Let's get the token
    token_url = f"{url}/api/current-user/token"
    r_token = session.get(token_url)
    print(f"Token endpoint status: {r_token.status_code}")
    print("Token Data:", r_token.text)
else:
    print(r_api.text)
