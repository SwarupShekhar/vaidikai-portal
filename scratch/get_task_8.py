import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

ls_url = "http://127.0.0.1:4006"
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6MTc3ODc2MDY2MCwiaWF0IjoxNzc4Njc0MjYwLCJqdGkiOiJjMGQzYWI5NjA1MjY0ZjA1OTQ2ZWJlOGQ1YzhlZTYzMiIsInVzZXJfaWQiOiIxIn0.yVvzwAAv45_mG1juPpEpgEzwvHcpK8sDoNhSmAtXMA8"

# Resolve token
def _resolve_token(token, url):
    if not token or not token.startswith("eyJ"):
        print("Token does not start with eyJ!")
        return token
    try:
        r = requests.post(f"{url}/api/token/refresh", json={"refresh": token}, timeout=15)
        print(f"Refresh response code: {r.status_code}")
        if r.status_code == 200:
            access = r.json()["access"]
            print(f"Refresh succeeded, access token: {access[:15]}...")
            return access
        else:
            print(f"Refresh failed response: {r.text}")
    except Exception as e:
        print(f"Refresh failed with error: {e}")
    return token

access_token = _resolve_token(api_key, ls_url)
prefix = "Token" if not access_token.startswith("eyJ") else "Bearer"
headers = {"Authorization": f"{prefix} {access_token}"}

print(f"Connecting to {ls_url}...")
r = requests.get(f"{ls_url}/api/tasks/8", headers=headers)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    print(json.dumps(r.json(), indent=2))
else:
    print(r.text)
