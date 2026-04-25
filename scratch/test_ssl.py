import urllib.request
import ssl

try:
    urllib.request.urlopen("https://vaidikaidata.blob.core.windows.net", timeout=5)
    print("SSL Connection Successful")
except Exception as e:
    print(f"SSL Connection Failed: {e}")
