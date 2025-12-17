import os
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_ANON_KEY"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

def main():
    url = f"{SUPABASE_URL}/rest/v1/keywords?select=keyword,topic,country"
    r = requests.get(url, headers=HEADERS, timeout=30)
    print("Status:", r.status_code)
    print("Response:", r.text[:2000])  # print lidt, ikke uendeligt

if __name__ == "__main__":
    main()
