import os
import time
import datetime as dt
import requests
from pytrends.request import TrendReq

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_ANON_KEY"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

COUNTRY_TO_GEO = {
    "DK": "DK",
}

def supabase_get_keywords(limit=20):
    # limit for at undgå at blive throttled af Google i starten
    url = f"{SUPABASE_URL}/rest/v1/keywords?select=keyword,topic,country&limit={limit}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def supabase_insert_trends(rows):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/trends"
    r = requests.post(url, headers=HEADERS, json=rows, timeout=30)
    # Supabase kan svare 201/204 ved succes afhængigt af prefs
    if r.status_code not in (200, 201, 204):
        raise RuntimeError(f"Insert failed: {r.status_code} {r.text}")

def fetch_interest_over_time(pytrends, keyword, geo):
    pytrends.build_payload([keyword], timeframe="today 12-m", geo=geo)
    df = pytrends.interest_over_time()
    if df is None or df.empty:
        return []
    if "isPartial" in df.columns:
        df = df.drop(columns=["isPartial"])

    out = []
    for date, row in df.iterrows():
        # date kan være pandas Timestamp
        interest = int(row[keyword])
        out.append({
            "keyword": keyword,
            "date": str(date.date()),
            "interest": interest
        })
    return out

def main():
    keywords = supabase_get_keywords(limit=10)  # start småt
    print(f"Fetched {len(keywords)} keywords")

    pytrends = TrendReq(hl="da-DK", tz=60)

    total_inserted = 0

    for i, k in enumerate(keywords, start=1):
        keyword = k["keyword"]
        country = (k.get("country") or "DK").upper()
        geo = COUNTRY_TO_GEO.get(country, "DK")

        print(f"[{i}/{len(keywords)}] Fetching trends for: {keyword} ({geo})")

        rows = fetch_interest_over_time(pytrends, keyword, geo)

        # Tilføj created_at automatisk af DB, vi sender kun de nødvendige felter
        supabase_insert_trends(rows)
        total_inserted += len(rows)

        print(f"Inserted {len(rows)} rows for {keyword}")

        time.sleep(3)  # vigtigt: undgå rate limiting

    print(f"DONE. Total rows inserted: {total_inserted}")

if __name__ == "__main__":
    main()
