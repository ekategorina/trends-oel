import os
import time
import random
import datetime as dt
import requests
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_ANON_KEY"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

COUNTRY_TO_GEO = {"DK": "DK"}

def supabase_get_keywords(limit=5):
    url = f"{SUPABASE_URL}/rest/v1/keywords?select=keyword,topic,country&limit={limit}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def supabase_insert_trends(rows):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/trends"
    r = requests.post(url, headers=HEADERS, json=rows, timeout=30)
    if r.status_code not in (200, 201, 204):
        raise RuntimeError(f"Insert failed: {r.status_code} {r.text}")

def fetch_interest_over_time_with_retry(pytrends, keyword, geo, max_attempts=5):
    # Exponential backoff + jitter
    for attempt in range(1, max_attempts + 1):
        try:
            pytrends.build_payload([keyword], timeframe="today 12-m", geo=geo)
            df = pytrends.interest_over_time()
            if df is None or df.empty:
                return []

            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])

            out = []
            for date, row in df.iterrows():
                out.append({
                    "keyword": keyword,
                    "date": str(date.date()),
                    "interest": int(row[keyword])
                })
            return out

        except TooManyRequestsError:
            sleep_s = (2 ** attempt) * 10 + random.randint(0, 7)  # 20s, 40s, 80s...
            print(f"429 TooManyRequests for '{keyword}'. Attempt {attempt}/{max_attempts}. Sleeping {sleep_s}s…")
            time.sleep(sleep_s)

    # If we get here, we failed all retries
    raise TooManyRequestsError("Too many requests even after retries")

def main():
    keywords = supabase_get_keywords(limit=5)  # start small
    print(f"Fetched {len(keywords)} keywords")

    pytrends = TrendReq(hl="da-DK", tz=60)

    total_inserted = 0
    failures = 0

    for i, k in enumerate(keywords, start=1):
        keyword = k["keyword"]
        country = (k.get("country") or "DK").upper()
        geo = COUNTRY_TO_GEO.get(country, "DK")

        print(f"[{i}/{len(keywords)}] Fetching trends for: {keyword} ({geo})")

        try:
            rows = fetch_interest_over_time_with_retry(pytrends, keyword, geo, max_attempts=5)
            supabase_insert_trends(rows)
            total_inserted += len(rows)
            print(f"Inserted {len(rows)} rows for {keyword}")

        except Exception as e:
            failures += 1
            print(f"FAILED keyword '{keyword}': {type(e).__name__}: {e}")

        # Always slow down between keywords
        base_sleep = 20
        jitter = random.randint(0, 10)
        time.sleep(base_sleep + jitter)

    print(f"DONE. Total rows inserted: {total_inserted}. Failures: {failures}")

if __name__ == "__main__":
    main()
