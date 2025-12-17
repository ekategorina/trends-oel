import os
import time
import random
import requests

from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_ANON_KEY"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    # IMPORTANT: prevents duplicate insert crashes when you add UNIQUE(keyword, date)
    "Prefer": "resolution=merge-duplicates",
}

COUNTRY_TO_GEO = {"DK": "DK"}


def supabase_get_keywords(limit=5):
    # Hent flere end vi bruger
    url = f"{SUPABASE_URL}/rest/v1/keywords?select=keyword,topic,country&limit=30"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    keywords = r.json()

    # Bland rækkefølgen så vi ikke altid rammer de samme
    random.shuffle(keywords)

    # Returnér kun det antal vi vil bruge i dette run
    return keywords[:limit]


def supabase_upsert_trends(rows):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/trends?on_conflict=keyword,date"
    r = requests.post(url, headers=HEADERS, json=rows, timeout=30)
    if r.status_code not in (200, 201, 204):
        raise RuntimeError(f"Upsert failed: {r.status_code} {r.text}")


def fetch_interest_over_time_with_retry(pytrends, keyword, geo, max_attempts=5):
    """
    Returns a list of rows like:
    {"keyword": "...", "date": "YYYY-MM-DD", "interest": 0-100}
    Retries on Google 429 with exponential backoff.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            pytrends.build_payload([keyword], timeframe="today 12-m", geo=geo)
            df = pytrends.interest_over_time()

            if df is None or df.empty:
                return []

            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])

            rows = []
            for date, row in df.iterrows():
                rows.append(
                    {
                        "keyword": keyword,
                        "date": str(date.date()),
                        "interest": int(row[keyword]),
                    }
                )

            return rows

        except TooManyRequestsError:
            sleep_s = (2 ** attempt) * 10 + random.randint(0, 7)  # 20s, 40s, 80s...
            print(
                f"429 TooManyRequests for '{keyword}'. "
                f"Attempt {attempt}/{max_attempts}. Sleeping {sleep_s}s…"
            )
            time.sleep(sleep_s)

    # If we get here, retries didn't help
    raise TooManyRequestsError("Too many requests even after retries")


def main():
    keywords = supabase_get_keywords(limit=5)
    print(f"Fetched {len(keywords)} keywords")

    pytrends = TrendReq(hl="da-DK", tz=60)

    total_upserted = 0
    failures = 0

    for i, k in enumerate(keywords, start=1):
        keyword = k["keyword"]
        country = (k.get("country") or "DK").upper()
        geo = COUNTRY_TO_GEO.get(country, "DK")

        print(f"[{i}/{len(keywords)}] Fetching trends for: {keyword} ({geo})")

        try:
            rows = fetch_interest_over_time_with_retry(pytrends, keyword, geo, max_attempts=5)
            supabase_upsert_trends(rows)
            total_upserted += len(rows)
            print(f"Upserted {len(rows)} rows for {keyword}")

        except Exception as e:
            failures += 1
            print(f"FAILED keyword '{keyword}': {type(e).__name__}: {e}")

        # Always slow down between keywords to reduce 429s
        time.sleep(35 + random.randint(0, 15))


    print(f"DONE. Total rows upserted: {total_upserted}. Failures: {failures}")


if __name__ == "__main__":
    main()
