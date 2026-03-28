"""
Parliament Register of Members' Financial Interests - Full Historical Scraper
Uses only Python standard library - no pip install needed.

Run: python3 scrape_interests_nopip.py
Output: mpdata.js
"""

import urllib.request
import urllib.parse
import json
import time
from collections import defaultdict

BASE = "https://interests-api.parliament.uk/api/v1"

CATEGORIES = {
    1: "employment",
    2: "donations",
    3: "gifts",
    4: "visits",
    5: "land",
    6: "shareholdings",
    7: "loans",
    8: "misc",
    9: "family_employment",
    10: "other",
}

def fetch_json(url):
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))

def fetch_all(category_id, page_size=500):
    records = []
    skip = 0
    total = None

    while True:
        params = urllib.parse.urlencode({"categories": category_id, "take": page_size, "skip": skip})
        url = f"{BASE}/Interests?{params}"
        try:
            data = fetch_json(url)
        except Exception as e:
            print(f"\n  Error at skip={skip}: {e} - retrying in 5s")
            time.sleep(5)
            try:
                data = fetch_json(url)
            except Exception as e2:
                print(f"  Failed again: {e2} - skipping")
                break

        items = data.get("items", [])
        if total is None:
            total = data.get("totalResults", 0)
            print(f"  {CATEGORIES.get(category_id)}: {total} total records")

        records.extend(items)
        skip += len(items)

        if not items or skip >= total:
            break

        time.sleep(0.3)
        sys.stdout.write(f"  {skip}/{total}\r")
        sys.stdout.flush()

    print(f"  Fetched: {len(records)}")
    return records

import sys

def safe(val, fallback=""):
    return str(val).strip() if val else fallback

def safe_float(val):
    try:
        return float(val) if val else 0.0
    except:
        return 0.0

def parse_date(val):
    return safe(val).split("T")[0] if val else ""

def get_member(record):
    m = record.get("member") or {}
    return safe(m.get("nameDisplayAs"))

def process_employment(r):
    return {
        "summary": safe(r.get("summary")),
        "payer": safe(r.get("payerName")),
        "job": safe(r.get("jobTitle")),
        "start": parse_date(r.get("startDate")),
        "end": parse_date(r.get("endDate")),
        "registered": parse_date(r.get("registered")),
    }

def process_donation(r):
    return {
        "summary": safe(r.get("summary")),
        "donor": safe(r.get("donorName")),
        "value": safe_float(r.get("value")),
        "type": safe(r.get("paymentType")),
        "received": parse_date(r.get("receivedDate")),
        "donor_status": safe(r.get("donorStatus")),
    }

def process_gift(r):
    return {
        "summary": safe(r.get("summary")),
        "donor": safe(r.get("donorName")),
        "value": safe_float(r.get("value")),
        "description": safe(r.get("paymentDescription")),
        "received": parse_date(r.get("receivedDate")),
    }

def process_visit(r):
    locations = r.get("visitLocations") or []
    donors = r.get("donors") or []
    country = safe(locations[0].get("country")) if locations else ""
    destination = safe(locations[0].get("destination")) if locations else ""
    donor_names = [safe(d.get("name")) for d in donors if d.get("name")]
    return {
        "summary": safe(r.get("summary")),
        "country": country,
        "destination": destination,
        "purpose": safe(r.get("purpose")),
        "donors": donor_names,
        "start": parse_date(r.get("startDate")),
    }

def process_shareholding(r):
    return {
        "summary": safe(r.get("summary")),
        "company": safe(r.get("companyName") or r.get("summary")),
        "registered": parse_date(r.get("registered")),
    }

def process_misc(r):
    return {
        "summary": safe(r.get("summary")),
        "donor": safe(r.get("donorName")),
        "registered": parse_date(r.get("registered")),
    }

PROCESSORS = {
    1: ("employment", process_employment),
    2: ("donations", process_donation),
    3: ("gifts", process_gift),
    4: ("visits", process_visit),
    6: ("shareholdings", process_shareholding),
    8: ("misc", process_misc),
}

def main():
    mp_data = defaultdict(lambda: {
        "name": "",
        "employment": [],
        "donations": [],
        "gifts": [],
        "visits": [],
        "shareholdings": [],
        "misc": [],
    })

    for cat_id, (field, processor) in PROCESSORS.items():
        print(f"\nFetching {CATEGORIES[cat_id]}...")
        records = fetch_all(cat_id)
        skipped = 0
        for record in records:
            try:
                member = get_member(record)
                if not member:
                    skipped += 1
                    continue
                mp_data[member]["name"] = member
                mp_data[member][field].append(processor(record))
            except Exception as e:
                skipped += 1
        if skipped:
            print(f"  Skipped {skipped} malformed records")

    # Remaining categories - store as misc summary only
    for cat_id in [5, 7, 9, 10]:
        print(f"\nFetching {CATEGORIES[cat_id]}...")
        records = fetch_all(cat_id)
        for record in records:
            try:
                member = get_member(record)
                if not member:
                    continue
                mp_data[member]["name"] = member
                mp_data[member]["misc"].append({
                    "summary": safe(record.get("summary")),
                    "donor": "",
                    "registered": parse_date(record.get("registered")),
                })
            except Exception:
                pass

    total_mps = len(mp_data)
    total_records = sum(
        len(mp[f]) for mp in mp_data.values()
        for f in ["employment", "donations", "gifts", "visits", "shareholdings", "misc"]
    )

    print(f"\nDone. {total_mps} MPs, {total_records} total records.")

    donor_totals = defaultdict(float)
    for mp in mp_data.values():
        for d in mp["donations"]:
            if d["donor"]:
                donor_totals[d["donor"]] += d["value"]
    top = sorted(donor_totals.items(), key=lambda x: -x[1])[:10]
    print("\nTop donors by total value:")
    for donor, val in top:
        print(f"  £{val:>12,.0f}  {donor}")

    js = "const MP_DATA = " + json.dumps(dict(mp_data), separators=(",", ":")) + ";"
    with open("mpdata.js", "w", encoding="utf-8") as f:
        f.write(js)

    size_kb = len(js.encode("utf-8")) // 1024
    print(f"\nWritten mpdata.js ({size_kb}KB)")
    print("Replace the mpdata.js next to whoownsthem.html with this file.")

if __name__ == "__main__":
    main()
