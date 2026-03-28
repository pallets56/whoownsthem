"""
Parliament Register of Members' Financial Interests - Full Historical Scraper
Pulls current data from Parliament API (2024 parliament)
AND historical data from mySociety CSV exports (2019-2024 parliament)

Requirements: Python 3 standard library only - no pip needed.

Run: python3 scrape_interests_nopip.py
Output: mpdata.js
"""

import urllib.request
import urllib.parse
import json
import csv
import io
import time
from collections import defaultdict

BASE = "https://interests-api.parliament.uk/api/v1"

# mySociety historical CSV exports - 2019 parliament
MYSOCIETY_CSVS = {
    "employment": "https://pages.mysociety.org/parl_register_interests/datasets/commons_rmfi/latest/category_1.csv",
    "employment_1": "https://pages.mysociety.org/parl_register_interests/datasets/commons_rmfi/latest/category_1.1.csv",
    "donations": "https://pages.mysociety.org/parl_register_interests/datasets/commons_rmfi/latest/category_2.csv",
    "gifts": "https://pages.mysociety.org/parl_register_interests/datasets/commons_rmfi/latest/category_3.csv",
    "visits": "https://pages.mysociety.org/parl_register_interests/datasets/commons_rmfi/latest/category_4.csv",
    "shareholdings": "https://pages.mysociety.org/parl_register_interests/datasets/commons_rmfi/latest/category_6.csv",
    "misc": "https://pages.mysociety.org/parl_register_interests/datasets/commons_rmfi/latest/category_8.csv",
}

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

import sys

def fetch_url(url):
    req = urllib.request.Request(url, headers={"Accept": "*/*", "User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()

def fetch_json(url):
    return json.loads(fetch_url(url).decode("utf-8"))

def fetch_csv(url):
    data = fetch_url(url).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(data))
    return list(reader)

def safe(val, fallback=""):
    return str(val).strip() if val else fallback

def safe_float(val):
    try:
        return float(str(val).replace(",","").replace("£","")) if val else 0.0
    except:
        return 0.0

def parse_date(val):
    return safe(val).split("T")[0] if val else ""

def get_or_create(mp_data, name):
    if name not in mp_data:
        mp_data[name] = {
            "name": name,
            "employment": [],
            "donations": [],
            "gifts": [],
            "visits": [],
            "shareholdings": [],
            "misc": [],
        }
    return mp_data[name]

# ---- PARLIAMENT API (current 2024 parliament) ----

def fetch_all_api(category_id, page_size=500):
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
            print(f"  {CATEGORIES.get(category_id)}: {total} records")
        records.extend(items)
        skip += len(items)
        if not items or skip >= total:
            break
        time.sleep(0.3)
        sys.stdout.write(f"  {skip}/{total}\r")
        sys.stdout.flush()
    return records

def get_member_api(record):
    m = record.get("member") or {}
    return safe(m.get("nameDisplayAs"))

def ingest_api(mp_data):
    # First pass: fetch all employment records and build parent ID -> payer name lookup
    print(f"\n[API] Fetching employment (building parent lookup)...")
    emp_records = fetch_all_api(1)
    
    # Build lookup: interest ID -> payer name
    parent_payer = {}
    for record in emp_records:
        interest_id = record.get("id")
        payer = safe(record.get("payerName"))
        if interest_id and payer:
            parent_payer[str(interest_id)] = payer

    # Process employment records with parent lookup
    for record in emp_records:
        try:
            member = get_member_api(record)
            if not member: continue
            get_or_create(mp_data, member)
            parent_id = str(record.get("parentInterestId") or "")
            payer = safe(record.get("payerName")) or parent_payer.get(parent_id, "")
            mp_data[member]["employment"].append({
                "summary": safe(record.get("summary")),
                "payer": payer,
                "job": safe(record.get("jobTitle")),
                "start": parse_date(record.get("startDate")),
                "end": parse_date(record.get("endDate")),
                "registered": parse_date(record.get("registered")),
            })
        except Exception:
            pass

    processors = {
        2: ("donations", lambda r: {
            "summary": safe(r.get("summary")),
            "donor": safe(r.get("donorName")),
            "value": safe_float(r.get("value")),
            "type": safe(r.get("paymentType")),
            "received": parse_date(r.get("receivedDate")),
            "donor_status": safe(r.get("donorStatus")),
        }),
        3: ("gifts", lambda r: {
            "summary": safe(r.get("summary")),
            "donor": safe(r.get("donorName")),
            "value": safe_float(r.get("value")),
            "description": safe(r.get("paymentDescription")),
            "received": parse_date(r.get("receivedDate")),
        }),
        4: ("visits", lambda r: {
            "summary": safe(r.get("summary")),
            "country": safe((r.get("visitLocations") or [{}])[0].get("country")) if r.get("visitLocations") else "",
            "destination": safe((r.get("visitLocations") or [{}])[0].get("destination")) if r.get("visitLocations") else "",
            "purpose": safe(r.get("purpose")),
            "donors": [safe(d.get("name")) for d in (r.get("donors") or []) if d.get("name")],
            "start": parse_date(r.get("startDate")),
        }),
        6: ("shareholdings", lambda r: {
            "summary": safe(r.get("summary")),
            "company": safe(r.get("companyName") or r.get("summary")),
            "registered": parse_date(r.get("registered")),
        }),
        8: ("misc", lambda r: {
            "summary": safe(r.get("summary")),
            "donor": safe(r.get("donorName")),
            "registered": parse_date(r.get("registered")),
        }),
    }

    for cat_id, (field, processor) in processors.items():
        print(f"\n[API] Fetching {CATEGORIES[cat_id]}...")
        records = fetch_all_api(cat_id)
        for record in records:
            try:
                member = get_member_api(record)
                if not member: continue
                get_or_create(mp_data, member)
                mp_data[member][field].append(processor(record))
            except Exception:
                pass

    # Remaining categories as misc
    for cat_id in [5, 7, 9, 10]:
        print(f"\n[API] Fetching {CATEGORIES[cat_id]}...")
        records = fetch_all_api(cat_id)
        for record in records:
            try:
                member = get_member_api(record)
                if not member: continue
                get_or_create(mp_data, member)
                mp_data[member]["misc"].append({
                    "summary": safe(record.get("summary")),
                    "donor": "",
                    "registered": parse_date(record.get("registered")),
                })
            except Exception:
                pass

# ---- MYSOCIETY CSV (historical 2019-2024 parliament) ----

def ingest_mysociety(mp_data):
    print("\n\n--- Fetching historical data from mySociety (2019-2024 parliament) ---")

    # Employment
    print("\n[History] Fetching employment...")
    try:
        rows = fetch_csv(MYSOCIETY_CSVS["employment"])
        rows += fetch_csv(MYSOCIETY_CSVS["employment_1"])
        for row in rows:
            member = safe(row.get("Member"))
            if not member: continue
            get_or_create(mp_data, member)
            mp_data[member]["employment"].append({
                "summary": safe(row.get("Summary")),
                "payer": safe(row.get("PayerName")),
                "job": safe(row.get("JobTitle")),
                "start": safe(row.get("StartDate")),
                "end": safe(row.get("EndDate")),
                "registered": safe(row.get("Registered")),
            })
        print(f"  {len(rows)} rows")
    except Exception as e:
        print(f"  Failed: {e}")

    # Donations
    print("\n[History] Fetching donations...")
    try:
        rows = fetch_csv(MYSOCIETY_CSVS["donations"])
        for row in rows:
            member = safe(row.get("Member"))
            if not member: continue
            get_or_create(mp_data, member)
            mp_data[member]["donations"].append({
                "summary": safe(row.get("Summary")),
                "donor": safe(row.get("DonorName")),
                "value": safe_float(row.get("Value")),
                "type": safe(row.get("PaymentType")),
                "received": safe(row.get("ReceivedDate")),
                "donor_status": safe(row.get("DonorStatus")),
            })
        print(f"  {len(rows)} rows")
    except Exception as e:
        print(f"  Failed: {e}")

    # Gifts
    print("\n[History] Fetching gifts...")
    try:
        rows = fetch_csv(MYSOCIETY_CSVS["gifts"])
        for row in rows:
            member = safe(row.get("Member"))
            if not member: continue
            get_or_create(mp_data, member)
            mp_data[member]["gifts"].append({
                "summary": safe(row.get("Summary")),
                "donor": safe(row.get("DonorName")),
                "value": safe_float(row.get("Value")),
                "description": safe(row.get("PaymentDescription")),
                "received": safe(row.get("ReceivedDate")),
            })
        print(f"  {len(rows)} rows")
    except Exception as e:
        print(f"  Failed: {e}")

    # Visits
    print("\n[History] Fetching visits...")
    try:
        rows = fetch_csv(MYSOCIETY_CSVS["visits"])
        for row in rows:
            member = safe(row.get("Member"))
            if not member: continue
            get_or_create(mp_data, member)
            mp_data[member]["visits"].append({
                "summary": safe(row.get("Summary")),
                "country": safe(row.get("VisitLocations_Country_1")),
                "destination": safe(row.get("VisitLocations_Destination_1")),
                "purpose": safe(row.get("Purpose")),
                "donors": [safe(row.get("Donors_Name_1"))] if row.get("Donors_Name_1") else [],
                "start": safe(row.get("StartDate")),
            })
        print(f"  {len(rows)} rows")
    except Exception as e:
        print(f"  Failed: {e}")

    # Shareholdings
    print("\n[History] Fetching shareholdings...")
    try:
        rows = fetch_csv(MYSOCIETY_CSVS["shareholdings"])
        for row in rows:
            member = safe(row.get("Member"))
            if not member: continue
            get_or_create(mp_data, member)
            mp_data[member]["shareholdings"].append({
                "summary": safe(row.get("Summary")),
                "company": safe(row.get("CompanyName") or row.get("Summary")),
                "registered": safe(row.get("Registered")),
            })
        print(f"  {len(rows)} rows")
    except Exception as e:
        print(f"  Failed: {e}")

    # Misc
    print("\n[History] Fetching misc...")
    try:
        rows = fetch_csv(MYSOCIETY_CSVS["misc"])
        for row in rows:
            member = safe(row.get("Member"))
            if not member: continue
            get_or_create(mp_data, member)
            mp_data[member]["misc"].append({
                "summary": safe(row.get("Summary")),
                "donor": safe(row.get("DonorName","")),
                "registered": safe(row.get("Registered")),
            })
        print(f"  {len(rows)} rows")
    except Exception as e:
        print(f"  Failed: {e}")

# ---- DEDUP ----

def dedup(items, key_fn):
    """Remove duplicate entries by key."""
    seen = set()
    out = []
    for item in items:
        k = key_fn(item)
        if k not in seen:
            seen.add(k)
            out.append(item)
    return out

def dedup_all(mp_data):
    for mp in mp_data.values():
        mp["employment"] = dedup(mp["employment"], lambda x: (x.get("payer",""), x.get("job",""), x.get("start","")))
        mp["donations"] = dedup(mp["donations"], lambda x: (x.get("donor",""), str(x.get("value","")), x.get("received","")))
        mp["gifts"] = dedup(mp["gifts"], lambda x: (x.get("donor",""), str(x.get("value","")), x.get("received","")))
        mp["visits"] = dedup(mp["visits"], lambda x: (x.get("country",""), x.get("start","")))
        mp["shareholdings"] = dedup(mp["shareholdings"], lambda x: x.get("company",""))
        mp["misc"] = dedup(mp["misc"], lambda x: x.get("summary",""))

# ---- MAIN ----

def main():
    mp_data = {}

    ingest_api(mp_data)
    ingest_mysociety(mp_data)
    dedup_all(mp_data)

    total_mps = len(mp_data)
    total_records = sum(
        len(mp[f]) for mp in mp_data.values()
        for f in ["employment", "donations", "gifts", "visits", "shareholdings", "misc"]
    )

    print(f"\n\nDone. {total_mps} MPs, {total_records} total records.")

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

if __name__ == "__main__":
    main()


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
