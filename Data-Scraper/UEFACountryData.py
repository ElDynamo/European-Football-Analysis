# CountryData.py
# Fetch JSONs from UEFA API per-year (cache in Data-UEFA/Countries), save per-year CSVs if requested,
# then aggregate and write final parquet files to Output/ (country_details.parquet, country_names.parquet, country_flags.parquet)
# country_details no longer contains country names (they live in country_names.parquet)

import time, random, json, requests, sys
from pathlib import Path

try:
    import pandas as pd
except Exception as e:
    raise SystemExit("Pandas (and pyarrow or fastparquet) are required. Install: pip install pandas pyarrow") from e

# ---------------- config ----------------
BASE_URL = "https://comp.uefa.com/v2/coefficients"
PAGE_SIZE = 55
YEARS = range(2005, 2027)   # 2005..2026
TIMEOUT = 25
SLEEP_BETWEEN_PAGES = 0.12
MAX_RETRIES = 3
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}

# ---------------- paths ----------------
def resolve_dirs_force():
    """
    Force Data-UEFA folder next to this script (create if missing) and Countries subfolder.
    This guarantees saving into Data-Scraper/Data-UEFA/Countries when script lives in Data-Scraper.
    """
    if "__file__" in globals():
        base = Path(__file__).resolve().parent
    else:
        base = Path.cwd()
    data_base = base / "Data-UEFA"
    countries_dir = data_base / "Countries"
    output_dir = base / "Output"
    # create explicitly
    countries_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return base, data_base, countries_dir, output_dir

BASE, DATA_BASE, DIR_RAW, OUTPUT_DIR = resolve_dirs_force()
OUT_DETAILS = OUTPUT_DIR / "UEFA_country_details.parquet"
OUT_NAMES = OUTPUT_DIR / "UEFA_country_names.parquet"
OUT_FLAGS = OUTPUT_DIR / "UEFA_country_flags.parquet"

print(f"Base script dir: {BASE}")
print(f"Data-UEFA base: {DATA_BASE}")
print(f"Countries folder (RAW): {DIR_RAW}")
print(f"Output folder: {OUTPUT_DIR}")

# ---------------- helpers ----------------
def get_members_page(session, year, page):
    params = {
        "coefficientRange": "OVERALL",
        "coefficientType": "MEN_ASSOCIATION",
        "language": "EN",
        "page": page,
        "pagesize": PAGE_SIZE,
        "seasonYear": year,
    }
    last_exc = None
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r = session.get(BASE_URL, params=params, timeout=TIMEOUT)
            if r.status_code >= 500 or r.status_code == 429:
                last_exc = RuntimeError(f"HTTP {r.status_code}")
                time.sleep(attempt * 0.3)
                continue
            r.raise_for_status()
            payload = r.json() or {}
            data = payload.get("data") or {}
            members = data.get("members") or []
            return members
        except requests.RequestException as e:
            last_exc = e
            time.sleep(attempt * 0.3 + random.random()*0.1)
    raise last_exc or RuntimeError("Failed to fetch members page")

def get_all_members_for_year(session, year):
    out = []
    page = 1
    while True:
        members = get_members_page(session, year, page)
        if not members:
            break
        out.extend(members)
        if len(members) < PAGE_SIZE:
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_PAGES + random.random()*0.05)
    return out

def season_points_from_entry(entry):
    srs = entry.get("seasonRankings") or []
    if srs:
        try:
            return float(srs[0].get("totalPoints") or 0.0)
        except Exception:
            try:
                return float(str(srs[0].get("totalPoints")).replace(",", "."))
            except Exception:
                return 0.0
    return 0.0

def safe_str(v):
    return "" if v is None else str(v)

# ---------------- main ----------------
def main(years=YEARS, write_per_year=True):
    session = requests.Session()
    session.headers.update(HEADERS)

    details_rows = []
    names_map = {}
    flags_map = {}

    for y in years:
        print(f"\n=== Processing year {y} ===")
        raw_json_path = DIR_RAW / f"members_{y}.json"
        members = None

        # 1) if cached JSON exists -> load it (avoid network)
        if raw_json_path.exists():
            try:
                with raw_json_path.open("r", encoding="utf-8") as f:
                    members = json.load(f)
                print(f"  Loaded cached JSON for {y} from {raw_json_path.name}")
            except Exception as e:
                print(f"  Warning: failed to load cached JSON {raw_json_path.name}: {e} -- will re-fetch")
                members = None

        # 2) if no cached JSON -> fetch from API and save JSON
        if members is None:
            try:
                members = get_all_members_for_year(session, y)
                # save JSON for caching
                try:
                    with raw_json_path.open("w", encoding="utf-8") as f:
                        json.dump(members, f, ensure_ascii=False, indent=2)
                    print(f"  Fetched and saved raw JSON: {raw_json_path.name}")
                except Exception as e:
                    print("  Warning: failed to save raw JSON:", e)
            except Exception as e:
                print(f"  FAILED fetching year {y}: {e}")
                continue

        if not members:
            print(f"  No members returned for {y}")
            continue

        per_year_rows = []

        for entry in members:
            member = entry.get("member") or {}
            overall = entry.get("overallRanking") or {}
            associationId = safe_str(member.get("associationId"))
            if associationId == "":
                continue

            season_points = season_points_from_entry(entry)
            overall_pos = overall.get("position")
            overall_total = overall.get("totalPoints")
            number_of_teams = overall.get("numberOfTeams")

            # NOTE: do not include CountryName here in final details to avoid duplication;
            # keep it in names_map only
            row = {
                "Season": int(y),
                "AssociationId": associationId,
                "Position": overall_pos if overall_pos is not None else "",
                "YearPoints": season_points,
                "OverallTotalPoints": overall_total if overall_total is not None else "",
                "Teams": number_of_teams if number_of_teams is not None else ""
            }
            details_rows.append(row)
            per_year_rows.append(row)

            # maintain names_map (latest seen wins)
            names_map[associationId] = {
                "AssociationId": associationId,
                "CountryCode": safe_str(member.get("countryCode")),
                "DisplayOfficialName": safe_str(member.get("displayOfficialName") or member.get("displayName"))
            }
            # flags_map (exclude bigLogoUrl)
            flags_map[associationId] = {
                "AssociationId": associationId,
                "logoUrl": safe_str(member.get("logoUrl")),
                "mediumLogoUrl": safe_str(member.get("mediumLogoUrl"))
            }

        # write per-year CSV to Data-UEFA/Countries if requested and missing
        if write_per_year:
            per_csv = DIR_RAW / f"country_details_{y}.csv"
            try:
                # if per-year CSV already exists, skip rewriting
                if per_csv.exists():
                    print(f"  Per-year CSV already exists: {per_csv.name} (skipping write)")
                else:
                    df_py = pd.DataFrame(per_year_rows)
                    cols = ["Season","AssociationId","Position","YearPoints","OverallTotalPoints","Teams"]
                    for c in cols:
                        if c not in df_py.columns:
                            df_py[c] = ""
                    df_py = df_py[cols]
                    df_py.to_csv(per_csv, index=False, encoding="utf-8-sig")
                    print(f"  Wrote per-year CSV: {per_csv.name}")
            except Exception as e:
                print("  Warning: failed to write per-year CSV:", e)

        time.sleep(0.12 + random.random()*0.08)

    # build final DataFrames
    if details_rows:
        df_details = pd.DataFrame(details_rows)
        # ensure order & types
        df_details = df_details[["Season","AssociationId","Position","YearPoints","OverallTotalPoints","Teams"]]
        df_details['Season'] = pd.to_numeric(df_details['Season'], errors='coerce').astype('Int64')
        df_details['YearPoints'] = pd.to_numeric(df_details['YearPoints'], errors='coerce')
        df_details['Position'] = pd.to_numeric(df_details['Position'], errors='coerce').astype('Int64')
        df_details['OverallTotalPoints'] = pd.to_numeric(df_details['OverallTotalPoints'], errors='coerce')
        df_details['Teams'] = pd.to_numeric(df_details['Teams'], errors='coerce').astype('Int64')
    else:
        df_details = pd.DataFrame(columns=["Season","AssociationId","Position","YearPoints","OverallTotalPoints","Teams"])

    df_names = pd.DataFrame(list(names_map.values())) if names_map else pd.DataFrame(columns=["AssociationId","CountryCode","DisplayOfficialName"])
    df_flags = pd.DataFrame(list(flags_map.values())) if flags_map else pd.DataFrame(columns=["AssociationId","logoUrl","mediumLogoUrl"])

    # write final parquets to Output/
    try:
        df_details.to_parquet(OUT_DETAILS, engine="pyarrow", index=False)
        df_names.to_parquet(OUT_NAMES, engine="pyarrow", index=False)
        df_flags.to_parquet(OUT_FLAGS, engine="pyarrow", index=False)
        engine_used = "pyarrow"
    except Exception:
        try:
            df_details.to_parquet(OUT_DETAILS, engine="fastparquet", index=False)
            df_names.to_parquet(OUT_NAMES, engine="fastparquet", index=False)
            df_flags.to_parquet(OUT_FLAGS, engine="fastparquet", index=False)
            engine_used = "fastparquet"
        except Exception as e:
            raise SystemExit("Failed to write Parquet. Install pyarrow or fastparquet.") from e

    print("\nDone. Files written:")
    print(f" - per-year CSVs and raw JSONs (if enabled) saved into: {DIR_RAW}")
    print(f" - final country_details: {OUT_DETAILS} ({len(df_details)} rows)")
    print(f" - final country_names:   {OUT_NAMES} ({len(df_names)} rows)")
    print(f" - final country_flags:   {OUT_FLAGS} ({len(df_flags)} rows)")
    print("Parquet engine used:", engine_used)

if __name__ == "__main__":
    main()
