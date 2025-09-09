# ClubData.py
# Fetch clubs from UEFA API per-year, save per-year raw JSON + per-year CSV into Data-UEFA/Clubs,
# then aggregate and write final parquet files into Output/
# Behavior: cache-first. If any members_{YEAR}.json exist, only refresh newest year (and fetch missing years).

import time, random, json, requests, sys
from pathlib import Path

try:
    import pandas as pd
except Exception as e:
    raise SystemExit("Pandas (and pyarrow or fastparquet) required. Install: pip install pandas pyarrow") from e

# ---------------- config ----------------
BASE_URL = "https://comp.uefa.com/v2/coefficients"
PAGE_SIZE = 200
YEARS = list(range(2005, 2027))   # 2005..2026
TIMEOUT = 25
SLEEP_BETWEEN_PAGES = 0.2
MAX_RETRIES = 3
HEADERS = {"User-Agent": "Mozilla/5.0"}

# tournament mapping (kept)
TOURNAMENT_ROWS = [
    {"tournament_id": 0, "tournament_name": "No competition"},
    {"tournament_id": 1, "tournament_name": "UEFA Conference League"},
    {"tournament_id": 2, "tournament_name": "UEFA Europa League"},
    {"tournament_id": 3, "tournament_name": "UEFA Champions League"},
]

# ---------------- CHANGED: fields/order ----------------
# per-year CSV fields
DETAILS_FIELDS = ["season","club_id","associationId","tournament_id","season_points","season_matches","overall_totalValue"]
# CLUB_FIELDS changed so associationId is next to club_id
CLUB_FIELDS = ["club_id","associationId","displayOfficialName","displayName","displayTeamCode"]
# LOGO_FIELDS changed to remove associationId
LOGO_FIELDS = ["id","logoUrl","mediumLogoUrl"]

# ---------------- paths (force Data-UEFA/Clubs next to script) ----------------
def resolve_dirs_force():
    if "__file__" in globals():
        base = Path(__file__).resolve().parent
    else:
        base = Path.cwd()
    data_base = base / "Data-UEFA"
    clubs_dir = data_base / "Clubs"
    output_dir = base / "Output"
    clubs_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return base, data_base, clubs_dir, output_dir

BASE, DATA_BASE, DIR_RAW, OUTPUT_DIR = resolve_dirs_force()
OUT_DETAILS = OUTPUT_DIR / "UEFA_club_details.parquet"
OUT_CLUB_DIM = OUTPUT_DIR / "UEFA_club_names.parquet"
OUT_LOGOS = OUTPUT_DIR / "UEFA_club_logos.parquet"
OUT_TOURNAMENTS = OUTPUT_DIR / "UEFA_tournament_names.parquet"

print("Script base:", BASE)
print("Raw save folder (Data-UEFA/Clubs):", DIR_RAW)
print("Output folder:", OUTPUT_DIR)

# ---------------- helpers ----------------
def to_float(v):
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).replace(",", "."))
        except Exception:
            return 0.0

def safe_str(v):
    return "" if v is None else str(v)

_OFFICIAL = {
    "uefa conference league": 1,
    "uefa europa league": 2,
    "uefa champions league": 3,
}

def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

def map_tournament_id(name: str) -> int:
    if not name:
        return 0
    return _OFFICIAL.get(_norm(name), 0)

def get_members_page(session, year, page):
    params = {
        "coefficientRange": "OVERALL",
        "coefficientType": "MEN_CLUB",
        "language": "EN",
        "page": page,
        "pagesize": PAGE_SIZE,
        "seasonYear": year,
    }
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(BASE_URL, params=params, timeout=TIMEOUT)
            if r.status_code >= 500 or r.status_code == 429:
                last = RuntimeError(f"HTTP {r.status_code}")
                time.sleep(0.4 * attempt)
                continue
            r.raise_for_status()
            payload = r.json() or {}
            return payload.get("data", {}).get("members", []) or []
        except requests.RequestException as e:
            last = e
        time.sleep(0.4 * attempt)
    if last:
        raise last
    return []

def fetch_year_clubs(session, year):
    out, page = [], 1
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

def find_season_row(entry, year):
    for s in entry.get("seasonRankings") or []:
        if s.get("seasonYear") == year:
            return s
    return None

def write_csv(path: Path, rows, fields):
    df = pd.DataFrame(rows)
    # ensure columns order
    for f in fields:
        if f not in df.columns:
            df[f] = ""
    df = df[fields]
    df.to_csv(path, index=False, encoding="utf-8-sig")

# ---------------- main flow ----------------
def main(years=YEARS, write_per_year=True):
    session = requests.Session()
    session.headers.update(HEADERS)

    logos_map = {}
    clubs_map = {}
    details_rows = []

    # detect cached JSON years
    cached_years = set()
    for y in years:
        if (DIR_RAW / f"members_{y}.json").exists():
            cached_years.add(y)

    newest = max(years)
    fetch_all = (len(cached_years) == 0)
    if fetch_all:
        print("No cached JSONs found -> will fetch all years.")
    else:
        print(f"Found cached JSONs for years: {sorted(cached_years)}")
        print(f"Will refresh only newest year: {newest} and load others from cache (missing years will be fetched)")

    for y in years:
        print(f"\n--- Processing year {y} ---")
        raw_json_path = DIR_RAW / f"members_{y}.json"
        members = None
        loaded_from_cache = False

        # if not fetching all, prefer cached file if present
        if not fetch_all and raw_json_path.exists():
            try:
                with raw_json_path.open("r", encoding="utf-8") as f:
                    members = json.load(f)
                loaded_from_cache = True
                print(f"  Loaded cached JSON for {y} from {raw_json_path.name}")
            except Exception as e:
                print(f"  Warning: failed to load cached JSON {raw_json_path.name}: {e}. Will fetch.")

        # decide if we need to fetch:
        need_fetch = False
        if fetch_all:
            need_fetch = True
        else:
            # fetch newest always (refresh)
            if y == newest:
                need_fetch = True
            else:
                # if cached failed to load or file missing -> fetch
                if not loaded_from_cache:
                    need_fetch = True

        if need_fetch:
            try:
                members = fetch_year_clubs(session, y)
                # save raw JSON if fetched
                try:
                    with raw_json_path.open("w", encoding="utf-8") as f:
                        json.dump(members, f, ensure_ascii=False, indent=2)
                    print(f"  Fetched and saved raw JSON: {raw_json_path.name}")
                except Exception as e:
                    print("  Warning: cannot save raw JSON:", e)
            except Exception as e:
                print("  FAILED fetching year", y, ":", e)
                continue

        if not members:
            print("  No members for", y)
            continue

        per_year_details = []

        for e in members:
            m = e.get("member", {}) or {}
            c = e.get("competition", {}) or {}
            ov = e.get("overallRanking", {}) or {}

            cid = safe_str(m.get("id"))
            if not cid:
                continue
            association_id = safe_str(m.get("associationId"))

            # logos (latest) -- CHANGED: no associationId stored
            logos_map[cid] = {
                "id": cid,
                "logoUrl": safe_str(m.get("logoUrl")),
                "mediumLogoUrl": safe_str(m.get("mediumLogoUrl"))
            }
            # club dim (latest) -- CHANGED: associationId immediately after club_id
            clubs_map[cid] = {
                "club_id": cid,
                "associationId": association_id,
                "displayOfficialName": safe_str(m.get("displayOfficialName") or m.get("displayName")),
                "displayName": safe_str(m.get("displayName")),
                "displayTeamCode": safe_str(m.get("displayTeamCode"))
            }

            srow = find_season_row(e, y)
            tid = map_tournament_id(c.get("displayName"))

            season_points = to_float(srow.get("totalValue")) if (srow and tid != 0) else 0.0
            season_matches = to_float(srow.get("numberOfMatches")) if (srow and tid != 0) else 0.0
            overall_val = to_float(ov.get("totalValue")) if ov else 0.0

            row = {
                "season": y,
                "club_id": cid,
                "associationId": association_id,
                "tournament_id": tid,
                "season_points": season_points,
                "season_matches": season_matches,
                "overall_totalValue": overall_val
            }
            details_rows.append(row)
            per_year_details.append(row)

        # write per-year CSVs into Data-UEFA/Clubs if requested and if not present
        if write_per_year:
            try:
                per_csv = DIR_RAW / f"club_details_{y}.csv"
                club_dim_csv = DIR_RAW / f"club_dim_{y}.csv"
                logo_csv = DIR_RAW / f"club_logos_{y}.csv"
                if per_csv.exists():
                    print(f"  Per-year CSV already exists: {per_csv.name} (skipping write)")
                else:
                    write_csv(per_csv, per_year_details, DETAILS_FIELDS)
                    print(f"  Wrote per-year CSV: {per_csv.name}")
                if not club_dim_csv.exists():
                    write_csv(club_dim_csv, list(clubs_map.values()), CLUB_FIELDS)
                else:
                    print(f"  Per-year club dim already exists: {club_dim_csv.name} (skipping write)")
                if not logo_csv.exists():
                    write_csv(logo_csv, list(logos_map.values()), LOGO_FIELDS)
                else:
                    print(f"  Per-year logos already exists: {logo_csv.name} (skipping write)")
            except Exception as e:
                print("  Warning: failed to write per-year CSVs:", e)

        time.sleep(0.12 + random.random()*0.08)

    # aggregate and write parquet outputs
    # details
    if details_rows:
        df_details = pd.DataFrame(details_rows)
        df_details['season'] = pd.to_numeric(df_details['season'], errors='coerce').astype('Int64')
        for col in ("season_points","season_matches","overall_totalValue"):
            if col in df_details.columns:
                df_details[col] = pd.to_numeric(df_details[col], errors='coerce')
    else:
        df_details = pd.DataFrame(columns=DETAILS_FIELDS)

    # club dim (take last known per club_id)
    club_files = sorted(DIR_RAW.glob("club_dim_*.csv"))
    if club_files:
        frames = []
        for pf in club_files:
            try:
                frames.append(pd.read_csv(pf, dtype=str, encoding="utf-8"))
            except Exception:
                pass
        if frames:
            clubs_df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["club_id"], keep="last").fillna("")
        else:
            clubs_df = pd.DataFrame(list(clubs_map.values()))
    else:
        clubs_df = pd.DataFrame(list(clubs_map.values()))

    # enforce CLUB_FIELDS order for final dim
    for c in CLUB_FIELDS:
        if c not in clubs_df.columns:
            clubs_df[c] = ""
    clubs_df = clubs_df[CLUB_FIELDS]

    # logos
    logo_files = sorted(DIR_RAW.glob("club_logos_*.csv"))
    if logo_files:
        frames = []
        for pf in logo_files:
            try:
                frames.append(pd.read_csv(pf, dtype=str, encoding="utf-8"))
            except Exception:
                pass
        if frames:
            logos_df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["id"], keep="last").fillna("")
        else:
            logos_df = pd.DataFrame(list(logos_map.values()))
    else:
        logos_df = pd.DataFrame(list(logos_map.values()))

    # enforce LOGO_FIELDS order for final logos
    for c in LOGO_FIELDS:
        if c not in logos_df.columns:
            logos_df[c] = ""
    logos_df = logos_df[LOGO_FIELDS]

    tour_df = pd.DataFrame(TOURNAMENT_ROWS)

    # write parquet outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        df_details.to_parquet(OUT_DETAILS, engine="pyarrow", index=False)
        clubs_df.to_parquet(OUT_CLUB_DIM, engine="pyarrow", index=False)
        logos_df.to_parquet(OUT_LOGOS, engine="pyarrow", index=False)
        tour_df.to_parquet(OUT_TOURNAMENTS, engine="pyarrow", index=False)
        engine_used = "pyarrow"
    except Exception:
        try:
            df_details.to_parquet(OUT_DETAILS, engine="fastparquet", index=False)
            clubs_df.to_parquet(OUT_CLUB_DIM, engine="fastparquet", index=False)
            logos_df.to_parquet(OUT_LOGOS, engine="fastparquet", index=False)
            tour_df.to_parquet(OUT_TOURNAMENTS, engine="fastparquet", index=False)
            engine_used = "fastparquet"
        except Exception as e:
            raise SystemExit("Failed to write Parquet. Install pyarrow or fastparquet.") from e

    print("\nDone. Written:")
    print(" - club_details:", OUT_DETAILS, "rows:", len(df_details))
    print(" - dim_clubs:", OUT_CLUB_DIM, "rows:", len(clubs_df))
    print(" - club_logos:", OUT_LOGOS, "rows:", len(logos_df))
    print(" - tournament_names:", OUT_TOURNAMENTS, "rows:", len(tour_df))
    print("Parquet engine:", engine_used)
    print("Raw/year CSVs & JSON saved to:", DIR_RAW)

if __name__ == "__main__":
    main()
