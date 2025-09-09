# CountryDataKes.py
# Fetch -> parse per-year CSVs named country_details_kes_{YEAR}.csv -> merge into Output/country_details_kes.parquet

import re, csv, time, random, requests, sys
from bs4 import BeautifulSoup
from pathlib import Path

# Try import pandas (required for parquet output)
try:
    import pandas as pd
except Exception as e:
    raise SystemExit("Pandas is required for this script. Install with: pip install pandas") from e

# ------------------ paths (relative to this file) ------------------
def resolve_dirs():
    if "__file__" in globals():
        base = Path(__file__).resolve().parent
    else:
        base = Path.cwd()
    candidates = [
        base / "Data-Kassiesa", base / "Data Kassiesa",
        base.parent / "Data-Kassiesa", base.parent / "Data Kassiesa"
    ]
    data_base = next((p for p in candidates if p.exists()), candidates[0])
    countries_dir = data_base / "Countries"
    output_dir = base / "Output"
    countries_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return base, data_base, countries_dir, output_dir

BASE, DATA_BASE, DIR_RAW, DIR_OUT = resolve_dirs()
OUT = DIR_OUT / "KES_country_details.parquet"   # final merged output (parquet)

# ------------------ config ------------------
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.1 Safari/537.36"),
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://kassiesa.net/uefa/data/",
}

# per-year CSV fields (internal)
CSV_FIELDS = ["season_year","country","pos","value","ranking","teams"]

# ------------------ helpers ------------------
def decode_html_bytes(raw: bytes) -> str:
    m = re.search(rb'charset=["\']?([A-Za-z0-9_-]+)', raw[:4000], re.I)
    enc = m.group(1).decode("ascii","ignore").lower() if m else None
    for attempt in (enc, "utf-8", "latin-1"):
        if not attempt: continue
        try:
            return raw.decode(attempt)
        except Exception:
            pass
    return raw.decode("utf-8", errors="ignore")

def norm_text(s: str) -> str:
    if not s: return ""
    s = s.replace("\xa0", " ").strip()
    if "Ã" in s or "Â" in s:
        try: s = s.encode("latin-1", "ignore").decode("utf-8", "ignore")
        except Exception: pass
    return s

def header_labels_with_colspan(table):
    hdr = table.select_one("tr.countryheader")
    labels = []
    if not hdr: return labels
    import re as _re
    for th in hdr.find_all(_re.compile("t(h|d)")):
        cs = int(th.get("colspan", 1))
        txt = th.get_text(strip=True)
        if cs > 1: labels.extend([""] * (cs - 1))
        labels.append(txt)
    return labels

def iter_country_rows(table):
    rows = table.select("tr.countryline")
    if rows: return rows
    out = []
    for tr in table.find_all("tr"):
        cls = tr.get("class") or []
        if any(c in ("countryheader","clubline") for c in cls): continue
        tds = tr.find_all("td")
        if len(tds) >= 3: out.append(tr)
    return out

def season_label_to_year(season_label: str) -> int:
    season_label = season_label.strip()
    m = re.match(r'(\d{4})\s*/\s*(\d{4})', season_label)
    if m: return int(m.group(2))
    m2 = re.match(r'(\d{1,2})\s*/\s*(\d{1,2})', season_label)
    if m2:
        s2 = int(m2.group(2))
        return 2000 + s2 if 0 <= s2 <= 29 else 1900 + s2
    m3 = re.search(r'(\d{4})', season_label)
    if m3: return int(m3.group(1))
    raise ValueError(f"Unknown season label: {season_label}")

# helpers to extract numeric pieces
def extract_year_points(cell_text: str):
    """
    From a cell like '2018-2020 7.500' or '7.500' returns float 7.5.
    Strategy: find all numbers with optional decimal part, return the last match (most likely the points).
    Returns None if not parseable.
    """
    if not cell_text: return None
    nums = re.findall(r'(\d+(?:[.,]\d+)?)', cell_text)
    if not nums: return None
    last = nums[-1].replace(",", ".")
    try:
        return float(last)
    except Exception:
        return None

def extract_teams(cell_text: str):
    """Extract first integer from text like '16 teams' -> 16; returns None if not found."""
    if not cell_text: return None
    m = re.search(r'(\d+)', cell_text)
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    return None

# ------------------ fetch & parse per-year page ------------------
def url_for_year(y: int) -> str:
    if 2018 <= y <= 2026: method = "method5"
    elif 2009 <= y <= 2017: method = "method4"
    elif 2004 <= y <= 2008: method = "method3"
    elif 2000 <= y <= 2003: method = "method2"
    else: raise ValueError(f"Unsupported year: {y}")
    return f"https://kassiesa.net/uefa/data/{method}/crank{y}.html"

def fetch_html(sess: requests.Session, url: str, retries=3, timeout=25) -> str:
    last = None
    for attempt in range(1, retries+1):
        try:
            r = sess.get(url, timeout=timeout)
            if r.ok: return decode_html_bytes(r.content)
            last = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last = e
        time.sleep(attempt + random.uniform(0, 0.5))
    raise last or RuntimeError(f"Failed to fetch {url}")

def parse_html_to_rows(html: str, page_year: int):
    """
    For each country row, take ONLY the right-most seasonal column (the last before 'ranking')
    and extract the numeric points from that cell as YearPoints.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for table in soup.select("table.t1"):
        labels = header_labels_with_colspan(table)
        if not labels: continue
        # identify indexes
        idx_country = next((i for i,lab in enumerate(labels) if lab and lab.strip().lower()=="country"), None)
        if idx_country is None:
            idx_country = next((i for i,lab in enumerate(labels) if lab and "country" in lab.lower()), None)
        idx_ranking = next((i for i,lab in enumerate(labels) if lab and lab.strip().lower() in ("ranking","rank")), None)
        if idx_country is None or idx_ranking is None: continue
        # seasonal indices are the columns between country and ranking; we want the last one
        season_indices = list(range(idx_country+1, idx_ranking))
        if not season_indices:
            continue
        last_si = season_indices[-1]
        last_label = labels[last_si] if last_si < len(labels) else ""
        for tr in iter_country_rows(table):
            tds = tr.find_all("td")
            if not tds: continue
            pos = norm_text(tds[0].get_text()) if len(tds) > 0 else ""
            country = norm_text(tds[idx_country].get_text()) if idx_country < len(tds) else ""
            ranking = norm_text(tds[idx_ranking].get_text()) if idx_ranking < len(tds) else ""
            teams_val = norm_text(tds[-1].get_text()) if tds else ""
            teams = extract_teams(teams_val)
            # get the cell corresponding to the last seasonal column (if present)
            cell_text = norm_text(tds[last_si].get_text()) if last_si < len(tds) else ""
            try:
                season_year = season_label_to_year(last_label) if last_label else page_year
            except Exception:
                season_year = page_year
            year_points = extract_year_points(cell_text)
            rows.append({
                "season_year": season_year,
                "country": country,
                "pos": pos,
                "value": "" if year_points is None else year_points,
                "ranking": ranking,
                "teams": "" if teams is None else teams
            })
    return rows

def save_per_year_csv(rows, year, dir_raw):
    per_file = dir_raw / f"country_details_kes_{year}.csv"
    with per_file.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        # write rows but ensure numeric floats are written as strings (CSV)
        safe_rows = []
        for r in rows:
            rr = r.copy()
            if isinstance(rr.get("value"), float):
                rr["value"] = f"{rr['value']:.6f}".rstrip("0").rstrip(".")
            safe_rows.append(rr)
        w.writerows(safe_rows)
    return per_file

# ------------------ main flow ------------------
def main(years=range(2000, 2026+1), overwrite=False):
    years = list(years)
    print("BASE:", BASE)
    print("DATA_BASE (raw dir):", DATA_BASE)
    print("DIR_RAW (Countries):", DIR_RAW)
    print("DIR_OUT (Output):", DIR_OUT)

    expected_files = {y: (DIR_RAW / f"country_details_kes_{y}.csv") for y in years}
    existing_years = [y for y,p in expected_files.items() if p.exists()]

    # decide which years to fetch
    if overwrite:
        to_fetch = years[:]  # all
    else:
        if not existing_years:
            to_fetch = years[:]  # none exist -> fetch all
        else:
            newest = max(years)
            to_fetch = [newest]

    print("Existing per-year files count:", len(existing_years))
    print("Years to fetch this run:", to_fetch)

    per_files = []
    with requests.Session() as sess:
        sess.headers.update(HEADERS)
        for y in to_fetch:
            print(f"Fetching year {y} ...")
            try:
                html = fetch_html(sess, url_for_year(y))
            except Exception as e:
                print("  FAILED fetch {y}: {e}")
                continue
            rows = parse_html_to_rows(html, y)
            if not rows:
                print("  Warning: no rows parsed for", y)
            saved = save_per_year_csv(rows, y, DIR_RAW)
            per_files.append(saved)
            time.sleep(0.2 + random.uniform(0, 0.3))

    # merge all per-year CSVs present
    all_per_files = sorted([p for p in DIR_RAW.glob("country_details_kes_*.csv")])
    merged_rows = []
    for pf in all_per_files:
        try:
            with pf.open("r", encoding="utf-8-sig", newline="") as f:
                r = csv.DictReader(f)
                for row in r:
                    merged_rows.append(row)
        except Exception as e:
            print("Error reading", pf, e)

    # convert to DataFrame and rename columns
    if merged_rows:
        df = pd.DataFrame(merged_rows)
        # ensure expected columns present
        for c in CSV_FIELDS:
            if c not in df.columns:
                df[c] = ""
        # convert types
        df['season_year'] = pd.to_numeric(df['season_year'], errors='coerce').astype('Int64')
        # value -> YearPoints (float)
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        # ranking -> RankingPoints (try numeric, keep NaN if not)
        df['ranking'] = pd.to_numeric(df['ranking'], errors='coerce')
        # teams -> Teams int
        df['teams'] = pd.to_numeric(df['teams'], errors='coerce').astype('Int64')

        # reorder and rename to requested final columns:
        df_final = df.rename(columns={
            "season_year": "Season",
            "country": "CountryName",
            "pos": "Position",
            "value": "YearPoints",
            "ranking": "RankingPoints",
            "teams": "Teams"
        })[["Season","CountryName","Position","YearPoints","RankingPoints","Teams"]]
    else:
        df_final = pd.DataFrame(columns=["Season","CountryName","Position","YearPoints","RankingPoints","Teams"])

    # attempt to write parquet using available engine
    try:
        df_final.to_parquet(OUT, engine="pyarrow", index=False)
        written_engine = "pyarrow"
    except Exception as e_py:
        try:
            df_final.to_parquet(OUT, engine="fastparquet", index=False)
            written_engine = "fastparquet"
        except Exception as e_fp:
            raise SystemExit(
                "Failed to write Parquet. Install pyarrow (recommended) or fastparquet:\n"
                "  pip install pyarrow\n"
                "or\n"
                "  pip install fastparquet"
            ) from e_fp

    print(f"Done. Merged {len(df_final)} rows into {OUT} (engine: {written_engine})")

if __name__ == "__main__":
    # default: overwrite=False -> if any per-year CSV exists, only fetch newest year (2026)
    main(years=range(2000, 2026+1), overwrite=False)
