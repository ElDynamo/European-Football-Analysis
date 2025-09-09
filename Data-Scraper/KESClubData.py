# ClubDataKes.py
# Fetch -> parse per-year CSVs named club_details_kes_{YEAR}.csv -> merge into Output/club_details_kes.parquet

import re, csv, time, random, requests, sys
from bs4 import BeautifulSoup
from pathlib import Path

# pandas required for parquet output
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
    clubs_dir = data_base / "Clubs"
    output_dir = base / "Output"
    clubs_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return base, data_base, clubs_dir, output_dir

BASE, DATA_BASE, DIR_RAW, DIR_OUT = resolve_dirs()
OUT = DIR_OUT / "KES_club_details.parquet"   # final merged output (parquet)

# ------------------ config ------------------
HEADERS_HTTP = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.1 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://kassiesa.net/uefa/data/",
}

# internal per-year CSV fields (used while parsing and saving per-year files)
CSV_FIELDS = [
    "year","country","teams_in_country",
    "Pos","Club","Cup","qW","qD","qL","fW","fD","fL","Bonus","Points"
]

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

# ------------------ parsing utilities ------------------
def header_index_map_with_colspan(table):
    m = {}
    hdr = table.select_one("tr.countryheader")
    if not hdr: return m
    labels = []
    for td in hdr.find_all("td"):
        cs = int(td.get("colspan", 1))
        txt = td.get_text(strip=True)
        if cs > 1:
            labels.extend([""] * (cs - 1))
        labels.append(txt)
    for i, lab in enumerate(labels):
        if lab: m[lab] = i
    return m

def iter_club_rows(table):
    rows = table.select("tr.clubline")
    if rows: return rows
    out = []
    for tr in table.find_all("tr"):
        cls = tr.get("class") or []
        if any(c in ("countryheader","countryline") for c in cls): continue
        if len(tr.find_all("td")) >= 3:
            out.append(tr)
    return out

def get_country_and_teams(table):
    country, teams = "", None
    cl = table.select_one("tr.countryline")
    if cl:
        tds = cl.find_all("td")
        if len(tds) >= 2:
            b = tds[1].find("b")
            if b: country = norm_text(b.get_text())
            txt_join = " ".join(norm_text(x) for x in tds[1].stripped_strings)
            m = re.search(r"(\d+)\s*teams?", txt_join, re.I)
            if m: teams = int(m.group(1))
    if teams is None:
        teams = len(table.select("tr.clubline")) or sum(1 for _ in iter_club_rows(table))
    return country, teams

def extract_row_from_tds(tds, hdr_idx):
    # return keys that match CSV_FIELDS internal names
    txt = [norm_text(td.get_text()) for td in tds]
    val = lambda i: (txt[i] if -len(txt) <= i < len(txt) else "")
    defaults = {"Cup":2, "qW":3, "qD":4, "qL":5, "#W":6, "#D":7, "#L":8, "Bonus":9, "Points":10}
    get_i = lambda key: hdr_idx.get(key, defaults[key])
    return {
        "Pos": val(0),
        "Club": val(1),
        "Cup": val(get_i("Cup")),
        "qW": val(get_i("qW")),
        "qD": val(get_i("qD")),
        "qL": val(get_i("qL")),
        "fW": val(get_i("#W")),
        "fD": val(get_i("#D")),
        "fL": val(get_i("#L")),
        "Bonus": val(get_i("Bonus")),
        "Points": val(get_i("Points")),
    }

# ------------------ fetching & parsing ------------------
def url_for_year(y: int) -> str:
    if 2018 <= y <= 2026: method = "method5"
    elif 2009 <= y <= 2017: method = "method4"
    elif 2004 <= y <= 2008: method = "method3"
    elif 2000 <= y <= 2003: method = "method2"
    else: raise ValueError(f"Unsupported year: {y}")
    return f"https://kassiesa.net/uefa/data/{method}/ccoef{y}.html"

def fetch_html(sess: requests.Session, url: str, retries=3, timeout=25) -> str:
    last = None
    for a in range(1, retries+1):
        try:
            r = sess.get(url, timeout=timeout)
            if r.ok:
                return decode_html_bytes(r.content)
            last = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last = e
        time.sleep(a + random.uniform(0, .5))
    raise last or RuntimeError(f"Failed {url}")

def parse_html_to_rows(html: str, year: int):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for table in soup.select("table.t1"):
        hdr_idx = header_index_map_with_colspan(table)
        country, teams = get_country_and_teams(table)
        for tr in iter_club_rows(table):
            tds = tr.find_all("td")
            if len(tds) < 2: continue
            row = extract_row_from_tds(tds, hdr_idx)
            if not row.get("Club") or row.get("Club").lower().startswith("total"): continue
            rows.append({"year": year, "country": country, "teams_in_country": teams, **row})
    return rows

def save_per_year_csv(rows, year, dir_raw):
    per_file = dir_raw / f"club_details_kes_{year}.csv"
    with per_file.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        w.writerows(rows)
    return per_file

# ------------------ main flow ------------------
def main(years=range(2000, 2026+1), overwrite=False):
    years = list(years)
    print("BASE:", BASE)
    print("DATA_BASE (raw dir):", DATA_BASE)
    print("DIR_RAW (Clubs):", DIR_RAW)
    print("DIR_OUT (Output):", DIR_OUT)

    # detect existing per-year files
    expected_files = {y: (DIR_RAW / f"club_details_kes_{y}.csv") for y in years}
    existing_years = [y for y, p in expected_files.items() if p.exists()]

    # choose years to fetch:
    if overwrite:
        to_fetch = years[:]  # all
    else:
        if not existing_years:
            to_fetch = years[:]  # none exist -> fetch all
        else:
            newest = max(years)
            # if some per-year files exist -> only refresh/fetch newest year
            to_fetch = [newest]

    print("Existing per-year files count:", len(existing_years))
    print("Years to fetch this run:", to_fetch)

    per_files = []
    with requests.Session() as sess:
        sess.headers.update(HEADERS_HTTP)
        for y in to_fetch:
            print(f"Fetching year {y} ...")
            try:
                html = fetch_html(sess, url_for_year(y))
            except Exception as e:
                print(f"  FAILED fetch {y}: {e}")
                continue
            rows = parse_html_to_rows(html, y)
            if not rows:
                print(f"  Warning: no rows parsed for {y}")
            saved = save_per_year_csv(rows, y, DIR_RAW)  # always overwrite per-year CSV for fetched years
            per_files.append(saved)
            time.sleep(0.2 + random.uniform(0,0.3))

    # merge all per-year files present in DIR_RAW (including previously existing)
    all_per_files = sorted([p for p in DIR_RAW.glob("club_details_kes_*.csv")])

    # mapping from internal fields -> final output field names (your requested names)
    FIELD_MAP = [
        ("year", "year"),
        ("country", "country"),
        ("teams_in_country", "teams_in_country"),
        ("Pos", "Position"),
        ("Club", "ClubName"),
        ("Cup", "Cup"),
        ("qW", "Quali Wins"),
        ("qD", "Quali Draws"),
        ("qL", "Quali Looses"),
        ("fW", "Finals Wins"),
        ("fD", "Finals Draws"),
        ("fL", "Finals Losses"),
        ("Bonus", "Bonus"),
        ("Points", "Points"),
    ]
    OUTPUT_FIELDNAMES = [out for _in, out in FIELD_MAP]

    merged_rows = []
    for pf in all_per_files:
        try:
            with pf.open("r", encoding="utf-8-sig", newline="") as f:
                r = csv.DictReader(f)
                for row in r:
                    out_row = {}
                    for in_key, out_key in FIELD_MAP:
                        out_row[out_key] = row.get(in_key, "")
                    merged_rows.append(out_row)
        except Exception as e:
            print("Error reading", pf, e)

    # convert to DataFrame and write parquet
    if merged_rows:
        df = pd.DataFrame(merged_rows)
        # ensure columns order exists
        for c in OUTPUT_FIELDNAMES:
            if c not in df.columns:
                df[c] = ""
        df = df[OUTPUT_FIELDNAMES]
    else:
        df = pd.DataFrame(columns=OUTPUT_FIELDNAMES)

    # attempt to write parquet using available engine
    try:
        df.to_parquet(OUT, engine="pyarrow", index=False)
        written_engine = "pyarrow"
    except Exception as e_py:
        try:
            df.to_parquet(OUT, engine="fastparquet", index=False)
            written_engine = "fastparquet"
        except Exception as e_fp:
            raise SystemExit(
                "Failed to write Parquet. Install pyarrow (recommended) or fastparquet:\n"
                "  pip install pyarrow\n"
                "or\n"
                "  pip install fastparquet"
            ) from e_fp

    print(f"Done. Merged {len(df)} rows into {OUT} (engine: {written_engine})")

if __name__ == "__main__":
    # default: overwrite=False -> if any per-year CSV exists, only fetch newest year (2026)
    main(years=range(2000, 2026+1), overwrite=False)
