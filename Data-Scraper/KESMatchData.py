# scrape_kassiesa_matches_with_winner_2000_2026.py
import re, csv, time, random, requests, sys
from bs4 import BeautifulSoup
from pathlib import Path

try:
    import pandas as pd
except Exception as e:
    raise SystemExit("Pandas is required. Install with: pip install pandas pyarrow") from e

import numpy as np  # for vectorized winner computation

# ------------------ path resolution (relative to this file) ------------------
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
    matches_dir = data_base / "Matches"
    output_dir = base / "Output"
    matches_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return base, data_base, matches_dir, output_dir

BASE, DATA_BASE, DIR_RAW, DIR_OUT = resolve_dirs()
FACT_OUT = DIR_OUT / "KES_match_results.parquet"
CUPS_OUT = DIR_OUT / "KES_cup_names.parquet"
STAGES_OUT = DIR_OUT / "KES_comp_stages.parquet"
CLUBS_OUT = DIR_OUT / "KES_clubs_names.parquet"

HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://kassiesa.net/uefa/data/"}

# per-year CSV schema (ONE ROW PER LEG)
CSV_FIELDS = [
    "season_page","cup","comp_stage","leg_no",
    "home","home_cc","away","away_cc",
    "score","goals_home","goals_away",
    "winner",                # placeholder in per-year CSV (filled later in fact)
    "two_leg_winner"         # winner NAME (temp) only on leg1 of a two-leg tie; empty otherwise
]

# ------------------ provided stage mapping (USER SUPPLIED) ------------------
PROVIDED_STAGE_MAPPING = [
    {"stage_id":1,"comp_stage":"Final","importance":1},
    {"stage_id":2,"comp_stage":"Semi Finals","importance":2},
    {"stage_id":3,"comp_stage":"Quarter Finals","importance":3},
    {"stage_id":4,"comp_stage":"2nd Group Stage","importance":4},
    {"stage_id":5,"comp_stage":"Round 4","importance":4},
    {"stage_id":6,"comp_stage":"Round of 16","importance":4},
    {"stage_id":7,"comp_stage":"Knockout round play-offs","importance":5},
    {"stage_id":8,"comp_stage":"Round 3","importance":6},
    {"stage_id":9,"comp_stage":"1st Group Stage","importance":7},
    {"stage_id":10,"comp_stage":"Group Stage","importance":7},
    {"stage_id":11,"comp_stage":"League Stage","importance":7},
    {"stage_id":12,"comp_stage":"Round 2","importance":8},
    {"stage_id":13,"comp_stage":"Round 1","importance":9},
    {"stage_id":14,"comp_stage":"4th Qualifying or Play-off Round","importance":10},
    {"stage_id":15,"comp_stage":"Qualifying Play-off Round","importance":10},
    {"stage_id":16,"comp_stage":"3rd Qualifying Round","importance":11},
    {"stage_id":17,"comp_stage":"2nd Qualifying Round","importance":12},
    {"stage_id":18,"comp_stage":"Qualifying Round","importance":12},
    {"stage_id":19,"comp_stage":"1st Qualifying Round","importance":13},
    {"stage_id":20,"comp_stage":"Preliminary Round","importance":14},
]

def _norm_key(s): return (s or "").strip().lower()
stage_map_provided = {_norm_key(d["comp_stage"]): d for d in PROVIDED_STAGE_MAPPING}

# ------------------ helpers ------------------
def norm_text(s: str) -> str:
    if not s: return ""
    s = s.replace("\xa0", " ").strip()
    if "Ã" in s or "Â" in s:
        try: s = s.encode("latin-1", "ignore").decode("utf-8", "ignore")
        except: pass
    return s

def parse_score(s: str):
    if not s: return None
    s = s.strip()
    m = re.match(r'^\s*(\d+)\s*[-–]\s*(\d+)\s*$', s)
    if not m: return None
    return int(m.group(1)), int(m.group(2))

def decode_html_bytes(raw: bytes) -> str:
    m = re.search(rb'charset=["\']?([A-Za-z0-9_-]+)', raw[:4000], re.I)
    enc = m.group(1).decode("ascii","ignore").lower() if m else None
    for attempt in (enc, "utf-8", "latin-1"):
        if not attempt: continue
        try: return raw.decode(attempt)
        except Exception: pass
    return raw.decode("utf-8", errors="ignore")

def norm_cup_name(s: str) -> str:
    s = (s or "").strip()
    if not s: return ""
    su = s.upper()
    if "UEFA CUP" in su or su == "UEFA CUP": return "EUROPA LEAGUE"
    if "EUROPA LEAGUE" in su or su.startswith("EUROPA"): return "EUROPA LEAGUE"
    if "CHAMPION" in su: return "CHAMPIONS LEAGUE"
    if "CONFERENCE" in su: return "CONFERENCE LEAGUE"
    return s

# ------------------ parse table -> rows for a year ------------------
def parse_table_matches(table, current_cup, current_stage, year, rows_acc):
    for tr in table.find_all("tr"):
        th = tr.find("th")
        if th:
            if (div_c:=th.find("div", class_="cupheader")):
                current_cup = norm_text(div_c.get_text()); current_stage = ""; continue
            if (div_r:=th.find("div", class_="roundheader")):
                current_stage = norm_text(div_r.get_text()); continue

        tds = tr.find_all("td")
        if not tds: continue
        if len(tds) == 1 and tds[0].get("colspan"): continue

        # main row (>=6 tds)
        if len(tds) >= 6:
            t1_td, t1_cc_td, t2_td, t2_cc_td = tds[0], tds[1], tds[2], tds[3]
            team1 = norm_text(t1_td.get_text())
            team1_cc = norm_text(t1_cc_td.get_text())
            team2 = norm_text(t2_td.get_text())
            team2_cc = norm_text(t2_cc_td.get_text())
            leg1_txt = norm_text(tds[4].get_text())
            leg2_txt = norm_text(tds[5].get_text())
            l1 = parse_score(leg1_txt)
            l2 = parse_score(leg2_txt)

            # two-leg winner NAME via <b> marker
            t1_bold = bool(t1_td.find("b"))
            t2_bold = bool(t2_td.find("b"))
            two_leg_winner_name = team1 if (t1_bold and not t2_bold) else (team2 if (t2_bold and not t1_bold) else "")

            # LEG 1 (team1 home)
            if team1 or team2:
                gh1 = str(l1[0]) if l1 else ""
                ga1 = str(l1[1]) if l1 else ""
                row1 = {
                    "season_page": year,
                    "cup": current_cup,
                    "comp_stage": current_stage,
                    "leg_no": 1,
                    "home": team1, "home_cc": team1_cc,
                    "away": team2, "away_cc": team2_cc,
                    "score": leg1_txt, "goals_home": gh1, "goals_away": ga1,
                    "winner": "",  # computed later in fact
                    "two_leg_winner": two_leg_winner_name if leg2_txt else ""
                }
                rows_acc.append(row1)

            # LEG 2 (team2 home): swap teams; swap goals; score = home-away (after swap)
            if leg2_txt:
                gh2_src = str(l2[0]) if l2 else ""   # printed for team1
                ga2_src = str(l2[1]) if l2 else ""   # printed for team2
                gh2 = ga2_src  # now home is team2
                ga2 = gh2_src  # away is team1
                score2 = (gh2 + "-" + ga2) if gh2_src!="" and ga2_src!="" else leg2_txt
                row2 = {
                    "season_page": year,
                    "cup": current_cup,
                    "comp_stage": current_stage,
                    "leg_no": 2,
                    "home": team2, "home_cc": team2_cc,
                    "away": team1, "away_cc": team1_cc,
                    "score": score2, "goals_home": gh2, "goals_away": ga2,
                    "winner": "",  # computed later in fact
                    "two_leg_winner": ""
                }
                rows_acc.append(row2)
            continue

        # fallback for >=4 tds (older formats)
        if len(tds) >= 4:
            t1_td, t1_cc_td, t2_td, t2_cc_td = tds[0], tds[1], tds[2], tds[3]
            team1 = norm_text(t1_td.get_text())
            team1_cc = norm_text(t1_cc_td.get_text()) if len(tds)>1 else ""
            team2 = norm_text(t2_td.get_text()) if len(tds)>2 else ""
            team2_cc = norm_text(t2_cc_td.get_text()) if len(tds)>3 else ""
            leg1_txt = norm_text(tds[4].get_text()) if len(tds)>4 else ""
            leg2_txt = norm_text(tds[5].get_text()) if len(tds)>5 else ""
            l1 = parse_score(leg1_txt)
            l2 = parse_score(leg2_txt)

            t1_bold = bool(t1_td.find("b"))
            t2_bold = bool(t2_td.find("b"))
            two_leg_winner_name = team1 if (t1_bold and not t2_bold) else (team2 if (t2_bold and not t1_bold) else "")

            if team1 or team2:
                gh1 = str(l1[0]) if l1 else ""
                ga1 = str(l1[1]) if l1 else ""
                row1 = {
                    "season_page": year,
                    "cup": current_cup,
                    "comp_stage": current_stage,
                    "leg_no": 1,
                    "home": team1, "home_cc": team1_cc,
                    "away": team2, "away_cc": team2_cc,
                    "score": leg1_txt, "goals_home": gh1, "goals_away": ga1,
                    "winner": "",
                    "two_leg_winner": two_leg_winner_name if leg2_txt else ""
                }
                rows_acc.append(row1)
            if leg2_txt:
                gh2_src = str(l2[0]) if l2 else ""
                ga2_src = str(l2[1]) if l2 else ""
                gh2 = ga2_src
                ga2 = gh2_src
                score2 = (gh2 + "-" + ga2) if gh2_src!="" and ga2_src!="" else leg2_txt
                row2 = {
                    "season_page": year,
                    "cup": current_cup,
                    "comp_stage": current_stage,
                    "leg_no": 2,
                    "home": team2, "home_cc": team2_cc,
                    "away": team1, "away_cc": team1_cc,
                    "score": score2, "goals_home": gh2, "goals_away": ga2,
                    "winner": "",
                    "two_leg_winner": ""
                }
                rows_acc.append(row2)
    return current_cup, current_stage

# ------------------ per-year CSV writer ------------------
def save_per_year_csv(rows, year, dir_raw):
    per_file = dir_raw / f"match_results_kes_{year}.csv"
    with per_file.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader(); w.writerows(rows)
    return per_file

# ------------------ fetch flow and merge into parquet with dims ------------------
def url_for_year(y: int) -> str:
    if 2018 <= y <= 2026: method = "method5"
    elif 2009 <= y <= 2017: method = "method4"
    elif 2004 <= y <= 2008: method = "method3"
    elif 2000 <= y <= 2003: method = "method2"
    else: raise ValueError("Unsupported year")
    return f"https://kassiesa.net/uefa/data/{method}/match{y}.html"

def fetch_html(sess: requests.Session, url: str, retries=3, timeout=25):
    last = None
    for a in range(1, retries+1):
        try:
            r = sess.get(url, timeout=timeout)
            if r.ok: return decode_html_bytes(r.content)
            last = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e: last = e
        time.sleep(a + random.uniform(0, .5))
    raise last or RuntimeError(f"Failed {url}")

def main(years=range(2000, 2026+1), overwrite=False):
    years = list(years)
    print("BASE:", BASE); print("DATA_BASE (raw):", DATA_BASE)
    print("DIR_RAW (Matches):", DIR_RAW); print("DIR_OUT (Output):", DIR_OUT)

    expected_files = {y: (DIR_RAW / f"match_results_kes_{y}.csv") for y in years}
    existing_years = [y for y,p in expected_files.items() if p.exists()]

    if overwrite or not existing_years:
        to_fetch = years[:]
    else:
        newest = max(years)
        to_fetch = [newest] if newest not in existing_years else [newest]
    print("Existing per-year files count:", len(existing_years))
    print("Years to fetch this run:", to_fetch)

    with requests.Session() as sess:
        sess.headers.update(HEADERS)
        for y in to_fetch:
            print("Fetching", y, "...")
            try: html = fetch_html(sess, url_for_year(y))
            except Exception as e:
                print("  FAILED fetch", y, ":", e); continue
            soup = BeautifulSoup(html, "html.parser")
            current_cup = ""; current_stage = ""; year_rows = []
            for table in soup.select("table.t1"):
                current_cup, current_stage = parse_table_matches(table, current_cup, current_stage, y, year_rows)
            save_per_year_csv(year_rows, y, DIR_RAW)
            time.sleep(0.2 + random.random()*0.3)

    all_per_files = sorted([p for p in DIR_RAW.glob("match_results_kes_*.csv")])
    frames = []
    for pf in all_per_files:
        try: frames.append(pd.read_csv(pf, encoding="utf-8-sig", dtype=str))
        except Exception as e: print("Error reading", pf, e)

    df = pd.concat(frames, ignore_index=True).fillna("") if frames else pd.DataFrame(columns=CSV_FIELDS)

    # ------------------ create dimensions ------------------
    df['cup_norm_kes'] = df['cup'].astype(str).map(norm_cup_name)

    cups_unique = [c for c in df['cup_norm_kes'].unique().tolist() if c]
    CUP_ID_FIXED = {'CHAMPIONS LEAGUE': 1, 'EUROPA LEAGUE': 2, 'CONFERENCE LEAGUE': 3}

    cup_rows, used = [], set()
    for cname, cid in CUP_ID_FIXED.items():
        if cname in cups_unique: cup_rows.append({'cup_id': cid, 'cup_kes': cname}); used.add(cid)
    remaining = sorted(c for c in cups_unique if c not in CUP_ID_FIXED)
    next_id = 4
    for cname in remaining:
        while next_id in used: next_id += 1
        cup_rows.append({'cup_id': next_id, 'cup_kes': cname}); used.add(next_id); next_id += 1
    dim_cups = pd.DataFrame(cup_rows).sort_values('cup_id').reset_index(drop=True)
    cup_map = dict(zip(dim_cups['cup_kes'], dim_cups['cup_id']))

    unique_stages = [s for s in df['comp_stage'].astype(str).str.strip().unique().tolist() if s]
    dim_stages_rows = {0: {'stage_id': 0, 'comp_stage_kes': 'Winner', 'importance': 0}}
    for item in PROVIDED_STAGE_MAPPING:
        sid = item['stage_id']; dim_stages_rows[sid] = {'stage_id': sid, 'comp_stage_kes': item['comp_stage'], 'importance': item['importance']}
    next_stage_id = max(dim_stages_rows.keys()) + 1
    # add any unseen stages
    seen_norm = {_norm_key(v['comp_stage_kes']) for v in dim_stages_rows.values()}
    for s in sorted(unique_stages):
        nk = _norm_key(s)
        if nk not in seen_norm:
            dim_stages_rows[next_stage_id] = {'stage_id': next_stage_id, 'comp_stage_kes': s, 'importance': 0}
            seen_norm.add(nk); next_stage_id += 1
    dim_stages = pd.DataFrame([dim_stages_rows[k] for k in sorted(dim_stages_rows.keys())])
    stage_map_norm = {_norm_key(r['comp_stage_kes']): r['stage_id'] for _, r in dim_stages.iterrows()}

    clubs = sorted({c for c in (df['home'].astype(str).str.strip().tolist() + df['away'].astype(str).str.strip().tolist()) if c})
    dim_clubs = pd.DataFrame({'club_id': range(1, len(clubs)+1), 'club_name_kes': clubs})
    club_map = dict(zip(dim_clubs['club_name_kes'], dim_clubs['club_id']))

    # ------------------ fact table (ONE ROW PER LEG) ------------------
    fact = pd.DataFrame()
    fact['season_page']   = df['season_page'].astype(str)
    fact['leg_no']        = pd.to_numeric(df['leg_no'], errors='coerce').astype('Int64')
    fact['cup_id']        = df['cup_norm_kes'].map(lambda x: cup_map.get((x or "").strip(), pd.NA))
    fact['comp_stage_id'] = df['comp_stage'].map(lambda x: stage_map_norm.get(_norm_key(x), pd.NA))
    fact['home_club_id']  = df['home'].map(lambda x: club_map.get((x or "").strip(), pd.NA)).astype('Int64')
    fact['away_club_id']  = df['away'].map(lambda x: club_map.get((x or "").strip(), pd.NA)).astype('Int64')
    fact['home_cc']       = df['home_cc'].astype(str)
    fact['away_cc']       = df['away_cc'].astype(str)
    fact['score']         = df['score'].astype(str)
    fact['goals_home']    = df['goals_home'].astype(str)
    fact['goals_away']    = df['goals_away'].astype(str)

    # compute per-leg winner: id of winner or "draw"
    gh = pd.to_numeric(fact['goals_home'], errors='coerce')
    ga = pd.to_numeric(fact['goals_away'], errors='coerce')
    home_id = fact['home_club_id']
    away_id = fact['away_club_id']

    winner_id = np.where(gh > ga, home_id, np.where(gh < ga, away_id, np.nan))
    winner_str = np.where(gh == ga, "draw", pd.Series(winner_id, dtype="float").astype('Int64').astype(str))
    # if goals missing -> empty
    missing_mask = gh.isna() | ga.isna()
    winner_str = pd.Series(winner_str).mask(missing_mask, "")

    fact['winner'] = winner_str.astype(str)

    # map two-leg winner NAME -> club_id (only for leg1)
    tlw_id = df['two_leg_winner'].map(lambda x: club_map.get((x or "").strip(), pd.NA))
    tlw_id = tlw_id.where(df['leg_no'].astype(str) == "1", pd.NA)
    fact['two_leg_winner'] = pd.to_numeric(tlw_id, errors='coerce').astype('Int64')

    for idcol in ['cup_id','comp_stage_id']:
        fact[idcol] = pd.to_numeric(fact[idcol], errors='coerce').astype('Int64')

    # ------------------ write parquet ------------------
    DIR_OUT.mkdir(parents=True, exist_ok=True)
    try:
        dim_cups.to_parquet(CUPS_OUT, engine='pyarrow', index=False)
        dim_stages.to_parquet(STAGES_OUT, engine='pyarrow', index=False)
        dim_clubs.to_parquet(CLUBS_OUT, engine='pyarrow', index=False)
        fact.to_parquet(FACT_OUT, engine='pyarrow', index=False)
        engine_used = "pyarrow"
    except Exception:
        try:
            dim_cups.to_parquet(CUPS_OUT, engine='fastparquet', index=False)
            dim_stages.to_parquet(STAGES_OUT, engine='fastparquet', index=False)
            dim_clubs.to_parquet(CLUBS_OUT, engine='fastparquet', index=False)
            fact.to_parquet(FACT_OUT, engine='fastparquet', index=False)
            engine_used = "fastparquet"
        except Exception as e_fp:
            raise SystemExit("Failed to write parquet. Install pyarrow (recommended) or fastparquet.") from e_fp

    print(f"Done. Wrote fact {FACT_OUT} ({len(fact)} rows) and dims: {CUPS_OUT}, {STAGES_OUT}, {CLUBS_OUT} (engine: {engine_used})")

if __name__ == "__main__":
    main(years=range(2000, 2026+1), overwrite=False)
