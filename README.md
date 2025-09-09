<p align="center">
  <img src="baner.png" alt="Project banner" width="100%" />
</p>

<p align="center" style="margin-top:18px;">
  <a href="https://app.powerbi.com/view?r=eyJrIjoiYzk0MTQ5MWEtMTE1Yy00ZGI0LWE2MzctNjBlZGRiMzI0ZjBkIiwidCI6IjVhNDZhNzkxLTk4MTQtNDlmNC05YTM0LTE4OGU2ZTRjMmM3YiJ9&pageName=c6f3abe677e68e7d004d&fbclid=IwY2xjawMsCptleHRuA2FlbQIxMABicmlkETFUYVJDVjdYRkJKcU5iczRYAR6a2cKnF19ZUHmJ8QnL1lRyy8zWomcLIKKA9fRtS_cgH803XhlUroC77SCRVA_aem_4Sha8MswVPpyMZX3vbQ-rg"
     target="_blank" rel="noopener"
     style="display:inline-flex; align-items:center; gap:20px; text-decoration:none;">
    <img src="Images/powerbi.png" alt="Power BI" width="24" height="24" style="display:block;">
    <span style="font-size:32px; height:100%; font-weight:700; line-height:1; color:inherit;">
      OPEN LIVE REPORT — Power BI Service
    </span>
  </a>
</p>


---

## Overview
This interactive report shows how European countries and their clubs have performed in UEFA competitions since 2000.  
- Page 1 (**Country Ranking**) — country-level KPIs (UEFA points, rank), historical trends and club contributions.  
- Page 2 (**Country Matchups**) — direct comparisons between two countries: aggregate KPIs and season-by-season club fixtures/results.


<center>
  <img src="https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExMWt6YnExMGtwMnRwZGQyaHM4OW8yZWs2bXRiZzRiZnUxMW9idXQwdiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q/ilx0yACNcfhkFRsyZo/giphy.gif" alt="Animated preview" width="600" />
</center>



## Data sources
- **kassiesa.net** — historical match results, club & country tables (used for match history and country points).  
- **UEFA** — official club & country coefficients (UEFA JSON endpoints).



## Key engineering highlights
- **Columnar storage (Parquet)** — final datasets are written to Parquet (pyarrow/fastparquet) for compact storage and very fast reads during Power BI refreshes.  
- **Star schema** — ETL produces skinny dimension tables (clubs, countries, competitions, seasons) and a central fact table (matches / coefficient contributions) for optimal Power BI performance and simple DAX.  
- **Idempotent & cached ETL** — per-year raw caches avoid redundant hitting of remote sources; scripts are safe to re-run and include provenance (source URL + timestamp).  
- **Minimal Power Query transforms** — most heavy lifting is done in Python (cleaning, normalization, joins); Power Query handles light shaping and visual-level formatting.  
- **Scalable & production-ready** — design choices (partitioned Parquet, schema checks, clear separation of extract/transform/load) reflect senior BI engineering standards.



## Quick view / run steps
1. Ensure `Output/` (or configured folder) contains the Parquet files produced by the `Data-Scraper` scripts.  
2. Open `Football Analysis Dashboard.pbix` in Power BI Desktop and refresh (point sources to the `Output/` folder).  
3. Or click **OPEN LIVE REPORT — Power BI Service** above to view the published report.




