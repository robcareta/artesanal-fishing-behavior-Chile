# artesanal-fishing-behavior-Chile



![background](assets/background.png)



\# Artisanal fishing behavior Chile



Pipeline to download SERNAPESCA SERNAPESCA (Chile’s National Fisheries and Aquaculture Service; Spanish acronym) satellite positioning CSV reports for artisanal fleets, build a DuckDB database, filter the Los Lagos region, and generate heatmaps/effort proxies.



\## Data source

SERNAPESCA: Geographic positions from the satellite positioning system (vessel tracking data)



\## Repository structure

\- `R/` scripts

\- `assets/` images used in this README



\## How to run

1\. Open R and install packages (or use renv if added).

2\. Run:

   - `source("R/Fisheries_Webbscrapping.R")`

