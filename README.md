# artesanal-fishing-behavior-Chile



!\[](assets/background.png)



\# artisanal-fishing-behavior-Chile



Pipeline to download SERNAPESCA satellite positioning CSV reports for artisanal fleets, build a DuckDB database, filter the Los Lagos region, and generate heatmaps/effort proxies.



\## Data source

SERNAPESCA: Posiciones geográficas del sistema de posicionamiento satelital.



\## Repository structure

\- `R/` scripts

\- `data\\\_raw/` downloaded CSVs (ignored)

\- `data\\\_processed/` filtered outputs (ignored)

\- `outputs/` figures/animations

\- `assets/` images used in this README



\## How to run

1\. Open R and install packages (or use renv if added).

2\. Run:

   - `source("R/main.R")`

