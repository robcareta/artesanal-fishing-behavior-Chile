# Artisanal fishing behavior Chile



Pipeline to download SERNAPESCA (Chile’s National Fisheries and Aquaculture Service; Spanish acronym) satellite positioning CSV reports for artisanal fleets, build a DuckDB database, filter the Los Lagos region, and generate heatmaps/effort proxies.


![background](assets/background.png)




## Data source

Data are obtained from SERNAPESCA (Chile’s National Fisheries and Aquaculture Service), specifically from the
[*Geographic positions from the satellite positioning system*](https://www.sernapesca.cl/informacion-utilidad/posiciones-geograficas-del-sistema-de-posicionamiento-satelital/) webpage.  
The source is updated daily by SERNAPESCA. The download script is designed to be incremental: when executed, it automatically checks which files are already present locally and only downloads new reports that have not been previously retrieved.


## Repository structure

- `R/` — data pipeline scripts
- `data_raw/` — downloaded CSVs 
- `data_processed/` — filtered outputs 
- `outputs/` — figures and animations
- `assets/` — images used in this README




## How to run

1\. Open R and install packages (or use renv if added).

2\. Run:

   - `source("R/Fisheries_Webbscrapping.R")`

## Outcomes 

![speed_points_id_1](assets/speed_points_id_1.png)

![direction_arrows_id_1](assets/direction_arrows_id_1.png)


## Interactive map

Open the interactive Leaflet map here: `docs/interactive_map.html`  
(Or via GitHub Pages if enabled.)

[![Interactive map preview](assets/interactive_map_preview.png)](https://YOUR_USERNAME.github.io/YOUR_REPO/interactive_map.html)

