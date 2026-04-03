## fishermen_pipeline.R
## Reads FISHERMEN_DATA.xlsx (all years), filters Los Lagos (Región de Captura == 10),
## merges with VMS artesanal-only data on vessel name, generates monthly maps by species.

library(readxl)
library(data.table)
library(DBI)
library(duckdb)
library(ggplot2)
library(patchwork)
library(sf)
library(maptiles)
library(tidyterra)
library(terra)

## ── Paths ──────────────────────────────────────────────────────────────────
xl_path  <- "C:/Users/rober/UF Dropbox/Roberto Cardenas Retamal/PHD/PAPERS/10. AQUACULTURE AND FISHERIES INTERACTION CHILE/Data/FISHERMEN_DATA.xlsx"
vms_csv  <- "C:/Users/rober/UF Dropbox/Roberto Cardenas Retamal/PHD/PAPERS/10. AQUACULTURE AND FISHERIES INTERACTION CHILE/Claude/artesanal-fishing-behavior-Chile/data_processed/los_lagos_artesanal_only.csv"
out_dir  <- "C:/Users/rober/UF Dropbox/Roberto Cardenas Retamal/PHD/PAPERS/10. AQUACULTURE AND FISHERIES INTERACTION CHILE/Claude/artesanal-fishing-behavior-Chile/new/outputs"
dir.create(out_dir, showWarnings=FALSE, recursive=TRUE)

## ── 1. Read all sheets, filter Los Lagos ───────────────────────────────────
cat("[1] Reading FISHERMEN_DATA.xlsx sheets...\n")
sheets <- excel_sheets(xl_path)
cat("Sheets found:", paste(sheets, collapse=", "), "\n")

fish_list <- lapply(sheets, function(sh) {
  df <- tryCatch(
    as.data.table(read_excel(xl_path, sheet=sh, skip=3)),
    error = function(e) { cat("  ERROR reading sheet", sh, ":", conditionMessage(e), "\n"); NULL }
  )
  if (is.null(df)) return(NULL)
  cat(sprintf("  Sheet %s: %d rows, %d cols\n", sh, nrow(df), ncol(df)))
  # Filter Los Lagos
  if (!"Región de Captura" %in% names(df)) {
    cat("  WARNING: 'Región de Captura' column not found in sheet", sh, "\n")
    return(NULL)
  }
  df <- df[`Región de Captura` == 10]
  cat(sprintf("    -> Los Lagos rows: %d\n", nrow(df)))
  df
})

fish_all <- rbindlist(fish_list, fill=TRUE)
cat(sprintf("\nTotal Los Lagos rows (all years): %d\n", nrow(fish_all)))

## Save processed fishermen data
fish_csv <- "C:/Users/rober/UF Dropbox/Roberto Cardenas Retamal/PHD/PAPERS/10. AQUACULTURE AND FISHERIES INTERACTION CHILE/Claude/artesanal-fishing-behavior-Chile/data_processed/fishermen_loslagos.csv"
fwrite(fish_all, fish_csv)
cat("Saved:", fish_csv, "\n")

## ── 2. Explore key columns ─────────────────────────────────────────────────
cat("\n[2] Column overview:\n")
cat("  Vessel name col: 'Embarcación'\n")
cat("  Sample vessel names:\n")
print(head(sort(unique(fish_all[["Embarcación"]])), 20))

cat("\n  Top species (all years, Los Lagos):\n")
sp_sum <- fish_all[, .(captura_ton = sum(`Captura (Ton)`, na.rm=TRUE), n=.N),
                   by=Especie][order(-captura_ton)]
print(head(sp_sum, 20))

cat("\n  Year × month coverage:\n")
yr_mo <- fish_all[, .N, by=.(anioLlegada, mesLlegada)][order(anioLlegada, mesLlegada)]
print(yr_mo)

## ── 3. Load VMS data via DuckDB, sample vessel names ──────────────────────
cat("\n[3] Sampling VMS vessel names from artesanal-only CSV...\n")
con <- dbConnect(duckdb())
on.exit(dbDisconnect(con, shutdown=TRUE), add=TRUE)

vms_vessels <- dbGetQuery(con, sprintf("
  SELECT DISTINCT trim(movil) AS movil_raw
  FROM read_csv(['%s'],
    delim=',', header=true, all_varchar=true, ignore_errors=true, null_padding=true,
    columns={movil: 'VARCHAR', fecha: 'VARCHAR', latitud: 'VARCHAR', longitud: 'VARCHAR',
             rumbo: 'VARCHAR', velocidad: 'VARCHAR', source_file: 'VARCHAR', category: 'VARCHAR'})
  WHERE movil IS NOT NULL
  ORDER BY movil_raw
", vms_csv))
cat(sprintf("  Unique VMS vessel names: %d\n", nrow(vms_vessels)))
cat("  Sample VMS names:\n")
print(head(vms_vessels$movil_raw, 20))

## Clean suffix from VMS names (same regex used later in step 5)
clean_suffix <- function(x) toupper(trimws(sub("\\s*\\([A-Za-z0-9/\\-]+\\)\\s*$", "", x)))
vms_vessels$movil_clean <- clean_suffix(vms_vessels$movil_raw)

## Normalize fishermen vessel names
fish_all[, vessel_clean := toupper(trimws(Embarcación))]

cat("\n  Sample VMS cleaned names:\n")
print(head(vms_vessels$movil_clean, 20))
cat("\n  Sample fishermen cleaned names:\n")
print(head(sort(unique(fish_all$vessel_clean)), 20))

## ── 4. Preliminary match diagnostics (pre-load) ───────────────────────────
cat("\n[4] Preliminary match diagnostics...\n")
fish_unique <- unique(fish_all$vessel_clean)
pre_matched <- intersect(fish_unique, unique(vms_vessels$movil_clean))
cat(sprintf("  Fishermen vessels: %d\n", length(fish_unique)))
cat(sprintf("  VMS unique (pre-load): %d\n", uniqueN(vms_vessels$movil_clean)))
cat(sprintf("  Pre-load matched: %d\n", length(pre_matched)))

## ── 5. Load ALL VMS fishing pings; join with matched vessels in R ──────────
cat("\n[5] Loading all VMS fishing pings (0.1–3.0 kt)...\n")
## Load via DuckDB without vessel filter; clean vessel names in R (more reliable)
vms_data <- as.data.table(dbGetQuery(con, sprintf("
  SELECT
    trim(movil) AS movil_raw,
    TRY_CAST(latitud  AS DOUBLE) AS lat,
    TRY_CAST(longitud AS DOUBLE) AS lon,
    source_file,
    category
  FROM read_csv(['%s'],
    delim=',', header=true, all_varchar=true, ignore_errors=true, null_padding=true,
    columns={movil: 'VARCHAR', fecha: 'VARCHAR', latitud: 'VARCHAR', longitud: 'VARCHAR',
             rumbo: 'VARCHAR', velocidad: 'VARCHAR', source_file: 'VARCHAR', category: 'VARCHAR'})
  WHERE
    TRY_CAST(latitud  AS DOUBLE) BETWEEN -44.5 AND -40.5
    AND TRY_CAST(longitud AS DOUBLE) BETWEEN -75.5 AND -71.5
    AND TRY_CAST(velocidad AS DOUBLE) BETWEEN 0.1 AND 3.0
", vms_csv)))

cat(sprintf("  Total VMS fishing pings: %d\n", nrow(vms_data)))

## Clean vessel names in R (same function as step 3)
vms_data[, vessel_clean := clean_suffix(movil_raw)]

## Extract year and month from source_file in R
## Patterns observed:
##  admin_2019:       report-YYYY-MM-DD_HH_MM_SS-sernapesca-admin.csv  (positions 8-11 = year, 13-14 = month)
##  flota_artesanal:  report_31_YYYYMMDD_flota_artesanal.csv            (positions 11-14 = year, 15-16 = month)
##  artesanal_legacy: artesanales_DD.MM.YYYY.csv                        (positions 19-22 = year, 16-17 = month)
is_admin  <- grepl("^report-[0-9]{4}-", vms_data$source_file)
is_flota  <- grepl("^report_31_[0-9]{8}", vms_data$source_file)
is_artes  <- grepl("^artesanales_", vms_data$source_file)

vms_data[, yr := NA_integer_]
vms_data[is_admin,  yr := as.integer(substr(source_file, 8,  11))]
vms_data[is_flota,  yr := as.integer(substr(source_file, 11, 14))]
vms_data[is_artes,  yr := as.integer(substr(source_file, 19, 22))]

vms_data[, mo := NA_integer_]
vms_data[is_admin,  mo := as.integer(substr(source_file, 13, 14))]
vms_data[is_flota,  mo := as.integer(substr(source_file, 15, 16))]
vms_data[is_artes,  mo := as.integer(substr(source_file, 16, 17))]

cat(sprintf("  Year range: %d–%d\n", min(vms_data$yr, na.rm=TRUE), max(vms_data$yr, na.rm=TRUE)))
cat(sprintf("  Month NAs: %.1f%%\n", 100*mean(is.na(vms_data$mo))))

## Recompute matched from ACTUAL loaded vessel names (consistent cleaning)
matched   <- intersect(fish_unique, unique(vms_data$vessel_clean))
unmatched <- setdiff(fish_unique, unique(vms_data$vessel_clean))
cat(sprintf("\n[4b] Final match diagnostics (post-load):\n"))
cat(sprintf("  VMS unique vessel names: %d\n", uniqueN(vms_data$vessel_clean)))
cat(sprintf("  Matched:                 %d\n", length(matched)))
cat(sprintf("  Unmatched fishermen:     %d\n", length(unmatched)))

## Keep only matched vessels
vms_data <- vms_data[vessel_clean %in% matched]
cat(sprintf("  VMS fishing pings for matched vessels: %d\n", nrow(vms_data)))
cat(sprintf("  Vessels: %d unique\n", uniqueN(vms_data$vessel_clean)))

## ── 6. Merge fishermen landings with VMS spatial ──────────────────────────
cat("\n[6] Merging...\n")
## Note: We merge on vessel×year only (not month) because landing date ≠ fishing date.
## VMS spatial location is averaged across all fishing pings for each vessel-year.

## Aggregate landings: vessel × year × month × species (keep monthly for time series)
fish_agg <- fish_all[, .(captura_ton = sum(`Captura (Ton)`, na.rm=TRUE)),
                     by=.(vessel_clean, yr=anioLlegada, mo=mesLlegada, Especie)]

## VMS spatial summary: mean fishing location per vessel × year
vms_yr_agg <- vms_data[!is.na(yr),
                       .(lat_mean   = mean(lat, na.rm=TRUE),
                         lon_mean   = mean(lon, na.rm=TRUE),
                         fishing_pings = .N),
                       by=.(vessel_clean, yr)]

## Also: monthly VMS for detailed time series
vms_mo_agg <- vms_data[!is.na(yr) & !is.na(mo),
                       .(lat_mean  = mean(lat, na.rm=TRUE),
                         lon_mean  = mean(lon, na.rm=TRUE),
                         fishing_pings = .N),
                       by=.(vessel_clean, yr, mo)]

## Primary merge on vessel × year
merged <- merge(fish_agg, vms_yr_agg, by=c("vessel_clean","yr"), all.x=TRUE)
cat(sprintf("  Merged rows (vessel×year): %d\n", nrow(merged)))
cat(sprintf("  Rows with spatial data: %d (%.1f%%)\n",
            sum(!is.na(merged$lat_mean)), 100*mean(!is.na(merged$lat_mean))))
cat(sprintf("  Unique vessel×year combos with spatial: %d\n",
            uniqueN(merged[!is.na(lat_mean), .(vessel_clean, yr)])))

## ── 7. Top species & map base ─────────────────────────────────────────────
top_sp <- fish_all[vessel_clean %in% matched,
                   .(total_ton = sum(`Captura (Ton)`, na.rm=TRUE)), by=Especie
                   ][order(-total_ton)][1:8, Especie]
cat("\nTop 8 species for matched vessels:\n")
print(top_sp)

cat("\n[7] Loading satellite tiles...\n")
bbox_sf   <- st_bbox(c(xmin=-75.5, xmax=-71.5, ymin=-44.5, ymax=-40.5), crs=st_crs(4326))
bbox_poly <- st_as_sfc(bbox_sf)
tiles <- suppressWarnings(
  get_tiles(bbox_poly, provider="Esri.OceanBasemap", zoom=7, crop=TRUE)
)

## Plotting helper
map_theme <- function(base=9) {
  theme_minimal(base_size=base) +
  theme(
    plot.background  = element_rect(fill="white", colour=NA),
    panel.background = element_rect(fill="white", colour=NA),
    strip.background = element_rect(fill="grey93", colour=NA),
    strip.text       = element_text(size=base - 2),
    legend.position  = "right",
    axis.text        = element_text(size=base - 3),
    panel.grid       = element_line(colour="grey80", linewidth=0.2)
  )
}

map_data <- merged[Especie %in% top_sp & !is.na(lat_mean) & !is.na(lon_mean) & yr >= 2019]

## ── 8. MAP A: Kernel-density spatial catch by species (all years) ─────────
cat("[8] Map A — kernel density by species...\n")

p_kde <- ggplot() +
  geom_spatraster_rgb(data=tiles, maxcell=5e6) +
  stat_density_2d(
    data    = map_data,
    aes(x=lon_mean, y=lat_mean, weight=captura_ton,
        fill=after_stat(ndensity), alpha=after_stat(ndensity)),
    geom    = "raster", contour=FALSE, n=200, h=c(0.6, 0.6)
  ) +
  scale_fill_viridis_c(option="plasma", name="Relative\ncatch density",
                       limits=c(0, 1)) +
  scale_alpha_continuous(range=c(0, 0.85), guide="none") +
  facet_wrap(~Especie, ncol=4) +
  coord_sf(xlim=c(-75.5,-71.5), ylim=c(-44.5,-40.5), expand=FALSE) +
  labs(x=NULL, y=NULL,
       caption="Kernel density of catch-weighted fishing locations from VMS (0.1–3.0 kt, 2019–2025).") +
  map_theme()

out_kde <- file.path(out_dir, "species_map_kde_loslagos.png")
ggsave(out_kde, p_kde, width=14, height=10, dpi=150)
cat("Saved:", out_kde, "\n")

## ── 9. MAP B: Year-faceted spatial catch — top 4 species ─────────────────
cat("[9] Map B — year-faceted maps for top 4 species...\n")

top4   <- top_sp[1:4]
map_yr <- merged[Especie %in% top4 & !is.na(lat_mean) & !is.na(lon_mean) & yr >= 2019]
## Nicely ordered year factor
map_yr[, yr_f := factor(yr)]

p_yr_list <- lapply(top4, function(sp) {
  d <- map_yr[Especie == sp]
  ggplot() +
    geom_spatraster_rgb(data=tiles, maxcell=5e6) +
    stat_density_2d(
      data    = d,
      aes(x=lon_mean, y=lat_mean, weight=captura_ton,
          fill=after_stat(ndensity), alpha=after_stat(ndensity)),
      geom    = "raster", contour=FALSE, n=150, h=c(0.7, 0.7)
    ) +
    scale_fill_viridis_c(option="plasma", limits=c(0, 1),
                         name="Relative\ndensity") +
    scale_alpha_continuous(range=c(0, 0.85), guide="none") +
    facet_wrap(~yr_f, nrow=2) +
    coord_sf(xlim=c(-75.5,-71.5), ylim=c(-44.5,-40.5), expand=FALSE) +
    labs(title=sp, x=NULL, y=NULL) +
    map_theme(8) +
    theme(plot.title=element_text(size=9, face="bold", hjust=0.5))
})

p_yr_grid <- wrap_plots(p_yr_list, ncol=2) +
  plot_annotation(
    caption="Catch-weighted kernel density by year. VMS (0.1–3.0 kt) × SERNAPESCA landings.",
    theme=theme(
      plot.background = element_rect(fill="white", colour=NA),
      plot.caption    = element_text(size=7)
    )
  )

out_yr <- file.path(out_dir, "species_map_byyear_loslagos.png")
ggsave(out_yr, p_yr_grid, width=16, height=14, dpi=150)
cat("Saved:", out_yr, "\n")

## ── 10. MAP C: Unmatched vessels via caleta location ─────────────────────
cat("[10] Map C — unmatched vessels via caleta landing location...\n")

## Caleta coordinates lookup: major landing ports in Los Lagos region
caleta_coords <- data.table(
  caleta  = c("PUERTO MONTT","CALBUCO","MAULLIN","MAULLÍN","CARELMAPU",
              "CASTRO","ANCUD","QUELLÓN","QUELLON","CHONCHI","DALCAHUE",
              "ACHAO","CURACO DE VELEZ","QUEMCHI","CHAITÉN","CHAITEN",
              "HORNOPIREN","HORNOPIRÉN","COCHAMÓ","COCHAMO",
              "SAN ANTONIO HUALAIHUE","HUALAIHUE",
              "PUELO","PARGUA","COÑARIPE","HUELMO","MELINKA",
              "CALETA LA ARENA","PURRANQUE","FRESIA","FRUTILLAR"),
  lat     = c(-41.469,-41.773,-41.625,-41.625,-41.717,
              -42.476,-41.872,-43.122,-43.122,-42.617,-42.383,
              -42.467,-42.417,-42.133,-42.917,-42.917,
              -41.931,-41.931,-41.495,-41.495,
              -42.100,-42.100,
              -41.650,-41.787,-39.654,-41.847,-43.897,
              -41.540,-40.934,-40.649,-41.082),
  lon     = c(-72.942,-73.132,-73.583,-73.583,-73.700,
              -73.760,-73.825,-73.615,-73.615,-73.783,-73.650,
              -73.500,-73.633,-73.500,-72.717,-72.717,
              -72.433,-72.433,-72.302,-72.302,
              -72.500,-72.500,
              -72.350,-73.850,-72.049,-73.150,-74.000,
              -72.797,-73.012,-73.548,-73.028)
)
## Collapse duplicates (e.g. MAULLIN/MAULLÍN)
caleta_coords <- caleta_coords[!duplicated(toupper(caleta))]
caleta_coords[, caleta_up := toupper(caleta)]

## Unmatched vessel landings: aggregate by caleta × species
unmatched_fish <- fish_all[!vessel_clean %in% matched &
                           `Caleta de Desembarque` %in% caleta_coords$caleta_up]
## Normalise caleta name
unmatched_fish[, caleta_up := toupper(`Caleta de Desembarque`)]

caleta_sp <- unmatched_fish[Especie %in% top_sp,
               .(captura_ton = sum(`Captura (Ton)`, na.rm=TRUE)),
               by=.(caleta_up, Especie)]
caleta_sp <- merge(caleta_sp, caleta_coords[, .(caleta_up, lat, lon)],
                   by="caleta_up", all.x=TRUE)
caleta_sp <- caleta_sp[!is.na(lat)]

cat(sprintf("  Unmatched vessel catch at known caletas: %.0f ton across %d caletas\n",
            sum(caleta_sp$captura_ton), uniqueN(caleta_sp$caleta_up)))

## One map per species: KDE for matched vessels + bubble for unmatched caletas
p_c_list <- lapply(top4, function(sp) {
  d_m  <- map_data[Especie == sp]         # matched (KDE)
  d_c  <- caleta_sp[Especie == sp]        # unmatched (bubble)
  gg <- ggplot() +
    geom_spatraster_rgb(data=tiles, maxcell=5e6)
  if (nrow(d_m) > 0) {
    gg <- gg +
      stat_density_2d(
        data=d_m,
        aes(x=lon_mean, y=lat_mean, weight=captura_ton,
            fill=after_stat(ndensity), alpha=after_stat(ndensity)),
        geom="raster", contour=FALSE, n=150, h=c(0.7, 0.7)
      ) +
      scale_fill_viridis_c(option="plasma", limits=c(0,1), guide="none") +
      scale_alpha_continuous(range=c(0, 0.85), guide="none")
  }
  if (nrow(d_c) > 0) {
    gg <- gg +
      geom_point(data=d_c,
                 aes(x=lon, y=lat, size=captura_ton),
                 colour="white", fill="steelblue", shape=21, alpha=0.75,
                 stroke=0.4) +
      scale_size_area(max_size=12, name="Unmatched\ncatch (ton)",
                      labels=scales::comma)
  }
  gg +
    coord_sf(xlim=c(-75.5,-71.5), ylim=c(-44.5,-40.5), expand=FALSE) +
    labs(title=sp, x=NULL, y=NULL) +
    map_theme(8) +
    theme(plot.title=element_text(size=9, face="bold", hjust=0.5))
})

p_c_grid <- wrap_plots(p_c_list, ncol=2) +
  plot_annotation(
    title   = "Top 4 species: VMS-matched fishing locations (density) + unmatched caleta landings (bubbles)",
    caption = "Density: catch-weighted KDE for VMS-tracked vessels (0.1–3.0 kt).\nBubbles: unmatched vessels aggregated at caleta of disembarkation.",
    theme=theme(
      plot.background = element_rect(fill="white", colour=NA),
      plot.title      = element_text(size=9),
      plot.caption    = element_text(size=7)
    )
  )

out_c <- file.path(out_dir, "species_map_combined_loslagos.png")
ggsave(out_c, p_c_grid, width=14, height=12, dpi=150)
cat("Saved:", out_c, "\n")

## ── 11. Monthly time series by species ────────────────────────────────────
cat("[11] Monthly catch time series by species...\n")

ts_sp <- fish_all[Especie %in% top_sp,
                  .(captura_ton = sum(`Captura (Ton)`, na.rm=TRUE)),
                  by=.(Especie, yr=anioLlegada, mo=mesLlegada)]
ts_sp[, date := as.Date(paste(yr, mo, 1, sep="-"))]

## Palette: one colour per species (colorblind-friendly)
sp_pal <- setNames(
  RColorBrewer::brewer.pal(min(length(top_sp), 8), "Set2"),
  top_sp
)

p_ts <- ggplot(ts_sp, aes(x=date, y=captura_ton, colour=Especie)) +
  geom_line(linewidth=0.65) +
  scale_colour_manual(values=sp_pal) +
  scale_x_date(date_breaks="1 year", date_labels="%Y") +
  scale_y_continuous(labels=scales::comma) +
  facet_wrap(~Especie, scales="free_y", ncol=2) +
  labs(x=NULL, y="Monthly catch (ton)",
       caption="All vessels, Los Lagos region. Source: SERNAPESCA FISHERMEN_DATA.") +
  theme_minimal(base_size=9) +
  theme(
    plot.background  = element_rect(fill="white", colour=NA),
    panel.background = element_rect(fill="white", colour=NA),
    strip.background = element_rect(fill="grey93", colour=NA),
    legend.position  = "none",
    axis.text.x      = element_text(angle=45, hjust=1, size=7)
  )

out_ts <- file.path(out_dir, "species_timeseries_loslagos.png")
ggsave(out_ts, p_ts, width=10, height=8, dpi=150)
cat("Saved:", out_ts, "\n")

## ── 12. Summary report ────────────────────────────────────────────────────
cat("\n========== SUMMARY ==========\n")
cat(sprintf("Fishermen data (Los Lagos, all years): %d rows\n", nrow(fish_all)))
cat(sprintf("Years covered: %s\n", paste(sort(unique(fish_all$anioLlegada)), collapse=", ")))
cat(sprintf("Unique vessels in fishermen data: %d\n", length(fish_unique)))
cat(sprintf("Vessels matched to VMS: %d (%.0f%%)\n",
            length(matched), 100*length(matched)/length(fish_unique)))
cat(sprintf("VMS fishing pings for matched vessels: %d\n", nrow(vms_data)))
cat("\nTop species total catch (Los Lagos, all vessels):\n")
print(fish_all[, .(total_ton = sum(`Captura (Ton)`, na.rm=TRUE)), by=Especie
               ][order(-total_ton)][1:10])
cat("\nOutputs:\n")
cat(" ", out_kde, "\n")
cat(" ", out_yr,  "\n")
cat(" ", out_c,   "\n")
cat(" ", out_ts,  "\n")
cat(" ", fish_csv, "\n")
