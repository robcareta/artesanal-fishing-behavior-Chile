## analyze_artesanal_only.R
## Stats + heatmap + time series + effort from los_lagos_artesanal_only.csv
## (admin_2019 restricted to ART+LB vessels — consistent artisanal fleet 2019-2026)

library(DBI); library(duckdb); library(data.table)
library(ggplot2); library(patchwork); library(sf)
library(maptiles); library(tidyterra); library(geodata)

base_dir <- sub("/new/?$", "", normalizePath(getwd(), winslash="/"))
csv_path <- normalizePath(
  file.path(base_dir, "data_processed/los_lagos_artesanal_only.csv"),
  winslash="/", mustWork=TRUE)
out_dir <- normalizePath(file.path(base_dir, "new/outputs"), winslash="/")
dir.create(out_dir, showWarnings=FALSE)
cat(sprintf("Input: %s\nOutputs: %s\n\n", csv_path, out_dir))

con <- dbConnect(duckdb(), ":memory:")
on.exit(dbDisconnect(con, shutdown=TRUE), add=TRUE)
dbExecute(con, "SET threads=4; SET memory_limit='6GB';")

rc <- sprintf("read_csv('%s', all_varchar=true, header=true, ignore_errors=true)",
              gsub("'","''", csv_path))

year_expr <- "
  CASE
    WHEN source_file LIKE 'report-%'
      THEN TRY_CAST(SUBSTR(source_file, 8, 4) AS INTEGER)
    WHEN source_file LIKE 'report_31_%'
      THEN TRY_CAST(SUBSTR(source_file, 11, 4) AS INTEGER)
    WHEN SUBSTR(TRIM(fecha),5,1) = '-'
      THEN TRY_CAST(SUBSTR(TRIM(fecha),1,4) AS INTEGER)
    WHEN LENGTH(TRIM(fecha)) >= 10
      AND SUBSTR(TRIM(fecha),3,1) IN ('/','-')
      THEN TRY_CAST(SUBSTR(TRIM(fecha),7,4) AS INTEGER)
    ELSE NULL
  END"

month_expr <- "
  CASE
    WHEN source_file LIKE 'report-%'
      THEN TRY_CAST(SUBSTR(source_file, 13, 2) AS INTEGER)
    WHEN source_file LIKE 'report_31_%'
      THEN TRY_CAST(SUBSTR(source_file, 15, 2) AS INTEGER)
    WHEN SUBSTR(TRIM(fecha),5,1)='-'
      THEN TRY_CAST(SUBSTR(TRIM(fecha),6,2) AS INTEGER)
    ELSE TRY_CAST(SUBSTR(TRIM(fecha),4,2) AS INTEGER)
  END"

## ── 1. Stats by year ──────────────────────────────────────────────────────────
cat("[1] Year × category:\n")
yr_cat <- as.data.table(dbGetQuery(con, sprintf("
  SELECT (%s) AS yr, category,
    COUNT(*) AS pings, COUNT(DISTINCT movil) AS vessels
  FROM %s
  WHERE (%s) BETWEEN 2019 AND 2026
  GROUP BY yr, category ORDER BY yr, category", year_expr, rc, year_expr)))
print(yr_cat)

cat("\n[2] Year totals + effort (0.1–3 kt = fishing):\n")
yr_tot <- as.data.table(dbGetQuery(con, sprintf("
  SELECT (%s) AS yr,
    COUNT(*) AS total_pings,
    COUNT(DISTINCT movil) AS unique_vessels,
    SUM(CASE WHEN TRY_CAST(velocidad AS DOUBLE) BETWEEN 0.1 AND 3.0 THEN 1 ELSE 0 END) AS fishing_pings,
    ROUND(SUM(CASE WHEN TRY_CAST(velocidad AS DOUBLE) BETWEEN 0.1 AND 3.0 THEN 1 ELSE 0 END)*15.0/60.0,1)
      AS fishing_hours,
    ROUND(AVG(TRY_CAST(velocidad AS DOUBLE)),2) AS mean_speed_kt
  FROM %s
  WHERE (%s) BETWEEN 2019 AND 2026
  GROUP BY yr ORDER BY yr", year_expr, rc, year_expr)))
yr_tot[, fishing_pct := round(fishing_pings/total_pings*100,1)]
print(yr_tot)

## ── 2. Monthly ────────────────────────────────────────────────────────────────
monthly <- as.data.table(dbGetQuery(con, sprintf("
  SELECT (%s) AS yr, (%s) AS mo,
    COUNT(*) AS pings, COUNT(DISTINCT movil) AS vessels
  FROM %s
  WHERE (%s) BETWEEN 2019 AND 2026
  GROUP BY yr, mo HAVING mo BETWEEN 1 AND 12
  ORDER BY yr, mo", year_expr, month_expr, rc, year_expr)))

## ── 3. Heatmap grid ───────────────────────────────────────────────────────────
cat("\n[3] Building heatmap grid...\n")
cell_res <- 0.05
grid <- as.data.table(dbGetQuery(con, sprintf("
  SELECT
    ROUND(TRY_CAST(latitud  AS DOUBLE)/%.2f)*%.2f AS lat_g,
    ROUND(TRY_CAST(longitud AS DOUBLE)/%.2f)*%.2f AS lon_g,
    (%s) AS yr, COUNT(*) AS n
  FROM %s
  WHERE (%s) BETWEEN 2019 AND 2026
    AND TRY_CAST(latitud  AS DOUBLE) BETWEEN -44.5 AND -40.5
    AND TRY_CAST(longitud AS DOUBLE) BETWEEN -75.5 AND -71.5
  GROUP BY lat_g, lon_g, yr ORDER BY yr",
  cell_res,cell_res,cell_res,cell_res, year_expr, rc, year_expr)))
grid[, log_n := log10(n)]
years_p <- sort(unique(grid$yr))
cat(sprintf("   Years: %s  |  Grid cells: %s\n",
            paste(years_p,collapse=", "), format(nrow(grid),big.mark=",")))

## ── 4. Effort grid ────────────────────────────────────────────────────────────
cat("[4] Building effort grid...\n")
effort_grid <- as.data.table(dbGetQuery(con, sprintf("
  SELECT
    ROUND(TRY_CAST(latitud  AS DOUBLE)/%.2f)*%.2f AS lat_g,
    ROUND(TRY_CAST(longitud AS DOUBLE)/%.2f)*%.2f AS lon_g,
    (%s) AS yr,
    SUM(CASE WHEN TRY_CAST(velocidad AS DOUBLE) BETWEEN 0.1 AND 3.0 THEN 1 ELSE 0 END)*15.0/60.0
      AS fish_hours
  FROM %s
  WHERE (%s) BETWEEN 2019 AND 2026
    AND TRY_CAST(latitud  AS DOUBLE) BETWEEN -44.5 AND -40.5
    AND TRY_CAST(longitud AS DOUBLE) BETWEEN -75.5 AND -71.5
  GROUP BY lat_g, lon_g, yr
  HAVING fish_hours > 0
  ORDER BY yr",
  cell_res,cell_res,cell_res,cell_res, year_expr, rc, year_expr)))
effort_grid[, log_h := log10(fish_hours)]

## ── 5. Map layers ─────────────────────────────────────────────────────────────
cat("[5] Loading map layers...\n")
bbox_sf  <- st_as_sfc(st_bbox(c(xmin=-75.5,ymin=-44.5,xmax=-71.5,ymax=-40.5),crs=4326))
sat      <- get_tiles(bbox_sf, provider="Esri.WorldImagery", zoom=7, crop=TRUE)
chile_sf <- st_as_sf(geodata::gadm("CHL",level=1,path=tempdir()))
ll_sf    <- st_transform(chile_sf[chile_sf$NAME_1=="Los Lagos",],4326)

map_theme <- theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",hjust=0.5,colour="grey15"),
        plot.subtitle=element_text(hjust=0.5,colour="grey45"),
        plot.background=element_rect(fill="white",colour=NA),
        panel.background=element_rect(fill="white",colour=NA),
        strip.text=element_text(face="bold",colour="grey15",size=11),
        strip.background=element_rect(fill="grey93",colour=NA),
        axis.text=element_text(colour="grey45",size=7),
        axis.title=element_text(colour="grey30"),
        legend.text=element_text(colour="grey20"),
        legend.title=element_text(colour="grey20"),
        legend.background=element_rect(fill="white",colour=NA),
        panel.grid.major=element_line(colour="grey30",linewidth=0.2))

## ── 6. Activity heatmap ───────────────────────────────────────────────────────
cat("[6] Activity heatmap...\n")
p_h <- ggplot() +
  geom_spatraster_rgb(data=sat, alpha=1) +
  geom_tile(data=grid, aes(lon_g,lat_g,fill=log_n),
            width=cell_res, height=cell_res, alpha=0.75) +
  scale_fill_viridis_c(option="inferno", name="Pings",
    limits=c(0,max(grid$log_n)),
    breaks=c(0,1,2,3,floor(max(grid$log_n))),
    labels=function(x) formatC(round(10^x),format="fg",big.mark=",")) +
  geom_sf(data=ll_sf, fill=NA, colour="white", linewidth=0.5, inherit.aes=FALSE) +
  facet_wrap(~yr, ncol=4) +
  coord_sf(xlim=c(-75.5,-71.5), ylim=c(-44.5,-40.5), expand=FALSE) +
  labs(title="Artisanal Fleet Activity — Los Lagos 2019–2026",
       subtitle="Pings per 0.05° cell  |  SERNAPESCA artisanal vessels only (ART + LB)  |  9.8 M records",
       x="Longitude", y="Latitude") + map_theme

out_hm <- file.path(out_dir,"heatmap_artesanal_only.png")
ggsave(out_hm, p_h,
       width=18, height=5L*ceiling(length(years_p)/4L)+2L, dpi=180, bg="white")
cat(sprintf("Saved: %s\n", out_hm))

## ── 7. Effort heatmap ─────────────────────────────────────────────────────────
cat("[7] Effort heatmap...\n")
p_e <- ggplot() +
  geom_spatraster_rgb(data=sat, alpha=1) +
  geom_tile(data=effort_grid, aes(lon_g,lat_g,fill=log_h),
            width=cell_res, height=cell_res, alpha=0.75) +
  scale_fill_viridis_c(option="plasma", name="Fishing\nhours",
    breaks=c(0,1,2,3),
    labels=function(x) formatC(round(10^x),format="fg",big.mark=",")) +
  geom_sf(data=ll_sf, fill=NA, colour="white", linewidth=0.5, inherit.aes=FALSE) +
  facet_wrap(~yr, ncol=4) +
  coord_sf(xlim=c(-75.5,-71.5), ylim=c(-44.5,-40.5), expand=FALSE) +
  labs(title="Artisanal Fleet Fishing Effort — Los Lagos 2019–2026",
       subtitle="Fishing hours per 0.05° cell (0.1–3 kt)  |  SERNAPESCA artisanal vessels only",
       x="Longitude", y="Latitude") + map_theme

out_ef <- file.path(out_dir,"effort_map_artesanal_only.png")
ggsave(out_ef, p_e,
       width=18, height=5L*ceiling(length(years_p)/4L)+2L, dpi=180, bg="white")
cat(sprintf("Saved: %s\n", out_ef))

## ── 8. Time series ────────────────────────────────────────────────────────────
cat("[8] Time series...\n")
monthly <- monthly[!is.na(yr) & !is.na(mo)]
monthly[, date   := as.Date(sprintf("%d-%02d-01", yr, mo))]
monthly[, yr_chr := as.character(yr)]
setorder(monthly, date)

yc <- c("2019"="#2dc653","2020"="#55a630","2021"="#38b000",
        "2022"="#b5179e","2023"="#f72585",
        "2024"="#ff9e00","2025"="#4cc9f0","2026"="#4361ee")
yc <- yc[names(yc) %in% monthly$yr_chr]
x_sc <- scale_x_date(date_breaks="3 months",date_labels="%b\n%Y",expand=c(0.01,0))
ts_t <- theme_minimal(base_size=12) +
  theme(plot.background=element_rect(fill="white",colour=NA),
        panel.background=element_rect(fill="#f7f7f7",colour=NA),
        panel.grid.major=element_line(colour="grey88",linewidth=0.35),
        panel.grid.minor=element_blank(),
        axis.text=element_text(colour="grey40"),
        axis.text.x=element_text(size=8))

tA <- ggplot(monthly,aes(date)) +
  geom_ribbon(aes(ymin=0,ymax=pings/1e3,fill=yr_chr),alpha=0.18) +
  geom_line(aes(y=pings/1e3,colour=yr_chr),linewidth=0.9) +
  geom_point(aes(y=pings/1e3,colour=yr_chr),size=1.8) +
  scale_colour_manual(values=yc) + scale_fill_manual(values=yc) +
  scale_y_continuous(labels=function(x) paste0(x,"k"),expand=c(0,0),limits=c(0,NA)) +
  x_sc + labs(y="Pings (×1000)",x=NULL,
              title="A · Monthly pings — artisanal fleet only (ART + LB)") +
  ts_t + theme(legend.position="none",
               plot.title=element_text(face="bold",size=11,colour="grey25"))

tB <- ggplot(monthly,aes(date)) +
  geom_ribbon(aes(ymin=0,ymax=vessels,fill=yr_chr),alpha=0.18) +
  geom_line(aes(y=vessels,colour=yr_chr),linewidth=0.9) +
  geom_point(aes(y=vessels,colour=yr_chr),size=1.8) +
  scale_colour_manual(values=yc,name="Year") + scale_fill_manual(values=yc,name="Year") +
  scale_y_continuous(expand=c(0,0),limits=c(0,NA)) +
  x_sc + labs(y="Unique vessels",x=NULL,title="B · Monthly unique vessels") +
  ts_t + theme(legend.position="right",
               legend.title=element_text(face="bold",size=10),
               plot.title=element_text(face="bold",size=11,colour="grey25"))

fig_ts <- (tA/tB) +
  plot_annotation(
    title="Artisanal Fleet Activity — Los Lagos 2019–2026",
    subtitle="SERNAPESCA · artisanal vessels only (ART + LB suffix)  |  9.8 M records",
    caption="Source: SERNAPESCA. admin_2019 restricted to (ART)+(LB) vessels; flota_artesanal all years.",
    theme=theme(plot.title=element_text(face="bold",hjust=0.5,size=14),
                plot.subtitle=element_text(hjust=0.5,colour="grey50",size=10),
                plot.background=element_rect(fill="white",colour=NA)))

out_ts <- file.path(out_dir,"timeseries_artesanal_only.png")
ggsave(out_ts, fig_ts, width=13, height=9, dpi=180, bg="white")
cat(sprintf("Saved: %s\n", out_ts))

## ── 9. Text report ────────────────────────────────────────────────────────────
out_rep <- file.path(out_dir,"report_artesanal_only.txt")
sink(out_rep)
cat("================================================================\n")
cat(" SERNAPESCA — Artisanal Fleet ONLY — Los Lagos 2019-2026\n")
cat(" Dataset: los_lagos_artesanal_only.csv (9.8 M rows)\n")
cat(" Filter: admin_2019 restricted to (ART)+(LB) vessels\n")
cat(sprintf(" Generated: %s\n", Sys.time()))
cat("================================================================\n\n")
cat("--- Year × Source ---\n"); print(yr_cat)
cat("\n--- Year Totals + Effort ---\n"); print(yr_tot)
cat("\n--- Monthly distribution ---\n"); print(monthly[order(yr,mo),.(yr,mo,pings,vessels)])
sink()
cat(sprintf("Saved: %s\n", out_rep))
cat("\n========== ALL DONE ==========\n")
cat("Outputs:\n")
cat(sprintf("  %s\n", basename(out_hm)))
cat(sprintf("  %s\n", basename(out_ef)))
cat(sprintf("  %s\n", basename(out_ts)))
cat(sprintf("  %s\n", basename(out_rep)))
