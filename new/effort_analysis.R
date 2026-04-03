## effort_analysis.R
## Fishing effort from VMS data using speed-threshold classification.
## Method: 0.1 <= velocidad <= 3.0 knots = fishing (Behivoke 2021; Frontiers 2021)
## Effort metrics: fishing pings, fishing hours (~15-min intervals), fishing days.
## All heavy aggregation in DuckDB — never fread the 65M-row file.

library(DBI); library(duckdb); library(data.table)
library(ggplot2); library(patchwork); library(sf)
library(maptiles); library(tidyterra); library(geodata)

base_dir <- sub("/new/?$", "", normalizePath(getwd(), winslash="/"))
csv_path <- normalizePath(
  file.path(base_dir, "data_processed/los_lagos_filtered.csv"),
  winslash="/", mustWork=TRUE)
out_dir <- normalizePath(file.path(base_dir, "new/outputs"), winslash="/")
dir.create(out_dir, showWarnings=FALSE)

con <- dbConnect(duckdb(), ":memory:")
on.exit(dbDisconnect(con, shutdown=TRUE), add=TRUE)
dbExecute(con, "SET threads=4; SET memory_limit='6GB';")

rc <- sprintf(
  "read_csv('%s', all_varchar=true, header=true, ignore_errors=true)",
  gsub("'","''", csv_path))

## Year from source_file (most reliable)
yr_sql <- "
  CASE
    WHEN source_file LIKE 'report-%'    THEN TRY_CAST(SUBSTR(source_file,8,4)  AS INTEGER)
    WHEN source_file LIKE 'report_31_%' THEN TRY_CAST(SUBSTR(source_file,11,4) AS INTEGER)
    WHEN SUBSTR(TRIM(fecha),5,1)='-'    THEN TRY_CAST(SUBSTR(TRIM(fecha),1,4)  AS INTEGER)
    WHEN SUBSTR(TRIM(fecha),3,1) IN ('/','-')
      THEN TRY_CAST(SUBSTR(TRIM(fecha),7,4) AS INTEGER)
    ELSE NULL
  END"

mo_sql <- "
  CASE
    WHEN source_file LIKE 'report-%'    THEN TRY_CAST(SUBSTR(source_file,13,2) AS INTEGER)
    WHEN source_file LIKE 'report_31_%' THEN TRY_CAST(SUBSTR(source_file,15,2) AS INTEGER)
    WHEN SUBSTR(TRIM(fecha),5,1)='-'    THEN TRY_CAST(SUBSTR(TRIM(fecha),6,2)  AS INTEGER)
    ELSE TRY_CAST(SUBSTR(TRIM(fecha),4,2) AS INTEGER)
  END"

## Speed classification thresholds (knots)
SPD_MIN <- 0.1   # below = moored/anchored
SPD_MAX <- 3.0   # above = steaming

cat("=== FISHING EFFORT — Los Lagos VMS ===\n")
cat(sprintf("Classification: %.1f ≤ speed ≤ %.1f knots = fishing\n\n", SPD_MIN, SPD_MAX))

## ── 1. Overall effort by year × category ──────────────────────────────────────
cat("[1] Effort by year...\n")
effort_yr <- as.data.table(dbGetQuery(con, sprintf("
  SELECT
    (%s)                            AS yr,
    category,
    COUNT(*)                        AS total_pings,
    SUM(CASE WHEN TRY_CAST(velocidad AS DOUBLE) BETWEEN %.2f AND %.2f
             THEN 1 ELSE 0 END)    AS fishing_pings,
    ROUND(SUM(CASE WHEN TRY_CAST(velocidad AS DOUBLE) BETWEEN %.2f AND %.2f
             THEN 1 ELSE 0 END)*0.25, 1) AS fishing_hours,
    COUNT(DISTINCT movil)           AS vessels
  FROM %s
  WHERE (%s) BETWEEN 2019 AND 2026
  GROUP BY yr, category ORDER BY yr, category",
  yr_sql, SPD_MIN, SPD_MAX, SPD_MIN, SPD_MAX, rc, yr_sql)))
effort_yr[, fishing_pct := round(fishing_pings/total_pings*100,1)]
print(effort_yr)

## ── 2. Effort per vessel per year ─────────────────────────────────────────────
cat("\n[2] Per-vessel effort by year...\n")
vessel_effort <- as.data.table(dbGetQuery(con, sprintf("
  SELECT
    (%s)                          AS yr,
    movil,
    COUNT(*)                      AS total_pings,
    SUM(CASE WHEN TRY_CAST(velocidad AS DOUBLE) BETWEEN %.2f AND %.2f
             THEN 1 ELSE 0 END)  AS fishing_pings,
    ROUND(SUM(CASE WHEN TRY_CAST(velocidad AS DOUBLE) BETWEEN %.2f AND %.2f
             THEN 1 ELSE 0 END)*0.25,1) AS fishing_hours
  FROM %s
  WHERE (%s) BETWEEN 2019 AND 2026
    AND movil IS NOT NULL AND TRIM(movil)<>''
  GROUP BY yr, movil
  HAVING fishing_pings > 0
  ORDER BY yr, fishing_pings DESC",
  yr_sql, SPD_MIN, SPD_MAX, SPD_MIN, SPD_MAX, rc, yr_sql)))

## Summary stats per year
cat("\nPer-vessel effort summary:\n")
summary_v <- vessel_effort[, .(
  n_vessels    = .N,
  med_fish_h   = round(median(fishing_hours),1),
  mean_fish_h  = round(mean(fishing_hours),1),
  max_fish_h   = max(fishing_hours),
  tot_fish_h   = round(sum(fishing_hours)/1000,1)
), by=yr][order(yr)]
print(summary_v)

## ── 3. Spatial effort map — fishing pings per 0.05° cell ──────────────────────
cat("\n[3] Building spatial effort grid...\n")
cell_res <- 0.05

effort_grid <- as.data.table(dbGetQuery(con, sprintf("
  SELECT
    ROUND(TRY_CAST(latitud  AS DOUBLE)/%.2f)*%.2f AS lat_g,
    ROUND(TRY_CAST(longitud AS DOUBLE)/%.2f)*%.2f AS lon_g,
    (%s)                                           AS yr,
    COUNT(*)                                       AS total_pings,
    SUM(CASE WHEN TRY_CAST(velocidad AS DOUBLE) BETWEEN %.2f AND %.2f
             THEN 1 ELSE 0 END)                    AS fishing_pings,
    ROUND(SUM(CASE WHEN TRY_CAST(velocidad AS DOUBLE) BETWEEN %.2f AND %.2f
             THEN 1 ELSE 0 END)*0.25,2)            AS fishing_hours
  FROM %s
  WHERE (%s) BETWEEN 2019 AND 2026
    AND TRY_CAST(latitud  AS DOUBLE) BETWEEN -44.5 AND -40.5
    AND TRY_CAST(longitud AS DOUBLE) BETWEEN -75.5 AND -71.5
  GROUP BY lat_g, lon_g, yr
  HAVING fishing_pings > 0
  ORDER BY yr",
  cell_res,cell_res,cell_res,cell_res,
  yr_sql, SPD_MIN, SPD_MAX, SPD_MIN, SPD_MAX,
  rc, yr_sql)))

effort_grid[, log_effort := log10(fishing_hours + 1)]
years_p <- sort(unique(effort_grid$yr))
cat(sprintf("  Years: %s  |  Fishing grid cells: %s\n",
            paste(years_p,collapse=", "), format(nrow(effort_grid),big.mark=",")))

## ── 4. Map layers ─────────────────────────────────────────────────────────────
cat("[4] Loading map layers...\n")
bbox_sf  <- st_as_sfc(st_bbox(c(xmin=-75.5,ymin=-44.5,xmax=-71.5,ymax=-40.5),crs=4326))
cat("  Fetching satellite tiles...\n")
sat      <- get_tiles(bbox_sf, provider="Esri.WorldImagery", zoom=7, crop=TRUE)
chile_sf <- st_as_sf(geodata::gadm("CHL",level=1,path=tempdir()))
ll_sf    <- st_transform(chile_sf[chile_sf$NAME_1=="Los Lagos",],4326)

## ── 5. Effort heatmap ─────────────────────────────────────────────────────────
cat("[5] Building effort map...\n")
p_eff <- ggplot() +
  geom_spatraster_rgb(data=sat, alpha=1) +
  geom_tile(data=effort_grid,
            aes(lon_g, lat_g, fill=log_effort),
            width=cell_res, height=cell_res, alpha=0.80) +
  scale_fill_viridis_c(
    option="inferno", name="Fishing\nhours",
    limits=c(0, max(effort_grid$log_effort)),
    breaks=c(0,1,2,3,floor(max(effort_grid$log_effort))),
    labels=function(x) formatC(round(10^x-1),format="fg",big.mark=",")) +
  geom_sf(data=ll_sf, fill=NA, colour="white", linewidth=0.5, inherit.aes=FALSE) +
  facet_wrap(~yr, ncol=4) +
  coord_sf(xlim=c(-75.5,-71.5), ylim=c(-44.5,-40.5), expand=FALSE) +
  labs(
    title    = "Artisanal Fishing Effort — Los Lagos 2019–2026",
    subtitle = sprintf("Fishing hours per 0.05° cell  |  Speed threshold: %.1f–%.1f knots  |  SERNAPESCA", SPD_MIN, SPD_MAX),
    x="Longitude", y="Latitude") +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",hjust=0.5,colour="grey15"),
        plot.subtitle=element_text(hjust=0.5,colour="grey45",size=9),
        plot.background=element_rect(fill="white",colour=NA),
        panel.background=element_rect(fill="white",colour=NA),
        strip.text=element_text(face="bold",colour="grey15",size=11),
        strip.background=element_rect(fill="grey93",colour=NA),
        axis.text=element_text(colour="grey45",size=7),
        axis.title=element_text(colour="grey30"),
        legend.text=element_text(colour="grey20"),
        legend.title=element_text(colour="grey20",face="bold"),
        legend.background=element_rect(fill="white",colour=NA),
        panel.grid.major=element_line(colour="grey30",linewidth=0.2))

out_eff <- paste0(out_dir,"/effort_map_loslagos.png")
ggsave(out_eff, p_eff,
       width=18, height=5L*ceiling(length(years_p)/4L)+2L, dpi=180, bg="white")
cat(sprintf("Saved: %s\n", out_eff))

## ── 6. Per-vessel effort bar chart ────────────────────────────────────────────
cat("[6] Building vessel effort plots...\n")
yr_summary <- merge(
  effort_yr[, .(total_pings=sum(total_pings),
                fishing_pings=sum(fishing_pings),
                fishing_hours=sum(fishing_hours),
                vessels=sum(vessels)), by=yr],
  summary_v, by="yr")
yr_summary[, yr_chr := as.character(yr)]

yc <- c("2019"="#2dc653","2020"="#55a630","2021"="#38b000",
        "2022"="#b5179e","2023"="#f72585",
        "2024"="#ff9e00","2025"="#4cc9f0","2026"="#4361ee")
yc <- yc[names(yc) %in% yr_summary$yr_chr]

bt <- theme_minimal(base_size=11) +
  theme(plot.background=element_rect(fill="white",colour=NA),
        panel.background=element_rect(fill="#f7f7f7",colour=NA),
        panel.grid.major=element_line(colour="grey88",linewidth=0.3),
        panel.grid.minor=element_blank(),
        axis.text=element_text(colour="grey35"),
        legend.position="none")

pA <- ggplot(yr_summary, aes(yr_chr, tot_fish_h, fill=yr_chr)) +
  geom_col(width=0.65) +
  geom_text(aes(label=paste0(tot_fish_h,"k")), vjust=-0.4, size=3, colour="grey25") +
  scale_fill_manual(values=yc) +
  scale_y_continuous(expand=c(0,0), limits=c(0,max(yr_summary$tot_fish_h)*1.2)) +
  labs(title="A · Total fishing hours (×1000)", x=NULL, y="Fishing hours (k)") + bt

pB <- ggplot(yr_summary, aes(yr_chr, vessels, fill=yr_chr)) +
  geom_col(width=0.65) +
  geom_text(aes(label=vessels), vjust=-0.4, size=3, colour="grey25") +
  scale_fill_manual(values=yc) +
  scale_y_continuous(expand=c(0,0), limits=c(0,max(yr_summary$vessels)*1.2)) +
  labs(title="B · Active vessels", x=NULL, y="Vessels") + bt

pC <- ggplot(yr_summary, aes(yr_chr, mean_fish_h, fill=yr_chr)) +
  geom_col(width=0.65) +
  geom_text(aes(label=round(mean_fish_h)), vjust=-0.4, size=3, colour="grey25") +
  scale_fill_manual(values=yc) +
  scale_y_continuous(expand=c(0,0), limits=c(0,max(yr_summary$mean_fish_h)*1.2)) +
  labs(title="C · Mean fishing hours / vessel", x=NULL, y="Hours/vessel") + bt

pD <- ggplot(yr_summary, aes(yr_chr, fishing_pings/total_pings*100, fill=yr_chr)) +
  geom_col(width=0.65) +
  geom_text(aes(label=paste0(round(fishing_pings/total_pings*100,1),"%")),
            vjust=-0.4, size=3, colour="grey25") +
  scale_fill_manual(values=yc) +
  scale_y_continuous(expand=c(0,0), limits=c(0,100)) +
  labs(title="D · % pings classified as fishing", x=NULL, y="%") + bt

fig_bars <- (pA|pB)/(pC|pD) +
  plot_annotation(
    title    = "Fishing Effort Summary — Los Lagos Artisanal Fleet 2019–2026",
    subtitle = sprintf("Speed threshold: %.1f–%.1f knots = fishing  |  SERNAPESCA VMS", SPD_MIN, SPD_MAX),
    theme=theme(plot.title=element_text(face="bold",hjust=0.5,size=14),
                plot.subtitle=element_text(hjust=0.5,colour="grey50",size=10),
                plot.background=element_rect(fill="white",colour=NA)))

out_bars <- paste0(out_dir,"/effort_bars_loslagos.png")
ggsave(out_bars, fig_bars, width=13, height=9, dpi=180, bg="white")
cat(sprintf("Saved: %s\n", out_bars))

## ── 7. Text report ────────────────────────────────────────────────────────────
sink(paste0(out_dir,"/effort_report_loslagos.txt"))
cat("================================================================\n")
cat(" SERNAPESCA VMS — Fishing Effort — Los Lagos Region\n")
cat(sprintf(" Classification: %.1f–%.1f knots = fishing (Behivoke 2021)\n", SPD_MIN, SPD_MAX))
cat(sprintf(" Generated: %s\n", Sys.time()))
cat("================================================================\n\n")
cat("--- Effort by year and source ---\n"); print(effort_yr)
cat("\n--- Per-vessel effort summary by year ---\n"); print(summary_v)
cat("\n--- Top 20 most active fishing vessels (all years) ---\n")
top20 <- vessel_effort[order(-fishing_hours)][1:20]
print(top20)
sink()

cat("\n========== ALL DONE ==========\n")
cat("Outputs:\n")
cat(sprintf("  %s\n", out_eff))
cat(sprintf("  %s\n", out_bars))
cat(sprintf("  %s/effort_report_loslagos.txt\n", out_dir))
