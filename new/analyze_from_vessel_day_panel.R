## analyze_from_vessel_day_panel.R
## Refreshes heatmap_artesanal_only.png / effort_map_artesanal_only.png /
## timeseries_artesanal_only.png for the website WITHOUT re-downloading raw
## SERNAPESCA pings. Uses data_processed/vessel_day_effort.csv, the same
## vessel-day panel that feeds the discrete-choice location model
## (new/fishing_location_choice.R). This panel is coarser than raw pings
## (one row per vessel-day rather than per ~15-min ping) so maps are less
## granular than the original April run, but require no fresh download.
##
## NOTE: 2021 has ~0 rows in this panel (gap in the combined-pipeline raw
## inputs) and 2019-2020 are very thin (~250 rows total) -- this is a known
## limitation flagged to the user; a full refresh with freshly downloaded
## data is deferred to a later session.

library(data.table); library(ggplot2); library(patchwork); library(sf)
library(maptiles); library(tidyterra); library(geodata)

base_dir <- sub("/new/?$", "", normalizePath(getwd(), winslash="/"))
csv_path <- normalizePath(
  file.path(base_dir, "data_processed/vessel_day_effort.csv"),
  winslash="/", mustWork=TRUE)
out_dir <- normalizePath(file.path(base_dir, "new/outputs"), winslash="/")
dir.create(out_dir, showWarnings=FALSE)
cat(sprintf("Input: %s\nOutputs: %s\n\n", csv_path, out_dir))

dt <- fread(csv_path)
cat(sprintf("Rows: %s | Years: %s\n", format(nrow(dt),big.mark=","),
            paste(sort(unique(dt$year)), collapse=", ")))

LAT_MIN <- -44.5; LAT_MAX <- -40.5
LON_MIN <- -75.5; LON_MAX <- -71.5
cell_res <- 0.05

## ── 1. Activity heatmap grid (daily mean position, all vessel-days) ───────────
cat("[1] Building heatmap grid (lat_mean/lon_mean)...\n")
act <- dt[!is.na(lat_mean) & !is.na(lon_mean) &
          lat_mean >= LAT_MIN & lat_mean <= LAT_MAX &
          lon_mean >= LON_MIN & lon_mean <= LON_MAX]
act[, lat_g := round(lat_mean/cell_res)*cell_res]
act[, lon_g := round(lon_mean/cell_res)*cell_res]
grid <- act[, .(n = sum(n_pings)), by=.(lat_g, lon_g, yr=year)]
grid[, log_n := log10(n)]
years_p <- sort(unique(grid$yr))
cat(sprintf("   Years: %s | Grid cells: %s\n",
            paste(years_p,collapse=", "), format(nrow(grid),big.mark=",")))

## ── 2. Effort grid (fishing-only position, hours_fishing) ─────────────────────
cat("[2] Building effort grid (lat_fishing/lon_fishing)...\n")
eff <- dt[!is.na(lat_fishing) & !is.na(lon_fishing) & hours_fishing > 0 &
          lat_fishing >= LAT_MIN & lat_fishing <= LAT_MAX &
          lon_fishing >= LON_MIN & lon_fishing <= LON_MAX]
eff[, lat_g := round(lat_fishing/cell_res)*cell_res]
eff[, lon_g := round(lon_fishing/cell_res)*cell_res]
effort_grid <- eff[, .(fish_hours = sum(hours_fishing)), by=.(lat_g, lon_g, yr=year)]
effort_grid[, log_h := log10(fish_hours)]

## ── 3. Map layers ──────────────────────────────────────────────────────────────
cat("[3] Loading map layers...\n")
bbox_sf  <- st_as_sfc(st_bbox(c(xmin=LON_MIN,ymin=LAT_MIN,xmax=LON_MAX,ymax=LAT_MAX),crs=4326))
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

## ── 4. Activity heatmap ─────────────────────────────────────────────────────────
cat("[4] Activity heatmap...\n")
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
  coord_sf(xlim=c(LON_MIN,LON_MAX), ylim=c(LAT_MIN,LAT_MAX), expand=FALSE) +
  labs(title="Artisanal Fleet Activity — Los Lagos 2019–2026",
       subtitle=sprintf("Pings per 0.05° cell (vessel-day panel)  |  SERNAPESCA artisanal vessels only  |  %s vessel-days",
                         format(nrow(dt),big.mark=",")),
       x="Longitude", y="Latitude") + map_theme

out_hm <- file.path(out_dir,"heatmap_artesanal_only.png")
ggsave(out_hm, p_h,
       width=18, height=5L*ceiling(length(years_p)/4L)+2L, dpi=180, bg="white")
cat(sprintf("Saved: %s\n", out_hm))

## ── 5. Effort heatmap ───────────────────────────────────────────────────────────
cat("[5] Effort heatmap...\n")
p_e <- ggplot() +
  geom_spatraster_rgb(data=sat, alpha=1) +
  geom_tile(data=effort_grid, aes(lon_g,lat_g,fill=log_h),
            width=cell_res, height=cell_res, alpha=0.75) +
  scale_fill_viridis_c(option="plasma", name="Fishing\nhours",
    breaks=c(0,1,2,3),
    labels=function(x) formatC(round(10^x),format="fg",big.mark=",")) +
  geom_sf(data=ll_sf, fill=NA, colour="white", linewidth=0.5, inherit.aes=FALSE) +
  facet_wrap(~yr, ncol=4) +
  coord_sf(xlim=c(LON_MIN,LON_MAX), ylim=c(LAT_MIN,LAT_MAX), expand=FALSE) +
  labs(title="Artisanal Fleet Fishing Effort — Los Lagos 2019–2026",
       subtitle="Fishing hours per 0.05° cell (vessel-day panel)  |  SERNAPESCA artisanal vessels only",
       x="Longitude", y="Latitude") + map_theme

out_ef <- file.path(out_dir,"effort_map_artesanal_only.png")
ggsave(out_ef, p_e,
       width=18, height=5L*ceiling(length(years_p)/4L)+2L, dpi=180, bg="white")
cat(sprintf("Saved: %s\n", out_ef))

## ── 6. Time series ──────────────────────────────────────────────────────────────
cat("[6] Time series...\n")
monthly <- dt[, .(pings = sum(n_pings), vessels = uniqueN(vessel_id)), by=year_month]
monthly[, date := as.Date(paste0(year_month,"-01"))]
monthly[, yr_chr := format(date,"%Y")]
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
              title="A · Monthly pings — artisanal fleet only (vessel-day panel)") +
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
    subtitle=sprintf("SERNAPESCA · artisanal vessels only · vessel-day panel  |  %s vessel-days",
                      format(nrow(dt),big.mark=",")),
    caption="Source: SERNAPESCA. Vessel-day panel (data_processed/vessel_day_effort.csv), same input used for the discrete-choice location model.",
    theme=theme(plot.title=element_text(face="bold",hjust=0.5,size=14),
                plot.subtitle=element_text(hjust=0.5,colour="grey50",size=10),
                plot.background=element_rect(fill="white",colour=NA)))

out_ts <- file.path(out_dir,"timeseries_artesanal_only.png")
ggsave(out_ts, fig_ts, width=13, height=9, dpi=180, bg="white")
cat(sprintf("Saved: %s\n", out_ts))

cat("\n========== ALL DONE ==========\n")
cat(sprintf("  %s\n", basename(out_hm)))
cat(sprintf("  %s\n", basename(out_ef)))
cat(sprintf("  %s\n", basename(out_ts)))
