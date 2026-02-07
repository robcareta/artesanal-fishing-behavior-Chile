# Script webscrapping fisheries data (SERNAPESCA) ------------

rm(list = ls()); gc()

# Packages ------------
pkgs <- c(
  "rvest", "xml2", "httr", "stringr",
  "DBI", "duckdb",
  "data.table",
  "ggplot2", "viridis", "hexbin",
  "sf", "rnaturalearth", "rnaturalearthdata",
  "leaflet", "htmlwidgets",
  "magick"
)
invisible(lapply(pkgs, require, character.only = TRUE))

# Config ------------
url <- "https://www.sernapesca.cl/informacion-utilidad/posiciones-geograficas-del-sistema-de-posicionamiento-satelital/"

dest_folder <- "data_raw"
processed_folder <- "data_processed"
out_folder  <- "outputs"
docs_folder <- "docs"
db_path     <- "sernapesca.duckdb"

dir.create(dest_folder, showWarnings = FALSE, recursive = TRUE)
dir.create(processed_folder, showWarnings = FALSE, recursive = TRUE)
dir.create(out_folder,  showWarnings = FALSE, recursive = TRUE)
dir.create(docs_folder, showWarnings = FALSE, recursive = TRUE)

exclude_years <- c("2025","2026")
tz_use <- "America/Santiago"

# Helper: normalize path for DuckDB on Windows ------------
normalize_for_duckdb <- function(path) normalizePath(path, winslash = "/", mustWork = FALSE)

# Helper: safely drop NA from logical vector ------------
na_false <- function(x) { x[is.na(x)] <- FALSE; x }

# 1) Download CSV reports (ONLY "Flota Artesanal") ------------
page <- read_html(url)

# Find element (button/a) with exact visible text "Flota Artesanal"
nodes <- page |> html_elements("a, button")
node_txt <- str_squish(html_text2(nodes))
idx <- which(str_detect(node_txt, "^Flota\\s+Artesanal$"))

if (length(idx) == 0) {
  stop("No se encontró el header 'Flota Artesanal' en el HTML. Puede que haya cambiado la estructura del sitio.")
}

art_header <- nodes[idx[1]]

# Bootstrap accordions often store target panel in href="#id" or data-target="#id"
art_target <- html_attr(art_header, "href")
if (is.na(art_target) || !str_detect(art_target, "^#")) {
  art_target <- html_attr(art_header, "data-target")
}
if (is.na(art_target) || !str_detect(art_target, "^#")) {
  stop("Encontré 'Flota Artesanal' pero no pude identificar el panel asociado (href/data-target).")
}

panel_id <- str_remove(art_target, "^#")

# Extract only links inside that panel
report_links <- page |>
  html_elements(xpath = sprintf("//*[@id='%s']//a", panel_id)) |>
  html_attr("href")

report_links <- report_links[!is.na(report_links)]
report_links <- url_absolute(report_links, url)

# Keep only CSV links
report_links <- report_links[str_detect(tolower(report_links), "\\.csv($|\\?)")]
report_links <- unique(report_links)

# Safety filter: remove other categories if present in url/name
flag_other <- str_detect(tolower(report_links), "acuicultura|industrial|transport")
flag_other <- na_false(flag_other)
report_links <- report_links[!flag_other]

cat("Links CSV encontrados SOLO en 'Flota Artesanal':", length(report_links), "\n")
print(head(report_links, 10))

if (length(report_links) == 0) {
  stop("No quedaron links CSV tras filtrar. Revisa patrones o estructura del HTML.")
}

# Download incremental (skip existing files)
for (link in report_links) {
  file_name <- basename(str_split(link, "\\?")[[1]][1])
  dest_file <- file.path(dest_folder, file_name)
  
  if (!file.exists(dest_file)) {
    cat("Descargando:", file_name, "\n")
    tryCatch(
      GET(link, write_disk(dest_file, overwrite = TRUE), timeout(120)),
      error = function(e) message("Error descargando: ", link, " -> ", e$message)
    )
  }
}

# 2) Select artisanal CSV files (local) ------------
files <- list.files(dest_folder, pattern = "\\.csv$", full.names = TRUE)

# Keep only artisanal patterns (be strict)
artesanal_files <- files[
  str_detect(tolower(files), "flota_artesanal|sernapesca-admin|artesanal")
]

# Exclude years if needed
if (length(exclude_years) > 0) {
  pat <- paste(exclude_years, collapse = "|")
  artesanal_files <- artesanal_files[!str_detect(artesanal_files, pat)]
}

cat("Archivos seleccionados (artesanal):", length(artesanal_files), "\n")
stopifnot(length(artesanal_files) >= 1)

# 3) DuckDB: build table from all artisanal files ------------
con <- dbConnect(duckdb(), dbdir = db_path, read_only = FALSE)
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

dbExecute(con, "DROP TABLE IF EXISTS artisanal")
errores <- character()

# Create table from first file (DELIM=';')
first_file <- normalize_for_duckdb(artesanal_files[1])

query_create <- sprintf("
  CREATE TABLE artisanal AS
  SELECT *, '%s' AS source_file
  FROM read_csv_auto('%s', DELIM=';', ALL_VARCHAR=TRUE)
", basename(first_file), first_file)

dbExecute(con, query_create)

# Insert remaining files
if (length(artesanal_files) > 1) {
  for (f in artesanal_files[-1]) {
    f2 <- normalize_for_duckdb(f)
    message("Importando: ", basename(f2))
    
    query_ins <- sprintf("
      INSERT INTO artisanal
      SELECT *, '%s' AS source_file
      FROM read_csv_auto('%s', DELIM=';', ALL_VARCHAR=TRUE)
    ", basename(f2), f2)
    
    tryCatch(
      dbExecute(con, query_ins),
      error = function(e) {
        errores <<- c(errores, f)
        warning("Error al importar: ", f, " -> ", e$message)
      }
    )
  }
}

if (length(errores) > 0) {
  message("Archivos que no se pudieron importar:")
  print(errores)
} else {
  message("✅ Todos los archivos se importaron correctamente.")
}

print(dbGetQuery(con, "SELECT COUNT(*) AS n FROM artisanal"))

# 4) Filter Los Lagos (bbox) + export ------------
# NOTE: columns in your data are Spanish: Latitud / Longitud
dbExecute(con, "DROP TABLE IF EXISTS artisanal_loslagos")

dbExecute(con, "
  CREATE TABLE artisanal_loslagos AS
  SELECT *
  FROM artisanal
  WHERE TRY_CAST(Latitud AS DOUBLE) BETWEEN -44.0 AND -39.5
    AND TRY_CAST(Longitud AS DOUBLE) BETWEEN -76.0 AND -72.0
")

print(dbGetQuery(con, "SELECT COUNT(*) AS n FROM artisanal_loslagos"))

loslagos_csv <- file.path(processed_folder, "artisanal_loslagos.csv")
dbExecute(con, sprintf("
  COPY artisanal_loslagos TO '%s' (HEADER, DELIMITER ',')
", normalize_for_duckdb(loslagos_csv)))

# Pull filtered data to R
DT <- as.data.table(dbGetQuery(con, "SELECT * FROM artisanal_loslagos"))

# Coerce core columns (Spanish)
DT[, Latitud  := as.numeric(Latitud)]
DT[, Longitud := as.numeric(Longitud)]
DT[, DateTime := as.POSIXct(Fecha, format = "%d/%m/%Y %H:%M:%S", tz = tz_use)]

DT <- DT[!is.na(Latitud) & !is.na(Longitud) & !is.na(DateTime)]
setorder(DT, `Móvil`, DateTime)

# 5) Heatmap of points (counts) ------------
agg_data <- DT[, .N, by = .(
  lat_bin = round(Latitud, 2),
  lon_bin = round(Longitud, 2)
)]

p_heat <- ggplot(agg_data, aes(x = lon_bin, y = lat_bin, fill = N)) +
  geom_tile() +
  coord_fixed() +
  scale_fill_viridis_c(option = "cividis", trans = "log") +
  theme_minimal() +
  labs(
    title = "Heat map (point density) — Flota Artesanal, Los Lagos (Chile)",
    x = "Longitud", y = "Latitud", fill = "Registros"
  )

ggsave(file.path(out_folder, "fishing_behavior.png"), p_heat, width = 8, height = 7, dpi = 300)

# 6) Basemap (Chile) cropped to vessel track ------------
chile_sf <- ne_countries(country = "Chile", scale = "medium", returnclass = "sf") |>
  st_transform(4326)

# 7) Choose one vessel and compute dwell time per grid cell ------------
one_id <- "ACANTILADA"
DT_one <- DT[`Móvil` == one_id]

stopifnot(nrow(DT_one) > 0)

# Crop basemap around this vessel
xmin <- min(DT_one$Longitud, na.rm = TRUE) - 0.3
xmax <- max(DT_one$Longitud, na.rm = TRUE) + 0.3
ymin <- min(DT_one$Latitud,  na.rm = TRUE) - 0.3
ymax <- max(DT_one$Latitud,  na.rm = TRUE) + 0.3
bb <- st_bbox(c(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax), crs = st_crs(4326))
chile_crop <- st_crop(chile_sf, bb)

# Minutes between pings (proxy for time in area)
DT_one[, dt_min := as.numeric(difftime(DateTime, shift(DateTime), units = "mins"))]
DT_one[dt_min > 60 | dt_min < 0, dt_min := NA]

grid_size <- 0.02
DT_one[, lat_bin := round(Latitud / grid_size) * grid_size]
DT_one[, lon_bin := round(Longitud / grid_size) * grid_size]

cell_time <- DT_one[, .(minutes = sum(dt_min, na.rm = TRUE)), by = .(lat_bin, lon_bin)]
cell_time <- cell_time[minutes > 0]

# 8) Time-colored movement track (one vessel) ------------
p_time <- ggplot() +
  geom_sf(data = chile_crop, fill = "grey95", color = "grey70", linewidth = 0.2) +
  geom_path(
    data = DT_one,
    aes(x = Longitud, y = Latitud, color = DateTime),
    linewidth = 0.8,
    alpha = 0.95
  ) +
  coord_sf(xlim = c(xmin, xmax), ylim = c(ymin, ymax), expand = FALSE) +
  theme_minimal() +
  labs(
    title = paste0("Movement track (time-colored): ", one_id),
    subtitle = paste0(format(min(DT_one$DateTime), "%Y-%m-%d"), " to ", format(max(DT_one$DateTime), "%Y-%m-%d")),
    x = "Longitude", y = "Latitude", color = "Time"
  )

ggsave(file.path(out_folder, paste0("track_time_", one_id, ".png")),
       p_time, width = 8, height = 7, dpi = 300)

# 9) Dwell heatmap (minutes per area) + path overlay ------------
p_dwell <- ggplot() +
  geom_sf(data = chile_crop, fill = "grey95", color = "grey70", linewidth = 0.2) +
  geom_tile(
    data = cell_time,
    aes(x = lon_bin, y = lat_bin, fill = minutes),
    alpha = 0.85
  ) +
  geom_path(
    data = DT_one,
    aes(x = Longitud, y = Latitud),
    linewidth = 0.4,
    alpha = 0.5
  ) +
  coord_sf(xlim = c(xmin, xmax), ylim = c(ymin, ymax), expand = FALSE) +
  scale_fill_viridis_c(trans = "sqrt") +
  theme_minimal() +
  labs(
    title = paste0("Time spent by area (minutes): ", one_id),
    x = "Longitude", y = "Latitude", fill = "Minutes"
  )

ggsave(file.path(out_folder, paste0("dwell_time_", one_id, ".png")),
       p_dwell, width = 8, height = 7, dpi = 300)

# 10) Daily tracks (small multiples) ------------
DT_one[, Day := as.Date(DateTime)]
top_days <- DT_one[, .N, by = Day][order(-N)][1:12, Day]
D_days <- DT_one[Day %in% top_days]

p_daily <- ggplot(D_days, aes(Longitud, Latitud, group = Day)) +
  geom_path(linewidth = 0.6, alpha = 0.8) +
  coord_fixed() +
  theme_minimal() +
  facet_wrap(~ Day, ncol = 4) +
  labs(title = paste0("Daily movement tracks: ", one_id),
       x = "Longitude", y = "Latitude")

ggsave(file.path(out_folder, paste0("daily_tracks_", one_id, ".png")),
       p_daily, width = 12, height = 9, dpi = 300)

# 11) Speed-colored points (if Velocidad exists) ------------
if ("Velocidad" %in% names(DT_one)) {
  DT_one[, Velocidad_num := as.numeric(gsub(",", ".", Velocidad))]
  DT_sp <- DT_one[!is.na(Velocidad_num)]
  
  p_speed <- ggplot() +
    geom_sf(data = chile_crop, fill = "grey95", color = "grey70", linewidth = 0.2) +
    geom_point(data = DT_sp, aes(Longitud, Latitud, color = Velocidad_num),
               alpha = 0.7, size = 0.7) +
    coord_sf(xlim = c(xmin, xmax), ylim = c(ymin, ymax), expand = FALSE) +
    theme_minimal() +
    labs(title = paste0("Positions colored by speed: ", one_id),
         x = "Longitude", y = "Latitude", color = "Speed")
  
  ggsave(file.path(out_folder, paste0("speed_points_", one_id, ".png")),
         p_speed, width = 8, height = 7, dpi = 300)
}

# 12) Direction arrows (if Rumbo exists) ------------
if ("Rumbo" %in% names(DT_one)) {
  DT_one[, Rumbo_num := suppressWarnings(as.numeric(Rumbo))]
  Ddir <- DT_one[!is.na(Rumbo_num)]
  
  k <- 30
  Dthin <- Ddir[seq(1, .N, by = k)]
  
  Dthin[, dx := cos((90 - Rumbo_num) * pi/180)]
  Dthin[, dy := sin((90 - Rumbo_num) * pi/180)]
  
  p_dir <- ggplot() +
    geom_sf(data = chile_crop, fill = "grey95", color = "grey70", linewidth = 0.2) +
    geom_segment(
      data = Dthin,
      aes(x = Longitud, y = Latitud,
          xend = Longitud + 0.02*dx, yend = Latitud + 0.02*dy),
      arrow = arrow(length = unit(0.08, "inches")),
      alpha = 0.7, linewidth = 0.4
    ) +
    coord_sf(xlim = c(xmin, xmax), ylim = c(ymin, ymax), expand = FALSE) +
    theme_minimal() +
    labs(title = paste0("Movement direction (sampled): ", one_id),
         x = "Longitude", y = "Latitude")
  
  ggsave(file.path(out_folder, paste0("direction_arrows_", one_id, ".png")),
         p_dir, width = 8, height = 7, dpi = 300)
}

# 13) Hexbin density (one vessel) ------------
p_hex <- ggplot() +
  geom_sf(data = chile_crop, fill = "grey95", color = "grey70", linewidth = 0.2) +
  geom_hex(data = DT_one, aes(Longitud, Latitud), bins = 60) +
  coord_sf(xlim = c(xmin, xmax), ylim = c(ymin, ymax), expand = FALSE) +
  scale_fill_viridis_c(trans = "sqrt") +
  theme_minimal() +
  labs(title = paste0("Spatial density (hexbin): ", one_id),
       x = "Longitude", y = "Latitude", fill = "Count")

ggsave(file.path(out_folder, paste0("hex_density_", one_id, ".png")),
       p_hex, width = 8, height = 7, dpi = 300)

# 14) Interactive Leaflet map (export to docs/) ------------
Dleaf <- DT_one[seq(1, .N, by = 10)]
time_num <- as.numeric(Dleaf$DateTime)
pal_time <- colorNumeric("viridis", domain = time_num)

m <- leaflet(Dleaf) |>
  addProviderTiles("CartoDB.Positron") |>
  addPolylines(lng = ~Longitud, lat = ~Latitud, color = "steelblue", weight = 2, opacity = 0.5) |>
  addCircleMarkers(
    lng = ~Longitud, lat = ~Latitud,
    radius = 3,
    color = ~pal_time(as.numeric(DateTime)),
    stroke = FALSE, fillOpacity = 0.8,
    popup = ~paste0(
      "<b>Vessel:</b> ", one_id, "<br>",
      "<b>Time:</b> ", format(DateTime, "%Y-%m-%d %H:%M:%S")
    )
  ) |>
  addLegend(
    position = "bottomright",
    pal = pal_time,
    values = time_num,
    title = "Time",
    labFormat = function(type, cuts, p) {
      format(as.POSIXct(cuts, origin = "1970-01-01", tz = tz_use), "%Y-%m-%d")
    }
  )

saveWidget(m, file = file.path(docs_folder, "interactive_map.html"), selfcontained = TRUE)

# 15) Animated GIF (basemap + vessel track) using magick frames loop ------------
frames_dir <- file.path(out_folder, "frames")
dir.create(frames_dir, showWarnings = FALSE, recursive = TRUE)

DT_anim <- DT_one[seq(1, .N, by = 5)]
DT_anim <- DT_anim[!is.na(DateTime) & !is.na(Longitud) & !is.na(Latitud)]
setorder(DT_anim, DateTime)

nframes <- 120
times <- DT_anim$DateTime[round(seq(1, nrow(DT_anim), length.out = nframes))]

for (i in seq_along(times)) {
  t <- times[i]
  Dsub <- DT_anim[DateTime <= t]
  
  p <- ggplot() +
    geom_sf(data = chile_crop, fill = "grey95", color = "grey70", linewidth = 0.2) +
    geom_path(data = Dsub, aes(Longitud, Latitud), alpha = 0.35, linewidth = 0.7) +
    geom_point(data = Dsub, aes(Longitud, Latitud), size = 1.8, alpha = 0.9) +
    coord_sf(xlim = c(xmin, xmax), ylim = c(ymin, ymax), expand = FALSE) +
    theme_minimal() +
    ggtitle(paste0("Movement: ", one_id),
            subtitle = paste0("Time: ", format(t, "%Y-%m-%d %H:%M:%S"))) +
    xlab("Longitude") + ylab("Latitude")
  
  ggsave(
    filename = file.path(frames_dir, sprintf("frame_%04d.png", i)),
    plot = p, width = 8, height = 7, dpi = 120
  )
}

png_files <- list.files(frames_dir, full.names = TRUE, pattern = "\\.png$")
img <- image_read(png_files)
gif <- image_animate(img, fps = 15)
image_write(gif, path = file.path(out_folder, paste0("movement_", one_id, "_chile.gif")))

message("✅ Done. Outputs saved to: ", out_folder, " and interactive map to: ", docs_folder)
