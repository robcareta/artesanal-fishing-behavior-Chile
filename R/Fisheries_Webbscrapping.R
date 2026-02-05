
# Script webbscrapping fisheries data 

# =============================================================================
# SERNAPESCA - Posiciones geográficas (descarga + DuckDB + filtrado + mapas)
# =============================================================================

rm(list = ls()); gc()

# ---- Packages ----
pkgs <- c(
  "rvest", "httr", "stringr", "DBI", "duckdb",
  "data.table", "ggplot2", "viridis", "gganimate", "gifski"
)
invisible(lapply(pkgs, require, character.only = TRUE))

# ---- Config ----
url <- "https://www.sernapesca.cl/informacion-utilidad/posiciones-geograficas-del-sistema-de-posicionamiento-satelital/"
dest_folder <- "data_raw"
out_folder  <- "outputs"
db_path     <- "sernapesca.duckdb"

dir.create(dest_folder, showWarnings = FALSE, recursive = TRUE)
dir.create(out_folder,  showWarnings = FALSE, recursive = TRUE)

exclude_years <- c("2025")
tz_use <- "America/Santiago"

# =============================================================================
# 1) Download CSV reports
# =============================================================================
page <- read_html(url)

links <- page %>%
  html_nodes("a") %>%
  html_attr("href")

links <- links[!is.na(links)]
links_abs <- url_absolute(links, url)

# Filter: be flexible (Spanish/English + file types)
report_links <- links_abs[
  str_detect(tolower(links_abs), "reporte|report|csv") &
    str_detect(tolower(links_abs), "\\.csv($|\\?)")
]

report_links <- unique(report_links)
cat("Links CSV encontrados:", length(report_links), "\n")

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

# =============================================================================
# 2) Select artisanal fleet files
# =============================================================================
files <- list.files(dest_folder, pattern = "\\.csv$", full.names = TRUE)

artesanal_files <- files[
  str_detect(tolower(files), "flota_artesanal|sernapesca-admin")
]

if (length(exclude_years) > 0) {
  pat <- paste(exclude_years, collapse = "|")
  artesanal_files <- artesanal_files[!str_detect(artesanal_files, pat)]
}

cat("Archivos seleccionados (artesanal):", length(artesanal_files), "\n")
stopifnot(length(artesanal_files) >= 1)

# =============================================================================
# 3) DuckDB: build table
# =============================================================================
con <- dbConnect(duckdb(), dbdir = db_path, read_only = FALSE)
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

dbExecute(con, "DROP TABLE IF EXISTS artisanal")

errores <- character()

normalize_for_duckdb <- function(path) normalizePath(path, winslash = "/", mustWork = FALSE)

first_file <- normalize_for_duckdb(artesanal_files[1])

query_create <- sprintf("
  CREATE TABLE artisanal AS
  SELECT *, '%s' AS source_file
  FROM read_csv_auto('%s', DELIM=';', ALL_VARCHAR=TRUE)
", basename(first_file), first_file)

dbExecute(con, query_create)

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

# =============================================================================
# 4) Filter Los Lagos (by bbox)
# =============================================================================
dbExecute(con, "DROP TABLE IF EXISTS artisanal_loslagos")

dbExecute(con, "
  CREATE TABLE artisanal_loslagos AS
  SELECT *
  FROM artisanal
  WHERE TRY_CAST(Latitude AS DOUBLE) BETWEEN -44.0 AND -39.5
    AND TRY_CAST(Longitude AS DOUBLE) BETWEEN -76.0 AND -72.0
")

print(dbGetQuery(con, "SELECT COUNT(*) AS n FROM artisanal_loslagos"))

# Export filtered CSV
loslagos_csv <- file.path("data_processed", "artisanal_loslagos.csv")
dir.create("data_processed", showWarnings = FALSE, recursive = TRUE)

dbExecute(con, sprintf("
  COPY artisanal_loslagos TO '%s' (HEADER, DELIMITER ',')
", normalize_for_duckdb(loslagos_csv)))

# Pull to R
DT <- as.data.table(dbGetQuery(con, "SELECT * FROM artisanal_loslagos"))

DT[, Latitude  := as.numeric(Latitude)]
DT[, Longitude := as.numeric(Longitude)]

# =============================================================================
# 5) Heatmap of points (counts)
# =============================================================================
agg_data <- DT[, .N, by = .(
  lat_bin = round(Latitude, 2),
  lon_bin = round(Longitude, 2)
)]

p1 <- ggplot(agg_data, aes(x = lon_bin, y = lat_bin, fill = N)) +
  geom_tile() +
  coord_fixed() +
  scale_fill_viridis_c(option = "cividis", trans = "log") +
  labs(
    title = "Mapa de calor de pesca artesanal - Región de Los Lagos",
    x = "Longitud", y = "Latitud", fill = "Registros"
  )

ggsave(file.path(out_folder, "fishing_behavior.png"), p1, width = 8, height = 10, dpi = 360)

# =============================================================================
# 6) Effort proxy (minutes) + animated heatmap
# =============================================================================
DT[, DateTime := as.POSIXct(Date, format = "%d/%m/%Y %H:%M:%S", tz = tz_use)]
DT[, Day := as.Date(DateTime)]

setorder(DT, Mobile, DateTime)
DT[, time_diff := as.numeric(difftime(DateTime, shift(DateTime), units = "mins")), by = Mobile]
DT[time_diff > 60, time_diff := NA]

daily_time <- DT[, .(total_minutes = sum(time_diff, na.rm = TRUE)), by = .(Mobile, Day)]
daily_coords <- DT[, .(Latitude = mean(Latitude, na.rm = TRUE),
                       Longitude = mean(Longitude, na.rm = TRUE)), by = .(Mobile, Day)]
daily_summary <- merge(daily_time, daily_coords, by = c("Mobile", "Day"))

grid_size <- 0.05
heatmap_data <- daily_summary[, .(
  total_minutes = sum(total_minutes, na.rm = TRUE)
), by = .(
  Day,
  lat_bin = round(Latitude / grid_size) * grid_size,
  lon_bin = round(Longitude / grid_size) * grid_size
)]

p_anim <- ggplot(heatmap_data, aes(x = lon_bin, y = lat_bin, fill = total_minutes)) +
  geom_tile() +
  scale_fill_viridis_c(option = "inferno", trans = "sqrt", na.value = "white") +
  coord_fixed() +
  theme_minimal() +
  labs(
    title = "Esfuerzo pesquero artesanal en Los Lagos",
    subtitle = "Día: {frame_time}",
    x = "Longitud", y = "Latitud", fill = "Minutos"
  ) +
  transition_time(Day) +
  ease_aes("linear")

anim <- animate(
  p_anim, nframes = 300, fps = 20,
  width = 800, height = 600,
  renderer = gifski_renderer()
)

anim_save(file.path(out_folder, "mapa_esfuerzo.gif"), animation = anim)
