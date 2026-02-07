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





