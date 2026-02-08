## R script to download and process vessel tracking reports from SERNAPESCA
##
## This script generalises the previous solution that only scraped the
## ``Flota Artesanal`` panel.  By defining the ``selected_category``
## variable at the top of the script you can scrape reports for one of
## the four panels currently published on the SERNAPESCA web site:
##
##   * ``"Acuicultura"``
##   * ``"Flota Industrial"``
##   * ``"Flota Artesanal"``
##   * ``"Flota Transportadoras"``
##
## The downloaded CSV files are cached in a ``data_raw`` folder.  When
## the script runs again it will skip any file that already exists in
## the folder, allowing incremental updates as new daily reports are
## published.  After downloading, all CSVs for the chosen category are
## imported into DuckDB.  The script adds two auxiliary columns to
## each record: ``source_file`` (the name of the downloaded CSV) and
## ``category`` (the vessel category, derived from the file name).  A
## second table is created by spatially joining the vessel positions
## with Chilean regional boundaries from the ``rnaturalearth`` package
## so that each record carries a region identifier.  Finally, the
## filtered dataset for the Los Lagos region is exported to
## ``data_processed/artisanal_loslagos.csv`` and loaded into an
## in‑memory ``data.table`` for further analysis.

## Load required packages.  If a package is missing on your system
## please install it via ``install.packages("<pkg>")`` before running
## the script.
required_packages <- c(
  "rvest", "xml2", "httr", "stringr", "DBI", "duckdb",
  "data.table", "sf", "rnaturalearth"
)
invisible(lapply(required_packages, function(pkg) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    stop(sprintf("The package '%s' is required but not installed.\nPlease install it before running this script.", pkg))
  }
}))

## --------------------------------------------------------------------
## Configuration
## --------------------------------------------------------------------
url               <- "https://www.sernapesca.cl/informacion-utilidad/posiciones-geograficas-del-sistema-de-posicionamiento-satelital/"
dest_folder       <- "data_raw"        # folder for downloaded CSVs
processed_folder  <- "data_processed"   # folder for processed outputs
docs_folder       <- "docs"             # folder for auxiliary documentation
out_folder        <- "outputs"          # folder for figures/analysis
db_path           <- "sernapesca.duckdb" # persistent DuckDB database
tz_use            <- "America/Santiago"   # time zone for parsing timestamps

## Choose which category to download.  Replace the value of
## ``selected_category`` with any of the options listed above.  The
## regular expression built from this string should match the heading
## exactly (case and diacritics matter) in the SERNAPESCA web page.
selected_category <- "Flota Artesanal"

## Create required folders if they do not exist
dir.create(dest_folder,      showWarnings = FALSE, recursive = TRUE)
dir.create(processed_folder, showWarnings = FALSE, recursive = TRUE)
dir.create(out_folder,       showWarnings = FALSE, recursive = TRUE)
dir.create(docs_folder,      showWarnings = FALSE, recursive = TRUE)

## Helper: normalise path separators for DuckDB on Windows
normalize_for_duckdb <- function(path) {
  normalizePath(path, winslash = "/", mustWork = FALSE)
}

## Helper: convert missing logical values to FALSE
na_false <- function(x) {
  x[is.na(x)] <- FALSE
  x
}

## --------------------------------------------------------------------
## 1) Download CSV reports for the selected category
## --------------------------------------------------------------------
cat(sprintf("Selected category: '%s'\n", selected_category))

## Read the page once.  We intentionally avoid relying on the
## accordion structure because it has changed over time.  Instead,
## we collect all links on the page, then filter them to retain only
## CSV files that contain the slug for the selected category.  This
## approach is robust to minor changes in the HTML layout because it
## relies on the naming convention of the report files (e.g.
## ``report_31_20260205_flota_artesanal.csv`` for the Flota Artesanal
## category, ``report_32_20260205_flota_industrial.csv`` for the
## industrial category, etc.).
page <- read_html(url)
all_links <- html_attr(html_elements(page, "a"), "href")
all_links <- all_links[!is.na(all_links)]
all_links <- url_absolute(all_links, url)

## Convert the selected category to a slug (used later for table and
## file naming) by replacing one or more whitespace characters with
## an underscore.  The double backslash is required so that ``\s`` is
## interpreted as the whitespace class in the regular expression.
slug_selected <- str_replace_all(tolower(selected_category), "\\s+", "_")

## ------------------------------------------------------------------------
## Determine which report links belong to which category.  Earlier we
## filtered by slug (e.g. "flota_artesanal"), but older files on the
## site sometimes follow different naming conventions (e.g.
## "report-2020-03-08_11_45_28-sernapesca-admin.csv").  To avoid
## missing these files we classify every CSV link based on patterns
## observed in the URL.  This function returns a human‑readable
## category name matching the values accepted by ``selected_category``.
get_category_from_link <- function(link) {
  link_lower <- tolower(link)
  if (str_detect(link_lower, "acuicultura")) {
    return("Acuicultura")
  }
  ## Files with "sernapesca-admin" or "sernapesca_admin" have a generic
  ## naming scheme used in older reports.  These reports cannot be
  ## reliably assigned to a specific fleet based solely on the file
  ## name, so they are labelled as "sernapesca-admin".  You may
  ## inspect the contents later to determine the true category.
  if (str_detect(link_lower, "sernapesca-admin") ||
      str_detect(link_lower, "sernapesca_admin")) {
    return("sernapesca-admin")
  }
  
  ## Artisanal: look for several possible patterns
  if (str_detect(link_lower, "flota_artesanal") ||
      str_detect(link_lower, "artesanal")) {
    return("Flota Artesanal")
  }
  if (str_detect(link_lower, "flota_industrial")) {
    return("Flota Industrial")
  }
  if (str_detect(link_lower, "flota_transportadoras")) {
    return("Flota Transportadoras")
  }
  return(NA_character_)
}

## Collect only CSV links
csv_links <- all_links[str_detect(tolower(all_links), "\\.csv($|\\?)")]  # only CSV files
csv_links <- csv_links[!is.na(csv_links)]

## Classify each link
link_categories <- vapply(csv_links, get_category_from_link, character(1))

## Select links that belong to the chosen category.  ``link_categories`` can
## contain NA when a URL does not match any known category.  A
## comparison like ``link_categories == selected_category`` yields NA
## for these cases, so we coerce NAs to FALSE using the ``na_false``
## helper defined above.
flag_category <- link_categories == selected_category
flag_category <- na_false(flag_category)
report_links <- csv_links[flag_category]
report_links <- unique(report_links)

cat(sprintf("Links CSV encontrados para '%s': %d\n", selected_category, length(report_links)))
print(utils::head(report_links, n = 10))
if (length(report_links) == 0) {
  stop(sprintf("No se encontraron enlaces CSV para la categoría '%s'.\nComprueba que el nombre esté escrito correctamente o que existan reportes en la página.", selected_category))
}

## Download new CSV files.  For each link the destination file name is
## derived from the URL.  If a file with the same name already exists
## in ``dest_folder`` it is skipped.  The ``httr::GET`` call writes
## directly to disk and uses a generous timeout (120 seconds) to
## accommodate large files.
for (link in report_links) {
  file_name  <- basename(str_split(link, "\\?")[[1]][1])
  dest_file  <- file.path(dest_folder, file_name)
  if (!file.exists(dest_file)) {
    cat("Descargando:", file_name, "...\n")
    tryCatch(
      {
        GET(link, write_disk(dest_file, overwrite = TRUE), timeout(120))
      },
      error = function(e) {
        message("Error descargando: ", link, " -> ", e$message)
      }
    )
  }
}

## --------------------------------------------------------------------
## 2) Identify the downloaded CSV files for the selected category
## --------------------------------------------------------------------
files <- list.files(dest_folder, pattern = "\\.csv$", full.names = TRUE)

## A helper to map a downloaded file name to its category based on
## patterns in the file name.  This mirrors ``get_category_from_link``
## above but operates on the base file name instead of the full URL.
get_category_from_filename <- function(name) {
  name_lower <- tolower(name)
  if (str_detect(name_lower, "acuicultura")) {
    return("Acuicultura")
  }
  ## Files with "sernapesca-admin" or "sernapesca_admin" cannot be
  ## unambiguously assigned to a fleet based on the filename alone.
  ## They are labelled as "sernapesca-admin" for further inspection.
  if (str_detect(name_lower, "sernapesca-admin") ||
      str_detect(name_lower, "sernapesca_admin")) {
    return("sernapesca-admin")
  }
  
  if (str_detect(name_lower, "flota_artesanal") ||
      str_detect(name_lower, "artesanal")) {
    return("Flota Artesanal")
  }
  if (str_detect(name_lower, "flota_industrial")) {
    return("Flota Industrial")
  }
  if (str_detect(name_lower, "flota_transportadoras")) {
    return("Flota Transportadoras")
  }
  return(NA_character_)
}

file_categories <- vapply(basename(files), get_category_from_filename, character(1))
## Filter files belonging to the current category using the human readable
## category names.  ``file_categories`` can contain NA when the
## filename does not match any pattern; we coerce NA comparisons to
## FALSE using the ``na_false`` helper before indexing.
flag_files <- file_categories == selected_category
flag_files <- na_false(flag_files)
category_files <- files[flag_files]
if (length(category_files) < 1) {
  stop(sprintf("No se encontraron archivos locales para la categoría '%s'.", selected_category))
}
cat(sprintf("Archivos CSV seleccionados para '%s': %d\n", selected_category, length(category_files)))

## --------------------------------------------------------------------
## 3) Import CSVs into DuckDB and append category/source metadata
## --------------------------------------------------------------------
con <- dbConnect(duckdb(), dbdir = db_path, read_only = FALSE)
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

## Drop any existing table for this category to avoid appending
## duplicates when the script is rerun.  Each category is stored in
## its own table inside the database named after the slug.  For
## example, 'flota_artesanal' becomes table 'flota_artesanal'.
table_name <- slug_selected
dbExecute(con, sprintf("DROP TABLE IF EXISTS %s", DBI::dbQuoteIdentifier(con, table_name)))

## Construct the CREATE TABLE statement using the first CSV.  The
## ``read_csv_auto`` function guesses column types but we force
## ``ALL_VARCHAR=TRUE`` so that all columns are imported as text.
first_file <- normalize_for_duckdb(category_files[1])
create_sql <- sprintf(
  "CREATE TABLE %s AS SELECT *, '%s' AS source_file, '%s' AS category FROM read_csv_auto('%s', DELIM=';', ALL_VARCHAR=TRUE)",
  DBI::dbQuoteIdentifier(con, table_name),
  basename(category_files[1]),
  slug_selected,
  first_file
)
dbExecute(con, create_sql)

## Insert the remaining CSVs into the table.  Each insertion adds two
## metadata columns as above.  Any file that fails to import is
## recorded in the ``errores`` vector for later inspection.
errores <- character(0)
if (length(category_files) > 1) {
  for (f in category_files[-1]) {
    f_norm <- normalize_for_duckdb(f)
    insert_sql <- sprintf(
      "INSERT INTO %s SELECT *, '%s' AS source_file, '%s' AS category FROM read_csv_auto('%s', DELIM=';', ALL_VARCHAR=TRUE)",
      DBI::dbQuoteIdentifier(con, table_name),
      basename(f),
      slug_selected,
      f_norm
    )
    tryCatch(
      {
        dbExecute(con, insert_sql)
      },
      error = function(e) {
        errores <<- c(errores, f)
        warning(sprintf("Error al importar %s -> %s", f, e$message))
      }
    )
  }
}
if (length(errores) > 0) {
  message("Archivos que no se pudieron importar:")
  print(errores)
}

## Count rows imported
cat(sprintf("Total de registros en la tabla %s: %d\n", table_name, dbGetQuery(con, sprintf("SELECT COUNT(*) as n FROM %s", DBI::dbQuoteIdentifier(con, table_name)))[1,1]))

## --------------------------------------------------------------------
## 4) Assign Chilean region identifiers using spatial join
## --------------------------------------------------------------------
## The original data includes latitude (``Latitud``) and longitude
## (``Longitud``) as strings using commas as decimal separators.  We
## import them as text in DuckDB above but now convert them to
## numeric in R.  After coercion we construct an ``sf`` object and
## spatially join it with Natural Earth first‐level administrative
## boundaries for Chile.  The ``rnaturalearth`` package provides
## ``ne_states(country = "Chile")`` which returns polygons for the
## 16 regions of Chile.  Each record will gain the region name in a
## column called ``region``.  Any point falling outside the polygons
## remains with ``NA`` region.

## Pull data into R as a data.table
dt_raw <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM %s", DBI::dbQuoteIdentifier(con, table_name))))

## Convert latitude/longitude strings to numeric.  Some CSVs use
## commas as decimal separators; replace them with dots before
## coercion.  ``suppressWarnings`` avoids noisy NA coercion messages.
suppressWarnings({
  dt_raw[, Latitud := as.numeric(str_replace_all(Latitud, ",", "."))]
  dt_raw[, Longitud := as.numeric(str_replace_all(Longitud, ",", "."))]
})
## Parse datetime.  If a row has an invalid date/time it becomes NA.
dt_raw[, DateTime := as.POSIXct(Fecha, format = "%d/%m/%Y %H:%M:%S", tz = tz_use)]

## Drop rows with missing coordinates or timestamps
dt_raw <- dt_raw[!is.na(Latitud) & !is.na(Longitud) & !is.na(DateTime)]

## Create an sf point object
dt_sf <- st_as_sf(dt_raw, coords = c("Longitud", "Latitud"), crs = 4326, remove = FALSE)

## Load Chilean region boundaries.  Use scale = "small" to keep
## geometry simple.  The ``rnaturalearth`` dataset uses WGS84
## coordinates (EPSG:4326), matching our points.  If the download
## fails due to network issues, instruct the user to run
## ``rnaturalearth::download_ne_data()`` manually.

# Download Chile admin‑1 boundaries via GADM
# level = 1 corresponds to regions; set path to a directory where the files can be saved
gadm_chile <- geodata::gadm(
  country = "CHL",
  level   = 1,
  path    = tempdir()  # or choose any directory you prefer
)

# Convert to sf for spatial joins
chile_regions <- sf::st_as_sf(gadm_chile)


# See which columns are present
print(names(chile_regions))
# You should see columns like "GID_1", "NAME_1", etc.

# If you want to work with human‑readable region names, copy or rename NAME_1:
chile_regions$region_name <- chile_regions$NAME_1

# tryCatch(
#   {
#     chile_regions <- rnaturalearth::ne_states(country = "Chile", returnclass = "sf")
#   },
#   error = function(e) {
#     stop("No se pudieron descargar los límites regionales de Chile.\nCompruebe su conexión a internet o instale datos manualmente con rnaturalearth::download_ne_data().")
#   }
# )

## Perform spatial join: assign each point to a region polygon.  The
## ``st_join`` call returns all attributes from the region polygons
## (including region names) and keeps the original point columns.
# Then join using that column (you can also keep only the geometry and region_name):
dt_joined <- sf::st_join(
  dt_sf,
  chile_regions[, c("region_name")],
  join = sf::st_within,
  left = TRUE
)

## Coerce back to data.table and rename the region field.  Some
## Natural Earth layers include both ``region`` and ``name``; the
## ``name`` field typically holds the region's name (e.g. "Los Lagos").
dt <- as.data.table(dt_joined)
if ("name" %in% names(dt)) {
  setnames(dt, "name", "region_name")
}

## Optionally order by vessel and timestamp
if ("Móvil" %in% names(dt)) {
  setorder(dt, Móvil, DateTime)
}

## --------------------------------------------------------------------
## 5) Filter Los Lagos region and export
## --------------------------------------------------------------------
## Create a separate table with only points in the Los Lagos region.
## This uses the region field assigned in the spatial join above.  A
## bounding box filter is kept as a backup, in case the spatial join
## fails or returns NA for some points.
dt_loslagos <- dt[(region_name == "Los Lagos") |
                    (Latitud >= -44.0 & Latitud <= -39.5 &
                       Longitud >= -76.0 & Longitud <= -72.0)]

## Drop and recreate the table in DuckDB for the Los Lagos subset
dbExecute(con, "DROP TABLE IF EXISTS artisanal_loslagos")
dbExecute(con, sprintf(
  "CREATE TABLE artisanal_loslagos AS SELECT * FROM read_csv_auto('%s', ALL_VARCHAR=TRUE)",
  normalize_for_duckdb(file.path(dest_folder, basename(category_files[1])))
))

## Alternatively, export the Los Lagos subset to CSV for inspection
loslagos_csv <- file.path(processed_folder, paste0(slug_selected, "_loslagos.csv"))
fwrite(dt_loslagos, loslagos_csv)

cat(sprintf("Se exportaron %d registros de la región de Los Lagos a %s\n", nrow(dt_loslagos), loslagos_csv))

## End of script