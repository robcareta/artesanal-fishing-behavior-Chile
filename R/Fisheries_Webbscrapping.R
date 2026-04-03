## R script to download and process vessel tracking reports from SERNAPESCA
##
## This script generalises the previous solution that only scraped the
## ``Flota Artesanal`` panel.  By defining the ``selected_category``
## variable at the top of the script you can scrape reports for one of
## the four panels currently published on the SERNAPESCA web site:
##
##   * ``"Acuicultura"``
##   * ``"Flota Industrial"``
##   * ``"Flota Artesanal"``
##   * ``"Flota Transportadoras"``
##
## The downloaded CSV files are cached in a ``data_raw`` folder.  When
## the script runs again it will skip any file that already exists in
## the folder, allowing incremental updates as new daily reports are
## published.  After downloading, all CSVs for the chosen category are
## normalised to a canonical 12-column schema (handling 50+ column name
## variations) and merged with rbindlist().  The merged data is saved to
## DuckDB and exported.  A spatial join assigns Chilean regions to each
## record.  Finally, the Los Lagos subset is exported to CSV.

## Load required packages.
required_packages <- c(
  "rvest", "xml2", "httr", "stringr", "DBI", "duckdb",
  "data.table", "sf", "geodata"
)
invisible(lapply(required_packages, function(pkg) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    stop(sprintf("The package '%s' is required but not installed.\nPlease install it before running this script.", pkg))
  }
}))

## --------------------------------------------------------------------
## Configuration
## --------------------------------------------------------------------
url_base          <- "https://www.sernapesca.cl"
url               <- paste0(url_base, "/informacion-utilidad/posiciones-geograficas-del-sistema-de-posicionamiento-satelital/")
dest_folder       <- "data_raw"        # folder for downloaded CSVs
processed_folder  <- "data_processed"  # folder for processed outputs
out_folder        <- "outputs"         # folder for figures/analysis
db_path           <- "sernapesca.duckdb"
tz_use            <- "America/Santiago"
max_retries       <- 3
retry_wait        <- 5

## Choose which category to download.
selected_category <- "Flota Artesanal"

## Create required folders if they do not exist
dir.create(dest_folder,      showWarnings = FALSE, recursive = TRUE)
dir.create(processed_folder, showWarnings = FALSE, recursive = TRUE)
dir.create(out_folder,       showWarnings = FALSE, recursive = TRUE)

# progress log helper
progress_log <- file.path(".", "script_progress.log")
write_log <- function(msg) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  cat(sprintf("[%s] %s\n", timestamp, msg), file = progress_log, append = TRUE)
}
write_log(sprintf("Script started; selected_category=%s", selected_category))

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
## Category parameters (report number and URL slug per category)
## --------------------------------------------------------------------
FLEET_PARAMS <- list(
  "Acuicultura"           = list(report_num = "30", slug = "acuicultura"),
  "Flota Artesanal"       = list(report_num = "31", slug = "flota_artesanal"),
  "Flota Industrial"      = list(report_num = "32", slug = "flota_industrial"),
  "Flota Transportadoras" = list(report_num = "33", slug = "flota_transportadoras")
)

if (!selected_category %in% names(FLEET_PARAMS)) {
  stop(sprintf("Categoría no válida. Opciones: %s",
               paste(names(FLEET_PARAMS), collapse = ", ")))
}

fleet_num  <- FLEET_PARAMS[[selected_category]]$report_num
fleet_slug <- FLEET_PARAMS[[selected_category]]$slug
slug_selected <- fleet_slug   # used for table/file naming

## --------------------------------------------------------------------
## Helper functions
## --------------------------------------------------------------------

## Classify a URL or filename into a category.
## Pre-2020 SERNAPESCA used "sernapesca-admin" as the filename suffix for
## what is now the Flota Artesanal report (all vessels have the (ART) tag).
## These files live at /sites/default/files/ with a timestamp in the name,
## so they cannot be discovered by the date-pattern generator — only via
## page scraping.  We map them to "Flota Artesanal" so they are not
## silently discarded.
classify_fleet <- function(text) {
  if (is.na(text) || nchar(trimws(text)) == 0) return(NA_character_)
  text <- tolower(text)
  if (str_detect(text, "acuicultura"))                          return("Acuicultura")
  if (str_detect(text, "flota_artesanal|artesanal"))            return("Flota Artesanal")
  if (str_detect(text, "flota_industrial"))                     return("Flota Industrial")
  if (str_detect(text, "flota_transportadoras|transportadora")) return("Flota Transportadoras")
  if (str_detect(text, "sernapesca.?admin"))                    return("Flota Artesanal")
  return(NA_character_)
}

## Download with retries
download_with_retry <- function(url, dest, retries = max_retries, wait = retry_wait) {
  for (i in seq_len(retries)) {
    result <- tryCatch({
      resp <- GET(url, write_disk(dest, overwrite = TRUE), timeout(120))
      code <- status_code(resp)
      size <- if (file.exists(dest)) file.size(dest) else 0
      list(ok = (code == 200 && size > 100), code = code)
    }, error = function(e) {
      message(sprintf("    Attempt %d/%d error: %s", i, retries, e$message))
      list(ok = FALSE, code = NA)
    })
    if (result$ok) return(TRUE)
    if (!is.na(result$code) && result$code == 404) return(FALSE)
    if (i < retries) Sys.sleep(wait)
  }
  if (file.exists(dest)) file.remove(dest)
  return(FALSE)
}

## Detect HTML error pages saved as CSV
is_html_file <- function(path) {
  tryCatch({
    first <- readLines(path, n = 1, warn = FALSE, encoding = "latin1")
    str_detect(tolower(first[1]), "<!doctype|<html")
  }, error = function(e) FALSE)
}

## Normalise a string for comparison: lowercase, remove accents.
## Uses gsub() instead of chartr() + iconv(ASCII//TRANSLIT) so the result
## is identical on Windows and Linux.  iconv//TRANSLIT converts ñ→~n on
## Windows glibc but n on Linux, breaking COL_MAP matches on Windows.
normalize_str <- function(s) {
  s <- tolower(trimws(s))
  s <- gsub("\u00e1|\u00e0|\u00e4|\u00e2", "a", s)   # á à ä â
  s <- gsub("\u00e9|\u00e8|\u00eb|\u00ea", "e", s)   # é è ë ê
  s <- gsub("\u00ed|\u00ec|\u00ef|\u00ee", "i", s)   # í ì ï î
  s <- gsub("\u00f3|\u00f2|\u00f6|\u00f4", "o", s)   # ó ò ö ô
  s <- gsub("\u00fa|\u00f9|\u00fc|\u00fb", "u", s)   # ú ù ü û
  s <- gsub("\u00f1",                      "n", s)   # ñ
  ## Strip any residual non-ASCII bytes (e.g. ~n from Windows iconv,
  ## BOM \uFEFF, or other encoding artefacts) that are not alphanumeric,
  ## spaces, parentheses, dots, underscores, or slashes.
  s <- gsub("[^a-z0-9 ()./_-]", "", s)
  s
}

## --------------------------------------------------------------------
## Canonical column schema (handles 50+ column name variations)
## --------------------------------------------------------------------
CANONICAL_COLS <- c(
  "movil", "matricula", "puerto", "baliza", "senal_radio",
  "fecha", "latitud", "longitud", "rumbo", "velocidad", "fuente", "fecha_mod"
)

## COL_MAP covers every column-name variant observed across all SERNAPESCA
## file generations (2019 sernapesca-admin → 2020 artesanales → 2022+ reports).
## Key additions vs original:
##   movil:       "mobile"          (2019 sernapesca-admin format)
##   senal_radio: "radio call sign" (2019 sernapesca-admin, without "(rc)")
##   fecha:       "date"            (already present; kept for clarity)
##   velocidad:   "speed"           (2019 sernapesca-admin, without "(kt)")
COL_MAP <- list(
  movil       = c("movil", "ma3vil", "name", "nombre", "vessel", "mobile"),
  matricula   = c("matricula", "matriucula", "internal ref.", "internal_ref",
                  "internal ref"),
  puerto      = c("puerto", "port"),
  baliza      = c("baliza", "beacon"),
  senal_radio = c("senal de llamada de radio", "seal de llamada de radio",
                  "senal de llamada", "seal de llamada",
                  "radio call sign (rc)", "radio call sign",
                  "call_sign", "callsign"),
  fecha       = c("fecha", "location date", "loc_date",
                  "fecha de posicion", "fecha de posicia3n",
                  "datetime", "date"),
  latitud     = c("latitud", "latitude"),
  longitud    = c("longitud", "longitude"),
  rumbo       = c("rumbo", "heading"),
  velocidad   = c("velocidad", "velocidad (kt)", "speed (kt)", "speed"),
  fuente      = c("fuente", "source"),
  fecha_mod   = c("fecha de modificacion", "fecha de modificacia3n",
                  "modification date")
)

## Read one CSV and map all columns to the canonical schema.
## Returns a data.table with exactly CANONICAL_COLS + source_file + category,
## so rbindlist() always produces a consistent table regardless of source file.
read_and_normalize <- function(f) {
  if (is_html_file(f)) return(NULL)

  dt <- NULL
  for (enc in c("UTF-8", "Latin-1")) {
    for (sep in c(";", ",")) {
      tryCatch({
        tmp <- fread(
          f, sep = sep, encoding = enc, colClasses = "character",
          fill = TRUE, showProgress = FALSE,
          na.strings = c("", "NA", "N/A", "-", "NULL")
        )
        if (ncol(tmp) >= 3 && nrow(tmp) > 0) { dt <- tmp; break }
      }, error = function(e) NULL)
      if (!is.null(dt)) break
    }
    if (!is.null(dt)) break
  }
  if (is.null(dt)) return(NULL)

  ## Fix column name encoding: convert to UTF-8 regardless of source encoding,
  ## then transliterate to ASCII so tolower() never encounters invalid bytes.
  names(dt) <- iconv(names(dt), from = "", to = "UTF-8", sub = "byte")
  names(dt) <- iconv(names(dt), to = "ASCII//TRANSLIT", sub = "")

  ## Drop garbage columns (HTML artefacts, zone codes, etc.)
  bad <- str_detect(tolower(names(dt)), "uda\\.position|zone[0-9]|<!doctype")
  dt  <- dt[, !bad, with = FALSE]

  ## Map each raw column to the canonical schema using normalize_str()
  cur_norm <- normalize_str(names(dt))
  result   <- data.table(matrix(NA_character_, nrow = nrow(dt), ncol = length(CANONICAL_COLS)))
  setnames(result, CANONICAL_COLS)

  for (canon in CANONICAL_COLS) {
    targets <- normalize_str(COL_MAP[[canon]])
    hit     <- which(cur_norm %in% targets)
    if (length(hit) > 0) result[[canon]] <- dt[[names(dt)[hit[1]]]]
  }

  result[, source_file := basename(f)]
  result[, category    := selected_category]
  return(result)
}

## --------------------------------------------------------------------
## 1) Build the full list of URLs (page scraping + date-pattern generation)
## --------------------------------------------------------------------
cat(sprintf("\n[1] Building URL list for '%s'...\n", selected_category))

## A) Scrape the current page (covers the last ~3 months)
page      <- read_html(url)
raw_links <- html_attr(html_elements(page, "a"), "href")
raw_links <- raw_links[!is.na(raw_links) & nchar(trimws(raw_links)) > 0]
raw_links <- url_absolute(raw_links, url)

csv_links_page <- raw_links[str_detect(tolower(raw_links), "\\.csv($|\\?)")]
cats_page      <- vapply(csv_links_page, classify_fleet, character(1))
links_from_page <- csv_links_page[!is.na(cats_page) & cats_page == selected_category]
cat(sprintf("  Links found on current page: %d\n", length(links_from_page)))

## B) Generate URLs by date pattern from 2019 to today.
## SERNAPESCA has used two base paths for the same report naming scheme:
##   - /sites/default/files/  (used up to roughly mid-2023)
##   - /app/uploads/YYYY/MM/  (used from roughly mid-2023 onwards)
## We generate both for every date so that files on either path are found.
## NOTE: Pre-2020 files use a timestamp-based "sernapesca-admin" name that
## cannot be predicted; those are captured exclusively via page scraping (A).
date_start <- as.Date("2019-01-01")
date_end   <- Sys.Date()
all_dates  <- seq(date_start, date_end, by = "day")

links_generated <- unlist(lapply(all_dates, function(d) {
  yyyy     <- format(d, "%Y")
  mm       <- format(d, "%m")
  yyyymmdd <- format(d, "%Y%m%d")
  c(
    ## Path 1: older WordPress uploads path (used ~2022–mid-2023)
    sprintf("%s/sites/default/files/report_%s_%s_%s.csv",
            url_base, fleet_num, yyyymmdd, fleet_slug),
    ## Path 2: newer app/uploads path (used mid-2023+)
    sprintf("%s/app/uploads/%s/%s/report_%s_%s_%s.csv",
            url_base, yyyy, mm, fleet_num, yyyymmdd, fleet_slug)
  )
}))

cat(sprintf("  URLs generated by date (%s to %s): %d\n",
            format(date_start), format(date_end), length(links_generated)))

## Combine both lists, deduplicate
all_report_links <- unique(c(links_from_page, links_generated))
cat(sprintf("  Total unique URLs to check/download: %d\n", length(all_report_links)))
write_log(sprintf("Total URLs: %d", length(all_report_links)))

if (length(all_report_links) == 0) {
  stop(sprintf("No CSV links found for category '%s'.", selected_category))
}

## --------------------------------------------------------------------
## 2) Incremental download
## --------------------------------------------------------------------
cat("\n[2] Downloading (skipping already-downloaded files)...\n")

failed       <- character(0)
skipped      <- 0L
n_downloaded <- 0L

for (i in seq_along(all_report_links)) {
  link  <- all_report_links[i]
  fname <- basename(str_split(link, "\\?")[[1]][1])
  dest  <- file.path(dest_folder, fname)

  ## Skip only if the file exists, is large enough, and is a real CSV
  if (file.exists(dest) && file.size(dest) > 100 && !is_html_file(dest)) {
    skipped <- skipped + 1L
    next
  }

  ## Remove stale HTML files before attempting re-download
  if (file.exists(dest) && is_html_file(dest)) file.remove(dest)

  ok <- download_with_retry(link, dest)
  if (ok) {
    if (is_html_file(dest)) {
      file.remove(dest)   ## server returned 200 but with HTML content
    } else {
      n_downloaded <- n_downloaded + 1L
      if (n_downloaded %% 100 == 0)
        cat(sprintf("  Downloaded: %d  |  Progress: %d/%d\n",
                    n_downloaded, i, length(all_report_links)))
    }
  } else {
    ## 404 or error — remove any partial/HTML file left on disk
    if (file.exists(dest)) file.remove(dest)
  }
  ## 404 = date with no report on server (normal for old years) → not a failure
}

cat(sprintf("\n  Download summary: new=%d | already existed=%d | failed=%d\n",
            n_downloaded, skipped, length(failed)))
write_log(sprintf("Downloads: new=%d, skipped=%d, failed=%d",
                  n_downloaded, skipped, length(failed)))

## --------------------------------------------------------------------
## 3) Identify valid local CSV files for the selected category
## --------------------------------------------------------------------
all_local  <- list.files(dest_folder, pattern = "\\.csv$", full.names = TRUE)
local_cats <- vapply(basename(all_local), classify_fleet, character(1))
cat_files  <- all_local[!is.na(local_cats) & local_cats == selected_category]
cat_files  <- cat_files[!vapply(cat_files, is_html_file, logical(1))]

cat(sprintf("\n[3] Valid CSV files for '%s': %d\n", selected_category, length(cat_files)))

if (length(cat_files) == 0) {
  stop(sprintf("No local files found for category '%s'.", selected_category))
}

## --------------------------------------------------------------------
## 4) Read all CSVs, normalise to canonical schema, and merge
## --------------------------------------------------------------------
## Because each file is mapped to the same CANONICAL_COLS before merging,
## rbindlist() always produces a consistent table — no information is lost
## due to column name differences across files.
cat("\n[4] Reading and normalising CSVs...\n")

list_dts  <- vector("list", length(cat_files))
n_skipped <- 0L

# for (i in seq_along(cat_files)) {
#   res <- read_and_normalize(cat_files[i])
#   if (is.null(res)) {
#     n_skipped <- n_skipped + 1L
#   } else {
#     list_dts[[i]] <- res
#   }
#   if (i %% 100 == 0 || i == length(cat_files))
#     cat(sprintf("  %d/%d processed  (discarded: %d)\n", i, length(cat_files), n_skipped))
# }

# # Filtering just for Los Lagos
# ## --------------------------------------------------------------------
# ## 4) Read all CSVs, normalise to canonical schema, and merge
# ## --------------------------------------------------------------------
# cat("\n[4] Reading and normalising CSVs...\n")
# 
# list_dts  <- vector("list", length(cat_files))
# n_skipped <- 0L
# bbox_lat  <- c(-44.0, -39.5)
# bbox_lon  <- c(-76.0, -72.0)
# 
# for (i in seq_along(cat_files)) {
#   res <- read_and_normalize(cat_files[i])
#   if (!is.null(res)) {
#     # parse and filter coordinates as shown earlier
#     res[, lat_num := as.numeric(gsub("[°º]", "", gsub(",", ".", latitud)))]
#     res[, lon_num := as.numeric(gsub("[°º]", "", gsub(",", ".", longitud)))]
#     res <- res[
#       !is.na(lat_num) & !is.na(lon_num) &
#         lat_num >= -44  & lat_num <= -39.5 &
#         lon_num >= -76  & lon_num <= -72
#     ]
#     res[, c("lat_num","lon_num") := NULL]
#     if (nrow(res) > 0) {
#       # append directly to a CSV or to DuckDB
#       # for example, write to a temp file with fwrite(..., append = TRUE)
#       fwrite(res, file = file.path(processed_folder, "los_lagos_filtered.csv"),
#              append = TRUE)
#     }
#   }
# }
# 
# # Remove NULL entries from the list
# list_dts <- Filter(Negate(is.null), list_dts)
# 
# 
# 
# list_dts <- Filter(Negate(is.null), list_dts)
# 
# 
# #list_dts <- Filter(Negate(is.null), list_dts)
# cat(sprintf("  Read: %d  |  Discarded: %d\n", length(list_dts), n_skipped))
# write_log(sprintf("Files read: %d, discarded: %d", length(list_dts), n_skipped))
# 
# if (length(list_dts) == 0) stop("No valid files found for this category.")
# 
# ## Merge: because all data.tables have identical columns, fill = FALSE is safe
# ## and guarantees a clean, consistent schema in the output.
# dt_all <- rbindlist(list_dts, fill = FALSE, use.names = TRUE)
# rm(list_dts); gc()
# 
# cat(sprintf("  Total records merged: %s\n", format(nrow(dt_all), big.mark = ",")))
# 
# ## Sanitize all character columns: force valid UTF-8, drop unrepresentable bytes.
# ## This prevents DuckDB and downstream tools from failing on malformed strings.
# char_cols <- names(dt_all)[vapply(dt_all, is.character, logical(1))]
# for (col in char_cols) {
#   set(dt_all, j = col,
#       value = iconv(dt_all[[col]], from = "UTF-8", to = "UTF-8", sub = ""))
# }
# write_log(sprintf("Total records: %d", nrow(dt_all)))


## --------------------------------------------------------------------
## 4) Read all CSVs, normalise to canonical schema, filter to Los Lagos, and merge
## --------------------------------------------------------------------
cat("\n[4] Reading and normalising CSVs (Los Lagos only)…\n")

list_dts  <- vector("list", length(cat_files))
n_skipped <- 0L

# Simple parser for decimal coordinates with optional degree symbol
parse_coord_simple <- function(x) {
  # remove degree symbols, replace comma decimal separator, trim whitespace
  x <- trimws(x)
  x <- gsub("[°º]", "", x)
  x <- gsub(",", ".", x)
  suppressWarnings(as.numeric(x))
}

for (i in seq_along(cat_files)) {
  res <- read_and_normalize(cat_files[i])
  if (is.null(res)) {
    n_skipped <- n_skipped + 1L
  } else {
    # Convert lat/lon strings to numeric
    res[, lat_num := parse_coord_simple(latitud)]
    res[, lon_num := parse_coord_simple(longitud)]
    # Filter to Los Lagos bounding box
    res <- res[
      !is.na(lat_num) & !is.na(lon_num) &
        lat_num >= -44.0 & lat_num <= -39.5 &
        lon_num >= -76.0 & lon_num <= -72.0
    ]
    # Drop temporary numeric columns
    res[, c("lat_num","lon_num") := NULL]
    # Append to the list only if any rows remain
    if (nrow(res) > 0) {
      list_dts[[i]] <- res
    } else {
      n_skipped <- n_skipped + 1L
    }
  }
  
  # Print progress every 100 files or at the end
  if (i %% 100 == 0 || i == length(cat_files))
    cat(sprintf("  %d/%d processed  (discarded: %d)\n",
                i, length(cat_files), n_skipped))
}

# Remove NULL entries (files with no Los Lagos records)
list_dts <- Filter(Negate(is.null), list_dts)
cat(sprintf("  Read: %d  |  Discarded: %d\n",
            length(list_dts), n_skipped))
write_log(sprintf("Files read: %d, discarded: %d",
                  length(list_dts), n_skipped))

if (length(list_dts) == 0) {
  stop("No valid Los Lagos records found.")
}

# Merge the filtered tables
# Instead of merging list_dts:
loslagos_csv <- file.path(processed_folder, "los_lagos_filtered.csv")
dt_all <- fread(loslagos_csv)
cat(sprintf("Loaded Los Lagos subset: %s records\n",
            format(nrow(dt_all), big.mark = ",")))

# rm(list_dts); gc()
# Read the filtered data

# Sanitize character columns: force valid UTF‑8
char_cols <- names(dt_all)[vapply(dt_all, is.character, logical(1))]
for (col in char_cols) {
  set(dt_all, j = col,
      value = iconv(dt_all[[col]], from = "UTF-8", to = "UTF-8", sub = ""))
}
write_log(sprintf("Total records: %d", nrow(dt_all)))

# # Sanitize character columns: force valid UTF‑8
# char_cols <- names(dt_all)[vapply(dt_all, is.character, logical(1))]
# for (col in char_cols) {
#   set(dt_all, j = col,
#       value = iconv(dt_all[[col]], from = "UTF-8", to = "UTF-8", sub = ""))
# }
# write_log(sprintf("Total records: %d", nrow(dt_all)))

## --------------------------------------------------------------------
## 5) Type conversion
## --------------------------------------------------------------------
cat("\n[5] Converting types...\n")

## Helper: parse coordinates that may be in any of three formats:
##   1. Plain decimal:              "-41.486667"
##   2. Decimal with degree symbol: "-41.486667°"  or  "-41.486667 °"
##   3. DMS with hemisphere:        "41°29'12.00 S"  /  "72°58'41.16 W"
parse_coord <- function(x) {
  x <- trimws(x)

  ## --- format 3: DMS  e.g. "41°29'12.00 S" or "41°29'12.00S" ----------
  dms_pat <- "^(\\d+)[°º](\\d+)'([0-9.]+)\"?\\s*([NSEWnsew])$"
  is_dms  <- grepl(dms_pat, x, perl = TRUE)
  if (any(is_dms, na.rm = TRUE)) {
    m    <- regmatches(x[is_dms], regexec(dms_pat, x[is_dms], perl = TRUE))
    degs <- as.numeric(vapply(m, `[[`, "", 2))
    mins <- as.numeric(vapply(m, `[[`, "", 3))
    secs <- as.numeric(vapply(m, `[[`, "", 4))
    hemi <- toupper(vapply(m, `[[`, "", 5))
    dec  <- (degs + mins / 60 + secs / 3600) * ifelse(hemi %in% c("S", "W"), -1, 1)
    ## strip ° from non-DMS values (e.g. "-40.128°") before numeric conversion
    x_clean <- str_remove_all(x, "[°º]")
    out        <- suppressWarnings(as.numeric(str_replace_all(x_clean, ",", ".")))
    out[is_dms] <- dec
    return(out)
  }

  ## --- format 2: strip degree symbol, then plain numeric ---------------
  x <- str_remove_all(x, "[°º]")

  ## --- format 1: replace comma decimal separator -----------------------
  suppressWarnings(as.numeric(str_replace_all(x, ",", ".")))
}

suppressWarnings({
  dt_all[, latitud   := parse_coord(latitud)]
  dt_all[, longitud  := parse_coord(longitud)]
  ## Strip " kt" suffix from 2024 speed values before numeric conversion
  dt_all[, velocidad := as.numeric(str_replace_all(
                          str_remove_all(velocidad, "\\s*kt\\s*"), ",", "."))]
  dt_all[, rumbo     := as.numeric(str_replace_all(rumbo, ",", "."))]
})

date_formats <- c(
  "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S",
  "%Y/%m/%d %H:%M:%S", "%d/%m/%Y %H:%M",    "%d/%m/%Y"
)
dt_all[, DateTime := as.POSIXct(NA_real_, tz = tz_use)]
for (fmt in date_formats) {
  mask <- is.na(dt_all$DateTime) & !is.na(dt_all$fecha)
  if (!any(mask)) break
  suppressWarnings(
    dt_all[mask, DateTime := as.POSIXct(fecha[mask], format = fmt, tz = tz_use)]
  )
}

pct_fecha <- 100 * mean(!is.na(dt_all$DateTime))
pct_coord <- 100 * mean(!is.na(dt_all$latitud) & !is.na(dt_all$longitud))
cat(sprintf("  Dates OK: %.1f%%  |  Coordinates OK: %.1f%%\n", pct_fecha, pct_coord))

dt_all[!is.na(DateTime), year := as.integer(format(DateTime, "%Y"))]
cat("\nTemporal coverage (records per year):\n")
print(dt_all[!is.na(year), .N, by = year][order(year)])

## --------------------------------------------------------------------
## 6) Save merged data to DuckDB
## --------------------------------------------------------------------
cat("\n[6] Saving to DuckDB...\n")

con <- dbConnect(duckdb(), dbdir = db_path, read_only = FALSE)
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

tryCatch({
  dbWriteTable(con, slug_selected, as.data.frame(dt_all), overwrite = TRUE)
  cat(sprintf("  Table '%s' saved: %s records\n", slug_selected,
              format(nrow(dt_all), big.mark = ",")))
}, error = function(e) {
  warning(sprintf("DuckDB write failed (non-fatal): %s", e$message))
})




# continue where the data R session crushed and collapsed, so I will work now with the saved data for Los Lagos 
library(data.table)
loslagos_csv <- file.path(processed_folder, "los_lagos_filtered.csv")
dt_all <- fread(loslagos_csv)
cat("Loaded Los Lagos subset: ", nrow(dt_all), " records\n")


# sanitize character columns 
char_cols <- names(dt_all)[vapply(dt_all, is.character, logical(1))]
for (col in char_cols) {
  set(dt_all, j = col,
      value = iconv(dt_all[[col]], from = "UTF-8", to = "UTF-8", sub = ""))
}


# Convert coordinates, speeds, headings and dates. 
parse_coord_simple <- function(x) {
  x <- trimws(x)
  x <- gsub("[°º]", "", x)
  x <- gsub(",", ".", x)
  suppressWarnings(as.numeric(x))
}

dt_all[, latitud  := parse_coord_simple(latitud)]
dt_all[, longitud := parse_coord_simple(longitud)]
dt_all[, velocidad := as.numeric(gsub(",", ".", gsub("\\s*kt\\s*", "", velocidad)))]
dt_all[, rumbo     := as.numeric(gsub(",", ".", rumbo))]

# Convert fecha to POSIXct (as in your original Step 5)
date_formats <- c("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                  "%d-%m-%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S",
                  "%d/%m/%Y %H:%M", "%d/%m/%Y")
dt_all[, DateTime := as.POSIXct(NA_real_, tz = tz_use)]
for (fmt in date_formats) {
  mask <- is.na(dt_all$DateTime) & !is.na(dt_all$fecha)
  if (!any(mask)) break
  suppressWarnings(
    dt_all[mask, DateTime := as.POSIXct(fecha[mask], format = fmt, tz = tz_use)]
  )
}

# save dt_all 
write.csv(dt_all, "dt_all.csv")

# Perform the spatial join on the Los Lagos subset.
library(sf)
# Filter out any stray records with missing or out‑of‑range coordinates
dt_geo <- dt_all[!is.na(latitud) & !is.na(longitud) &
                   latitud >= -44 & latitud <= -39.5 &
                   longitud >= -76 & longitud <= -72]

# Convert to sf and project to UTM 18S (EPSG 32718)
dt_sf <- st_as_sf(dt_geo, coords = c("longitud", "latitud"),
                  crs = 4326, remove = FALSE)
dt_sf <- st_transform(dt_sf, 32718)

# Load Chilean regions and isolate Los Lagos polygon
chile_sf  <- st_as_sf(geodata::gadm("CHL", level = 1, path = tempdir()))
chile_sf  <- st_transform(chile_sf, 32718)
los_lagos <- chile_sf[chile_sf$NAME_1 == "Los Lagos", ]

# Join only against the Los Lagos polygon
dt_joined <- st_join(dt_sf, los_lagos, join = st_within, left = TRUE)

# Convert back to data.table and drop geometry if desired
dt_final <- as.data.table(dt_joined)
dt_final[, geometry := NULL]



## --------------------------------------------------------------------
## 7) Spatial join: assign Chilean region to each record
## --------------------------------------------------------------------
cat("\n[7] Spatial join with Chilean regions...\n")

dt_geo <- dt_all[!is.na(latitud) & !is.na(longitud) &
                   between(latitud, -90, 90) & between(longitud, -180, 180)]

dt_sf <- st_as_sf(dt_geo, coords = c("longitud", "latitud"), crs = 4326, remove = FALSE)

chile_sf <- st_as_sf(geodata::gadm("CHL", level = 1, path = tempdir()))
chile_sf <- chile_sf[, c("NAME_1", "geometry")]
names(chile_sf)[1] <- "region_name"

dt_joined <- st_join(dt_sf, chile_sf, join = st_within, left = TRUE)
dt_final  <- as.data.table(dt_joined)
dt_final[, geometry := NULL]

## Sort by vessel and timestamp (optional — skipped if memory is insufficient)
if ("movil" %in% names(dt_final) && "DateTime" %in% names(dt_final)) {
  tryCatch(
    setorder(dt_final, movil, DateTime),
    error = function(e) message("  Sorting skipped (memory): ", e$message)
  )
}

tryCatch({
  dbWriteTable(con, paste0(slug_selected, "_geo"), as.data.frame(dt_final), overwrite = TRUE)
}, error = function(e) {
  warning(sprintf("DuckDB write (_geo) failed (non-fatal): %s", e$message))
})

## --------------------------------------------------------------------
## 8) Filter Los Lagos region and export
## --------------------------------------------------------------------
cat("\n[8] Exporting Los Lagos subset...\n")

## Primary filter: region assigned by the spatial join.
## Fallback bounding box: covers the Los Lagos region approximately.
dt_loslagos <- dt_final[
  (region_name == "Los Lagos") |
    (latitud >= -44.0 & latitud <= -39.5 & longitud >= -76.0 & longitud <= -72.0)
]

## Save Los Lagos subset to DuckDB
tryCatch({
  dbWriteTable(con, "artesanal_loslagos", as.data.frame(dt_loslagos), overwrite = TRUE)
}, error = function(e) {
  warning(sprintf("DuckDB write (loslagos) failed (non-fatal): %s", e$message))
})

## Export to CSV
loslagos_csv <- file.path(processed_folder, paste0(slug_selected, "_loslagos.csv"))
fwrite(dt_loslagos, loslagos_csv)

cat(sprintf("  Los Lagos: %s records exported to %s\n",
            format(nrow(dt_loslagos), big.mark = ","), loslagos_csv))
write_log(sprintf("Los Lagos records: %d", nrow(dt_loslagos)))

## --------------------------------------------------------------------
## Summary
## --------------------------------------------------------------------
cat("\n========== FINAL SUMMARY ==========\n")
cat(sprintf("Category         : %s\n", selected_category))
cat(sprintf("Files processed  : %d\n", length(cat_files)))
cat(sprintf("Total records    : %s\n", format(nrow(dt_all),      big.mark = ",")))
cat(sprintf("With coordinates : %s\n", format(nrow(dt_geo),      big.mark = ",")))
cat(sprintf("Los Lagos        : %s\n", format(nrow(dt_loslagos), big.mark = ",")))
cat(sprintf("Database         : %s\n", db_path))
cat(sprintf("Output CSV       : %s\n", loslagos_csv))
cat("====================================\n")

## Objects available in environment: dt_all, dt_final, dt_loslagos
