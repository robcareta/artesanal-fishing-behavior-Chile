## rebuild_artesanal_only.R
## Rebuilds a CONSISTENT artisanal-only dataset covering 2019-2026.
## Key difference from rebuild_los_lagos.R:
##   admin_2019 files are filtered to vessels with (ART) or (LB) suffix ONLY
##   (removes industrial, transport, unlabelled vessels that inflated 2019-2022 counts)
## Output: data_processed/los_lagos_artesanal_only.csv (separate from backup)

library(DBI); library(duckdb)

base_dir <- sub("/new/?$", "", normalizePath(getwd(), winslash="/"))
raw_dir  <- paste0(base_dir, "/data_raw")
proc_dir <- paste0(base_dir, "/data_processed")
out_file <- paste0(proc_dir, "/los_lagos_artesanal_only.csv")
cat(sprintf("Base: %s\nRaw : %s\nOut : %s\n\n", base_dir, raw_dir, out_file))
cat("NOTE: los_lagos_filtered.csv (all vessels) kept as backup — not modified.\n\n")

## ── 1. Helper functions ───────────────────────────────────────────────────────
is_bad_file <- function(f) {
  tryCatch({
    sz <- file.info(f)$size
    if (is.na(sz) || sz < 50) return(TRUE)
    first <- readLines(f, n=1, warn=FALSE, encoding="latin1")
    grepl("^\\s*[<{!]", first)
  }, error=function(e) TRUE)
}

count_cols_semi <- function(f) {
  tryCatch({
    sz <- file.info(f)$size
    if (is.na(sz) || sz < 50) return(0L)
    lns <- readLines(f, n=3, warn=FALSE, encoding="latin1")
    if (length(lns) < 2) return(0L)
    if (grepl("^\\s*[<{!]", lns[1])) return(0L)
    length(strsplit(lns[2], ";", fixed=TRUE)[[1]])
  }, error=function(e) 0L)
}

to_sql_array <- function(paths) {
  quoted <- paste0("'", gsub("\\\\", "/", gsub("'", "''", paths)), "'")
  paste0("[", paste(quoted, collapse=","), "]")
}

## ── 2. File lists ─────────────────────────────────────────────────────────────
cat("[1] Scanning file lists...\n")
legacy_files <- list.files(raw_dir, full.names=TRUE,
  pattern="^artesanales_.*\\.csv$")
admin_files  <- list.files(raw_dir, full.names=TRUE,
  pattern="^report-\\d{4}-\\d{2}-\\d{2}_\\d{2}_\\d{2}_\\d{2}-sernapesca-admin\\.csv$")
flota_files  <- list.files(raw_dir, full.names=TRUE,
  pattern="^report_31_\\d{8}_flota_artesanal\\.csv$")

cat(sprintf("  Found: %d legacy, %d admin, %d flota\n",
            length(legacy_files), length(admin_files), length(flota_files)))

legacy_ok <- legacy_files[!sapply(legacy_files, is_bad_file)]
flota_ok  <- flota_files[!sapply(flota_files,  is_bad_file)]

cat("  Checking admin column counts (may take ~30s)...\n")
admin_ncols <- sapply(admin_files, count_cols_semi)
admin_ok    <- admin_files[admin_ncols >= 5]

cat(sprintf("  Valid: %d legacy, %d admin, %d flota\n",
            length(legacy_ok), length(admin_ok), length(flota_ok)))

## ── 3. DuckDB ─────────────────────────────────────────────────────────────────
db_file <- paste0(proc_dir, "/artesanal_tmp.duckdb")
if (file.exists(db_file)) file.remove(db_file)
con <- dbConnect(duckdb(), dbdir=db_file, read_only=FALSE)
on.exit({ dbDisconnect(con, shutdown=TRUE)
          unlink(db_file); unlink(paste0(db_file,".wal")) }, add=TRUE)
dbExecute(con, "SET threads = 4; SET memory_limit = '4GB';")

LAT_MIN <- -44.5; LAT_MAX <- -40.5
LON_MIN <- -75.5; LON_MAX <- -71.5

## ── 4a. artesanal_legacy (all kept — already artisanal) ───────────────────────
cat(sprintf("\n[2] artesanal_legacy (%d files)...\n", length(legacy_ok)))
if (length(legacy_ok) > 0) {
  q <- sprintf("
  CREATE OR REPLACE TABLE tbl_legacy AS
  SELECT
    trim(c0) AS movil,
    trim(c2) AS fecha,
    CASE
      WHEN regexp_matches(trim(c3), '[NSns]\\s*$')
      THEN (
        TRY_CAST(regexp_extract(trim(c3), '^(\\d+)', 1) AS DOUBLE)
        + COALESCE(TRY_CAST(regexp_extract(trim(c3), '[°º](\\d+)[^''\\d]', 1) AS DOUBLE),0.0)/60.0
        + COALESCE(TRY_CAST(regexp_extract(trim(c3), '''([0-9.]+)', 1) AS DOUBLE),0.0)/3600.0
      ) * -1.0
      ELSE TRY_CAST(regexp_replace(trim(c3),'[^0-9.\\-]','','g') AS DOUBLE)
    END AS latitud,
    CASE
      WHEN regexp_matches(trim(c4), '[WwOo]\\s*$')
      THEN (
        TRY_CAST(regexp_extract(trim(c4), '^(\\d+)', 1) AS DOUBLE)
        + COALESCE(TRY_CAST(regexp_extract(trim(c4), '[°º](\\d+)[^''\\d]', 1) AS DOUBLE),0.0)/60.0
        + COALESCE(TRY_CAST(regexp_extract(trim(c4), '''([0-9.]+)', 1) AS DOUBLE),0.0)/3600.0
      ) * -1.0
      ELSE TRY_CAST(regexp_replace(trim(c4),'[^0-9.\\-]','','g') AS DOUBLE)
    END AS longitud,
    TRY_CAST(replace(trim(c5),',','.') AS DOUBLE) AS rumbo,
    TRY_CAST(replace(trim(c6),',','.') AS DOUBLE) AS velocidad,
    regexp_replace(filename,'.*[/\\\\]','') AS source_file,
    'artesanal_legacy' AS category
  FROM read_csv(%s,
    delim=';', header=false, skip=1, all_varchar=true,
    ignore_errors=true, null_padding=true, filename=true,
    names=['c0','c1','c2','c3','c4','c5','c6'])
  WHERE latitud BETWEEN %s AND %s AND longitud BETWEEN %s AND %s
    AND latitud IS NOT NULL AND longitud IS NOT NULL",
  to_sql_array(legacy_ok), LAT_MIN, LAT_MAX, LON_MIN, LON_MAX)
  tryCatch({
    dbExecute(con, q)
    n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM tbl_legacy")$n
    cat(sprintf("  rows: %s\n", format(n, big.mark=",")))
  }, error=function(e) {
    cat("  ERROR:", e$message, "\n")
    dbExecute(con, "CREATE OR REPLACE TABLE tbl_legacy AS SELECT NULL::VARCHAR AS movil, NULL::VARCHAR AS fecha, NULL::DOUBLE AS latitud, NULL::DOUBLE AS longitud, NULL::DOUBLE AS rumbo, NULL::DOUBLE AS velocidad, NULL::VARCHAR AS source_file, 'artesanal_legacy'::VARCHAR AS category WHERE 1=0")
  })
} else {
  dbExecute(con, "CREATE OR REPLACE TABLE tbl_legacy AS SELECT NULL::VARCHAR AS movil, NULL::VARCHAR AS fecha, NULL::DOUBLE AS latitud, NULL::DOUBLE AS longitud, NULL::DOUBLE AS rumbo, NULL::DOUBLE AS velocidad, NULL::VARCHAR AS source_file, 'artesanal_legacy'::VARCHAR AS category WHERE 1=0")
}

## ── 4b. admin_2019 — ARTISANAL VESSELS ONLY: (ART) or (LB) suffix ────────────
cat(sprintf("\n[3] admin_2019 (%d files) — filtering to (ART) and (LB) vessels only...\n",
            length(admin_ok)))

dbExecute(con, "CREATE OR REPLACE TABLE tbl_admin (movil VARCHAR, fecha VARCHAR, latitud DOUBLE, longitud DOUBLE, rumbo DOUBLE, velocidad DOUBLE, source_file VARCHAR, category VARCHAR)")

n_ok <- 0L; n_skip <- 0L
for (i in seq_along(admin_ok)) {
  f <- admin_ok[[i]]
  if (i %% 500 == 0 || i == length(admin_ok))
    cat(sprintf("  file %d/%d  (inserted so far: %s)\n",
                i, length(admin_ok), format(n_ok, big.mark=",")))
  q <- sprintf("
  INSERT INTO tbl_admin
  SELECT
    trim(c0) AS movil,
    trim(c1) AS fecha,
    TRY_CAST(replace(trim(c3),',','.') AS DOUBLE) AS latitud,
    TRY_CAST(replace(trim(c4),',','.') AS DOUBLE) AS longitud,
    TRY_CAST(replace(trim(c5),',','.') AS DOUBLE) AS rumbo,
    TRY_CAST(replace(trim(c6),',','.') AS DOUBLE) AS velocidad,
    regexp_replace(filename,'.*[/\\\\]','') AS source_file,
    'admin_2019' AS category
  FROM read_csv(['%s'],
    delim=';', header=false, skip=1, all_varchar=true,
    ignore_errors=true, null_padding=true, filename=true,
    names=['c0','c1','c2','c3','c4','c5','c6'])
  WHERE TRY_CAST(replace(trim(c3),',','.') AS DOUBLE) BETWEEN %s AND %s
    AND TRY_CAST(replace(trim(c4),',','.') AS DOUBLE) BETWEEN %s AND %s
    AND c3 IS NOT NULL AND c4 IS NOT NULL
    AND (trim(c0) LIKE '%%(ART)%%' OR trim(c0) LIKE '%%(LB)%%')",
  gsub("'","''", gsub("\\\\","/", f)),
  LAT_MIN, LAT_MAX, LON_MIN, LON_MAX)
  tryCatch({ dbExecute(con, q); n_ok <- n_ok + 1L },
           error=function(e) { n_skip <<- n_skip + 1L })
}
n_admin <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM tbl_admin")$n
cat(sprintf("  total rows: %s  (files ok: %d, skipped: %d)\n",
            format(n_admin, big.mark=","), n_ok, n_skip))

## Show vessel breakdown for admin
adm_types <- tryCatch(dbGetQuery(con, "
  SELECT
    CASE WHEN movil LIKE '%(ART)%' THEN 'Artesanal (ART)'
         WHEN movil LIKE '%(LB)%'  THEN 'Lancha Bote (LB)'
         ELSE 'Other' END AS fleet_type,
    COUNT(*) AS pings, COUNT(DISTINCT movil) AS vessels
  FROM tbl_admin GROUP BY fleet_type ORDER BY fleet_type"), error=function(e) NULL)
if (!is.null(adm_types)) { cat("  Vessel types retained:\n"); print(adm_types) }

## ── 4c. flota_artesanal (all kept — already artisanal only) ──────────────────
batch_size <- 200
cat(sprintf("\n[4] flota_artesanal (%d files)...\n", length(flota_ok)))

flota_batches <- split(flota_ok, ceiling(seq_along(flota_ok)/batch_size))
dbExecute(con, "CREATE OR REPLACE TABLE tbl_flota (movil VARCHAR, fecha VARCHAR, latitud DOUBLE, longitud DOUBLE, rumbo DOUBLE, velocidad DOUBLE, source_file VARCHAR, category VARCHAR)")

for (i in seq_along(flota_batches)) {
  batch <- flota_batches[[i]]
  cat(sprintf("  batch %d/%d (%d files)...\n", i, length(flota_batches), length(batch)))
  q <- sprintf("
  INSERT INTO tbl_flota
  SELECT
    trim(c0) AS movil,
    trim(c2) AS fecha,
    TRY_CAST(regexp_replace(trim(c3),'[^0-9.\\-]','','g') AS DOUBLE) AS latitud,
    TRY_CAST(regexp_replace(trim(c4),'[^0-9.\\-]','','g') AS DOUBLE) AS longitud,
    TRY_CAST(replace(trim(c5),',','.') AS DOUBLE)                     AS rumbo,
    TRY_CAST(regexp_replace(trim(c6),'[^0-9.,]','','g') AS DOUBLE)   AS velocidad,
    regexp_replace(filename,'.*[/\\\\]','')                           AS source_file,
    'flota_artesanal'                                                  AS category
  FROM read_csv(%s,
    delim=';', header=false, skip=1, all_varchar=true,
    ignore_errors=true, null_padding=true, filename=true,
    strict_mode=false,
    names=['c0','c1','c2','c3','c4','c5','c6','c7','c8','c9','c10','c11','c12','c13','c14','c15'])
  WHERE TRY_CAST(regexp_replace(trim(c3),'[^0-9.\\-]','','g') AS DOUBLE) BETWEEN %s AND %s
    AND TRY_CAST(regexp_replace(trim(c4),'[^0-9.\\-]','','g') AS DOUBLE) BETWEEN %s AND %s
    AND c3 IS NOT NULL AND c4 IS NOT NULL",
  to_sql_array(batch), LAT_MIN, LAT_MAX, LON_MIN, LON_MAX)
  tryCatch(dbExecute(con, q), error=function(e) cat("  batch error:", e$message, "\n"))
}
n_flota <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM tbl_flota")$n
cat(sprintf("  total rows: %s\n", format(n_flota, big.mark=",")))

## ── 5. Write output ───────────────────────────────────────────────────────────
cat(sprintf("\n[5] Writing %s...\n", basename(out_file)))
sq_out <- gsub("'","''", out_file)

dbExecute(con, sprintf("
COPY (
  SELECT movil, fecha,
    ROUND(latitud,6)  AS latitud,
    ROUND(longitud,6) AS longitud,
    rumbo, velocidad, source_file, category
  FROM tbl_legacy
  UNION ALL
  SELECT movil, fecha,
    ROUND(latitud,6)  AS latitud,
    ROUND(longitud,6) AS longitud,
    rumbo, velocidad, source_file, category
  FROM tbl_admin
  UNION ALL
  SELECT movil, fecha,
    ROUND(latitud,6)  AS latitud,
    ROUND(longitud,6) AS longitud,
    rumbo, velocidad, source_file, category
  FROM tbl_flota
) TO '%s' (FORMAT CSV, HEADER TRUE, DELIMITER ',')", sq_out))

## ── 6. Summary ────────────────────────────────────────────────────────────────
n_leg <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM tbl_legacy")$n
total <- n_leg + n_admin + n_flota
cat(sprintf("\n========== DONE ==========\n"))
cat(sprintf("  artesanal_legacy : %s rows\n", format(n_leg,   big.mark=",")))
cat(sprintf("  admin_2019 (ART+LB only): %s rows\n", format(n_admin, big.mark=",")))
cat(sprintf("  flota_artesanal  : %s rows\n", format(n_flota, big.mark=",")))
cat(sprintf("  TOTAL            : %s rows\n", format(total,   big.mark=",")))
cat(sprintf("  Output: %s\n", out_file))
cat(sprintf("  Backup: %s/los_lagos_filtered.csv (untouched)\n\n", proc_dir))

## ── 7. Quick year × category check ────────────────────────────────────────────
cat("[7] Year distribution check:\n")
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

chk <- dbGetQuery(con, sprintf("
  SELECT (%s) AS yr, category,
    COUNT(*) AS pings, COUNT(DISTINCT movil) AS vessels
  FROM (
    SELECT fecha, movil, source_file, category FROM tbl_legacy UNION ALL
    SELECT fecha, movil, source_file, category FROM tbl_admin  UNION ALL
    SELECT fecha, movil, source_file, category FROM tbl_flota
  )
  WHERE (%s) BETWEEN 2019 AND 2026
  GROUP BY yr, category ORDER BY yr, category", year_expr, year_expr))
print(chk)
cat("\nExpected: ~80-100 ART/LB vessels in admin_2019 (2019-2022)\n")
cat("         ~134-190 vessels in flota_artesanal (2022-2026)\n")
cat("These should now be comparable in scale.\n")
