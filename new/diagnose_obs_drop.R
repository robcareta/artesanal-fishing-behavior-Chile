## diagnose_obs_drop.R
## Investigates why observations drop dramatically from 2023 onward.
## Three hypotheses:
##   H1: Source switch — admin_2019 (4x/day, all vessels) replaced by
##       flota_artesanal (1x/day, artisanal only)
##   H2: Vessel count drop — fewer vessels reporting post-2022
##   H3: Ping frequency drop — same vessels but less frequent reports

library(DBI); library(duckdb); library(data.table)

base_dir <- sub("/new/?$", "", normalizePath(getwd(), winslash="/"))
csv_path <- normalizePath(file.path(base_dir, "data_processed/los_lagos_filtered.csv"),
                          winslash="/", mustWork=TRUE)
raw_dir  <- normalizePath(file.path(base_dir, "data_raw"), winslash="/")

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

cat("================================================================\n")
cat(" DIAGNOSTIC: Why do observations drop from 2023 onward?\n")
cat("================================================================\n\n")

## ── H1: Source composition by year ───────────────────────────────────────────
cat("--- H1: Source composition by year ---\n")
h1 <- as.data.table(dbGetQuery(con, sprintf("
  SELECT (%s) AS yr, category,
    COUNT(*) AS pings,
    COUNT(DISTINCT movil) AS vessels,
    COUNT(DISTINCT SUBSTR(source_file,1,30)) AS distinct_files
  FROM %s
  WHERE (%s) BETWEEN 2019 AND 2026
  GROUP BY yr, category
  ORDER BY yr, category", year_expr, rc, year_expr)))
print(h1)

cat("\nKEY: admin_2019 = snapshot reports taken 4x/day for ALL vessel types\n")
cat("     flota_artesanal = daily reports for ARTISANAL vessels only\n\n")

## ── H2: Ping frequency (pings per vessel per day) ────────────────────────────
cat("--- H2: Ping frequency (pings per vessel per active day) ---\n")
h2 <- as.data.table(dbGetQuery(con, sprintf("
  SELECT (%s) AS yr, category,
    COUNT(*) AS total_pings,
    COUNT(DISTINCT movil) AS vessels,
    COUNT(DISTINCT source_file) AS n_files,
    ROUND(COUNT(*)*1.0 / NULLIF(COUNT(DISTINCT source_file),0), 1) AS pings_per_file,
    ROUND(COUNT(*)*1.0 / NULLIF(COUNT(DISTINCT movil),0) /
          NULLIF(COUNT(DISTINCT source_file),0), 2) AS pings_per_vessel_per_file
  FROM %s
  WHERE (%s) BETWEEN 2019 AND 2026
  GROUP BY yr, category
  ORDER BY yr, category", year_expr, rc, year_expr)))
print(h2)

## ── H3: Files per day (how many snapshot files per calendar date?) ────────────
cat("\n--- H3: Admin files per calendar day (snapshot frequency) ---\n")
h3 <- as.data.table(dbGetQuery(con, sprintf("
  SELECT
    SUBSTR(source_file, 8, 4) AS yr,
    SUBSTR(source_file, 8, 7) AS ym,
    COUNT(DISTINCT source_file) AS files,
    COUNT(DISTINCT SUBSTR(source_file, 8, 10)) AS days,
    ROUND(COUNT(DISTINCT source_file)*1.0 /
          NULLIF(COUNT(DISTINCT SUBSTR(source_file,8,10)),0),1) AS files_per_day
  FROM %s
  WHERE source_file LIKE 'report-%%'
    AND SUBSTR(source_file, 8, 4) BETWEEN '2019' AND '2026'
  GROUP BY yr, ym
  ORDER BY ym", rc)))
cat("Admin snapshot files per month (should be ~4/day if 4 snapshots/day):\n")
print(h3[, .(yr, ym, days, files, files_per_day)])

## ── H4: Do admin files include non-artisanal vessels? ────────────────────────
cat("\n--- H4: Admin files — vessel name suffixes (fleet type indicators) ---\n")
h4 <- as.data.table(dbGetQuery(con, sprintf("
  SELECT
    CASE
      WHEN movil LIKE '%%(ART)%%' THEN 'Artesanal (ART)'
      WHEN movil LIKE '%%(IND)%%' THEN 'Industrial (IND)'
      WHEN movil LIKE '%%(TRP)%%' THEN 'Transport (TRP)'
      WHEN movil LIKE '%%(LB)%%'  THEN 'Lancha Bote (LB)'
      WHEN movil LIKE '%%(AC)%%'  THEN 'Acuicultura (AC)'
      ELSE 'No suffix / Other'
    END AS fleet_type,
    category,
    (%s) AS yr,
    COUNT(*) AS pings,
    COUNT(DISTINCT movil) AS vessels
  FROM %s
  WHERE (%s) BETWEEN 2019 AND 2026
  GROUP BY fleet_type, category, yr
  ORDER BY yr, category, fleet_type", year_expr, rc, year_expr)))
print(h4)

## ── Summary ──────────────────────────────────────────────────────────────────
cat("\n================================================================\n")
cat(" SUMMARY\n")
cat("================================================================\n")

tot <- as.data.table(dbGetQuery(con, sprintf("
  SELECT (%s) AS yr,
    SUM(CASE WHEN category='admin_2019' THEN 1 ELSE 0 END) AS admin_pings,
    SUM(CASE WHEN category='flota_artesanal' THEN 1 ELSE 0 END) AS flota_pings,
    COUNT(*) AS total_pings,
    COUNT(DISTINCT CASE WHEN category='admin_2019' THEN movil END) AS admin_vessels,
    COUNT(DISTINCT CASE WHEN category='flota_artesanal' THEN movil END) AS flota_vessels
  FROM %s
  WHERE (%s) BETWEEN 2019 AND 2026
  GROUP BY yr ORDER BY yr", year_expr, rc, year_expr)))
print(tot)

cat("\nINTERPRETATION:\n")
cat("If admin_pings >> flota_pings in 2019-2022 and admin_pings=0 in 2023+,\n")
cat("the drop is a DATA SOURCE CHANGE, not a real decline in fishing activity.\n")
cat("The admin_2019 source includes ALL vessel types 4x/day;\n")
cat("the flota_artesanal source includes ARTISANAL vessels only 1x/day.\n")
cat("\nFor the aquaculture-fisheries interaction analysis,\n")
cat("RECOMMENDATION: Use only flota_artesanal source for consistent comparison,\n")
cat("OR restrict admin_2019 to vessels with (ART) suffix only.\n")
