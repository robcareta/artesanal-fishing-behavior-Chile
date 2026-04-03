## inspect_fishermen.R — explore FISHERMEN_DATA.xlsx structure
library(readxl)

xl <- "C:/Users/rober/UF Dropbox/Roberto Cardenas Retamal/PHD/PAPERS/10. AQUACULTURE AND FISHERIES INTERACTION CHILE/Data/FISHERMEN_DATA.xlsx"
sheets <- excel_sheets(xl)
cat("Sheets:", paste(sheets, collapse=", "), "\n\n")

for (sh in sheets) {
  df <- read_excel(xl, sheet=sh, n_max=3)
  cat(sprintf("--- Sheet: %s (%d cols) ---\n", sh, ncol(df)))
  cat(paste(names(df), collapse=" | "), "\n")
  print(df[1:min(2,nrow(df)), ])
  cat("\n")
}
