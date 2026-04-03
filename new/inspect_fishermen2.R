library(readxl)
xl <- "C:/Users/rober/UF Dropbox/Roberto Cardenas Retamal/PHD/PAPERS/10. AQUACULTURE AND FISHERIES INTERACTION CHILE/Data/FISHERMEN_DATA.xlsx"
df <- read_excel(xl, sheet="2019", n_max=8, col_names=FALSE)
for(i in 1:nrow(df)) {
  vals <- paste(unlist(df[i, 1:min(10, ncol(df))]), collapse=" | ")
  cat(sprintf("Row %d: %s\n", i, vals))
}
