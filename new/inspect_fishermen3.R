library(readxl); library(data.table)
xl <- "C:/Users/rober/UF Dropbox/Roberto Cardenas Retamal/PHD/PAPERS/10. AQUACULTURE AND FISHERIES INTERACTION CHILE/Data/FISHERMEN_DATA.xlsx"

## Read 2019 with correct skip
df <- as.data.table(read_excel(xl, sheet="2019", skip=3))
cat("Columns:\n"); print(names(df))
cat("\nFirst 3 rows:\n"); print(head(df, 3))
cat("\nÚnique values of 'Región de Captura':\n")
print(sort(unique(df[["Región de Captura"]])))

## Filter Los Lagos (region 10)
ll <- df[`Región de Captura` == 10]
cat(sprintf("\nLos Lagos rows in 2019: %d\n", nrow(ll)))
cat("\nColumn with vessel name (search for 'Embarcación'):\n")
print(names(df)[grepl("mbarcaci", names(df), ignore.case=TRUE)])
cat("\nSample Embarcación values:\n")
emb_col <- names(df)[grepl("mbarcaci", names(df), ignore.case=TRUE)][1]
print(head(ll[[emb_col]], 20))

## Species column
cat("\nSpecies-like columns:\n")
print(names(df)[grepl("speci|Espec", names(df), ignore.case=TRUE)])

## Catch/tons columns
cat("\nAll column names:\n"); print(names(df))
