# Install required packages 
#install.packages("duckdb")
#install.packages("glue")

# Load required libraries
library(duckdb)
library(glue)

args <- commandArgs(trailingOnly = TRUE)

# prefixes 
inpatient_prefix <- "data/input/mbsf_medpar_denom/inpatient"
outcome_prefix <- "data/output/medpar_outcomes/michelle_garam_00/adrd"
output_prefix <- "data/output/medpar_outcomes/michelle_garam_00/hosp_subset_adrd"

if (length(args) >= 1) inpatient_prefix <- args[1]
if (length(args) >= 2) outcome_prefix <- args[2]
if (length(args) >= 3) output_prefix <- args[3]

# Main function
main <- function(inpatient_prefix, outcome_prefix, output_prefix) {
  # Connect to DuckDB 
  con <- dbConnect(duckdb::duckdb(), dbdir = "duckpond.db", read_only = FALSE)
  query <- glue("
    COPY (
      SELECT inpatient.*
      FROM '{inpatient_prefix}_*.parquet' AS inpatient
      INNER JOIN (
        SELECT DISTINCT bene_id
        FROM '{outcome_prefix}_*.parquet'
      ) AS outcome
      ON inpatient.bene_id = outcome.bene_id
    ) TO '{output_prefix}.parquet'
  ")
  
  # Execute the query and save the result
  tryCatch({
    dbExecute(con, query)
    message(sprintf("Filtered data saved at: %s.parquet", output_prefix))
  }, error = function(e) {
    stop("Error during query execution: ", e$message)
  }, finally = {
    dbDisconnect(con, shutdown = TRUE)
  })
}

# Execute the main function
main(inpatient_prefix, outcome_prefix, output_prefix)