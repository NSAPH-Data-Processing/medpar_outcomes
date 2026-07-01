import pandas as pd 
import pyarrow 
import duckdb
import argparse 
import yaml 
import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main(args):
    # Load ICD config
    with open(args.icd_yml, 'r') as f:
        icd_dict = yaml.safe_load(f)

    # Check if any outcome has readmission window
    if not any("readmission_window_days" in v for v in icd_dict.values()):
        LOGGER.info("No outcomes require readmission filtering. Exiting.")
        return

    for outcome in icd_dict.keys():
        readmission_window = icd_dict[outcome].get("readmission_window_days")

        LOGGER.info(f"Applying readmission filter (≥ {readmission_window} days) for {outcome}")
        conn = duckdb.connect(f"duckpond_{outcome}.db")

        # Load all years of outcome data
        conn.execute(f"""
            CREATE OR REPLACE TABLE outcomes AS 
            SELECT * 
            FROM '{args.input_prefix}{outcome}_*.parquet'
        """)

        total_outcomes_count = conn.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0]

        # Apply readmission filter
        readmission_query = f"""
        WITH previous_discharge AS (
            SELECT *,
                LAG(discharge_date) OVER (PARTITION BY bene_id ORDER BY admission_date) AS prev_discharge
            FROM outcomes
        ),
        admissions AS (
            SELECT *
            FROM previous_discharge
            WHERE prev_discharge IS NULL
                OR DATE_DIFF('day', prev_discharge, admission_date) >= {readmission_window}
        )
        SELECT * EXCLUDE prev_discharge
        FROM admissions
        """
        conn.execute(f"CREATE OR REPLACE TABLE outcomes_filtered AS {readmission_query}")

         # Count after filtering
        filtered_row_count = conn.execute("SELECT COUNT(*) FROM outcomes_filtered").fetchone()[0]
        rows_dropped = total_outcomes_count - filtered_row_count

        LOGGER.info(f"Total rows: {total_outcomes_count}")
        LOGGER.info(f"Filtered rows: {filtered_row_count}")
        LOGGER.info(f"Rows dropped due to readmission filter: {rows_dropped}")
        preview = conn.execute("SELECT * FROM outcomes_filtered LIMIT 5").fetchdf()
        LOGGER.info(f"Preview:\n{preview}")

        # Save filtered results year-wise
        years = conn.execute("SELECT DISTINCT year FROM outcomes_filtered ORDER BY year").fetchnumpy()
        for year in years['year']:
            output_path = f"{args.output_prefix}{outcome}_{year}.parquet"
            conn.execute(f"""
                COPY (
                    SELECT * FROM outcomes_filtered WHERE year = {year}
                ) TO '{output_path}'
            """)
            row_count = conn.execute(f"SELECT count(*) FROM read_parquet('{output_path}')").fetchone()[0]
            LOGGER.info(f"Saved----")

        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_prefix", default="./data/output/medpar_outcomes/michelle_tian_00/")
    parser.add_argument("--icd_yml", default="./conf/icd_codes/michelle_tian_00.yml")
    parser.add_argument("--output_prefix", default="./data/output/medpar_outcomes/michelle_tian_00/")
    args = parser.parse_args()

    main(args)