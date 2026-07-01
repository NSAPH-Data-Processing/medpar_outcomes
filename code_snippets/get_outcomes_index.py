import duckdb
import argparse
import logging
import pandas as pd

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main(args):

    conn = duckdb.connect(f"duckpond_{args.outcome}.db")

    LOGGER.info("Creating outcome_admissions table-----")
    query = f"""
        WITH outcomes AS (
            SELECT  bene_id, adm_id, outcome
            FROM '{args.outcomes_prefix}_*.parquet'
            WHERE outcome = '{args.outcome}'
        )
        SELECT 
            o.bene_id, 
            o.adm_id, 
            o.outcome,
            a.admission_date
        FROM '{args.adm_prefix}_*.parquet' AS a
        INNER JOIN outcomes AS o
        ON a.adm_id = o.adm_id  
    """

    conn.execute(f"""
        CREATE OR REPLACE TABLE outcome_admissions AS {query}
    """)

    LOGGER.info(f"outcome_admissions num_rows: {conn.execute('SELECT COUNT(*) AS num_rows FROM outcome_admissions').fetchone()}")

    LOGGER.info("Creating outcome_index table-----")

    query = """
        SELECT 
            bene_id,
            adm_id,
            outcome,
            admission_date,
            ROW_NUMBER() OVER (PARTITION by bene_id ORDER BY admission_date) as outcome_index
        FROM outcome_admissions
    """

    conn.execute(f"CREATE OR REPLACE TABLE outcome_index AS {query}")

    LOGGER.info("Exloring Data:")
    LOGGER.info(f"Num Bene: {conn.execute('SELECT COUNT(DISTINCT bene_id) AS num_of_bene FROM outcome_index').fetchdf()}")
    LOGGER.info(f"Min Admission Date : {conn.execute('SELECT MIN(admission_date) AS min_date FROM outcome_index').fetchdf()}")
    LOGGER.info(f"Max Admission Date : {conn.execute('SELECT MAX(admission_date) AS max_date FROM outcome_index').fetchdf()}")

    LOGGER.info("Incorporate discharge date")
    query = f"""
        SELECT 
            o.*,
            a.discharge_date,
            EXTRACT(YEAR FROM a.discharge_date) AS discharge_year
        FROM outcome_index AS o
        INNER JOIN '{args.adm_prefix}_*.parquet' AS a
        ON o.adm_id = a.adm_id
        ORDER BY o.bene_id, o.outcome_index
    """
    conn.execute(f"CREATE OR REPLACE TABLE outcome_index AS {query}")

    LOGGER.info(f"outcome_index num_rows: {conn.execute('SELECT COUNT(*) AS num_rows FROM outcome_index').fetchone()}")
    LOGGER.info(f"outcome_index head: {conn.execute('SELECT * FROM outcome_index LIMIT 5').fetchdf()}")
    LOGGER.info("Fetching distinct years from outcomes data...")
    LOGGER.info("# identify the unique years ----")
    years = conn.execute(f"""SELECT DISTINCT year FROM '{args.outcomes_prefix}_*.parquet' ORDER BY year """).fetchnumpy()
 
    LOGGER.info("Saving output year-wise----")
    for year in years['year']:
        conn.execute(f""" COPY (
            SELECT 
                o.*,
                oi.discharge_year,
                oi.admission_date,
                oi.discharge_date,
                oi.outcome_index
            FROM '{args.outcomes_prefix}_{year}.parquet' AS o
            INNER JOIN outcome_index AS oi
            ON 
                o.bene_id = oi.bene_id AND
                o.adm_id = oi.adm_id AND 
                o.outcome = oi.outcome
            WHERE o.year = {year}
        ) TO '{args.output_prefix}_{year}.{args.output_format}'
        """)

    LOGGER.info(f"# saved outcomes file at {args.output_prefix}_{year}.parquet ----")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outcomes_prefix", 
                        default="./data/intermediate/outcomes"
                        )
    parser.add_argument("--adm_prefix", 
                        default = "./data/input/mbsf_medpar_denom/inpatient"
                       )
    parser.add_argument("--output_format", 
                        default = "parquet", 
                        choices=["parquet", "feather", "csv"]
                       )         
    parser.add_argument("--outcome",
                        default = "dengue"
                        )  
    parser.add_argument("--output_prefix", 
                    default = "./data/output/medpar_outcomes/michelle_tian_00/dengue"
                       )
    args = parser.parse_args()

    main(args)
