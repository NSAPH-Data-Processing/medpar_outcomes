import duckdb
import argparse
import logging
import pandas as pd

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_admission_index(outcomes_prefix, adm_prefix, outcome):
    query = f"""
        WITH outcomes AS (
            SELECT DISTINCT bene_id, adm_id, outcome
            FROM '{outcomes_prefix}_*.parquet'
        ),
        admissions AS (
            SELECT bene_id, adm_id, admission_date, discharge_date,EXTRACT(YEAR FROM admission_date) AS year
            FROM '{adm_prefix}_*.parquet'
            WHERE bene_id IN (SELECT DISTINCT bene_id FROM outcomes)
        ),
        hosp_index AS (
            SELECT 
                a.bene_id,
                a.adm_id,
                o.outcome,
                a.admission_date,
                a.discharge_date,
                a.year,
                ROW_NUMBER() OVER (PARTITION BY a.bene_id ORDER BY a.admission_date) AS hosp_index,
            FROM admissions AS a
            LEFT JOIN outcomes AS o 
            ON a.bene_id = o.bene_id AND a.adm_id = o.adm_id
        ),
        outcome_index AS (
            SELECT 
                bene_id,
                adm_id,
                outcome,
                admission_date,
                discharge_date,
                year,
                hosp_index,
                CASE 
                    WHEN outcome = '{outcome}' THEN 
                        ROW_NUMBER() OVER (PARTITION by bene_id ORDER BY admission_date)
                    ELSE NULL
                END AS outcome_index
            FROM hosp_index
            WHERE outcome = '{outcome}'
        )
        SELECT 
            bene_id,
            adm_id,
            admission_date as adm_date,
            discharge_date,
            year,
            outcome,
            hosp_index,
            outcome_index
        FROM outcome_index
        ORDER BY bene_id, adm_date;

    """
    return query 

def save_by_year(df, output_prefix, output_format):
    years = df['year'].unique()

    for year in years:
        df_year = df[df['year'] == year]
        output_file = f"{output_prefix}_{year}.{output_format}"
        LOGGER.info(f"Saving data for year {year} to {output_file}")

        LOGGER.info(f"Year : {year} | df_year shape : {df_year.shape}")
        LOGGER.info(f"Year : {year} | df_year head:\n{df_year.head()}")

        if output_format == "parquet":
            df_year.to_parquet(output_file)
        elif output_format == "feather":
            df_year.to_feather(output_file)
        elif output_format == "csv":
            df_year.to_csv(output_file)

        LOGGER.info(f"Data for year {year} saved to {output_file}")

def main(args):

    conn = duckdb.connect()

    LOGGER.info("Creating admission_index table-----")
    query = get_admission_index(args.outcomes_prefix, args.adm_prefix, args.outcome)
    LOGGER.info(f"Executing the query : {query}")

    conn.execute(f"CREATE TEMPORARY TABLE outcome_index AS {query}")

    LOGGER.info("Exloring Data:")

    LOGGER.info(f"Num Bene: {conn.execute('SELECT COUNT(DISTINCT bene_id) AS num_of_bene FROM outcome_index').fetchdf()}")
    LOGGER.info(f"Min Admission Date : {conn.execute('SELECT MIN(adm_date) AS min_date FROM outcome_index').fetchdf()}")
    LOGGER.info(f"Max Admission Date : {conn.execute('SELECT MAX(adm_date) AS max_date FROM outcome_index').fetchdf()}")
    
    df = conn.execute(query).fetchdf()
    LOGGER.info(f"df shape: {df.shape}")
    LOGGER.info(f"df head: {df.head()}")

    LOGGER.info("Saving output year-wise----")
    save_by_year(df, args.output_prefix, args.output_format)


    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outcomes_prefix", 
                        default="data/output/legacy/outcomes"
                        )
    parser.add_argument("--adm_prefix", 
                        default = "./data/input/dw_legacy_medicare_00_16/adm"
                       )
    parser.add_argument("--outcome",
                        default = "adrd"
                        )
    parser.add_argument("--output_format", 
                        default = "parquet", 
                        choices=["parquet", "feather", "csv"]
                       )           
    parser.add_argument("--output_prefix", 
                    default = "./data/output/legacy/outcome_index"
                       )
    args = parser.parse_args()
    
    main(args)
