import duckdb
import argparse
import logging
import pandas as pd

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_outcome_index(outcomes_prefix, adm_prefix, outcome):
    query = f"""
        WITH outcomes AS (
            SELECT  bene_id, adm_id, outcome
            FROM '{outcomes_prefix}_*.parquet'
            WHERE outcome = '{outcome}'
        ),
        admissions AS (
            SELECT 
                o.bene_id, 
                o.adm_id, 
                o.outcome,
                a.admission_date
            FROM '{adm_prefix}_*.parquet' AS a
            INNER JOIN outcomes AS o
            ON a.adm_id = o.adm_id
        )
        SELECT 
            bene_id,
            adm_id,
            outcome,
            admission_date,
            ROW_NUMBER() OVER (PARTITION by bene_id ORDER BY admission_date) as outcome_index
        FROM admissions
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
            df_year.to_parquet(output_file, index = False)
        elif output_format == "feather":
            df_year.to_feather(output_file)
        elif output_format == "csv":
            df_year.to_csv(output_file, index = False)

        LOGGER.info(f"Data for year {year} saved to {output_file}")

def main(args):

    conn = duckdb.connect()

    LOGGER.info("Creating outcome_index table-----")
    query = get_outcome_index(args.outcomes_prefix, args.adm_prefix, args.outcome)
    LOGGER.info(f"Executing the query : {query}")

    conn.execute(f"CREATE TABLE outcome_index AS {query}")

    LOGGER.info("Exloring Data:")

    LOGGER.info(f"Num Bene: {conn.execute('SELECT COUNT(DISTINCT bene_id) AS num_of_bene FROM outcome_index').fetchdf()}")
    LOGGER.info(f"Min Admission Date : {conn.execute('SELECT MIN(adm_date) AS min_date FROM outcome_index').fetchdf()}")
    LOGGER.info(f"Max Admission Date : {conn.execute('SELECT MAX(adm_date) AS max_date FROM outcome_index').fetchdf()}")

    #df = conn.execute(query).fetchdf()
    #LOGGER.info(f"df shape: {df.shape}")
    #LOGGER.info(f"df head: {df.head()}")

    #LOGGER.info("Saving output year-wise----")
    #save_by_year(df, args.output_prefix, args.output_format)


    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outcomes_prefix", 
                        default="./data/output/medpar_outcomes/icd_codes_11/outcomes"
                        )
    parser.add_argument("--adm_prefix", 
                        default = "./data/input/dw_legacy_medicare_00_16/adm"
                       )
    parser.add_argument("--output_format", 
                        default = "parquet", 
                        choices=["parquet", "feather", "csv"]
                       )         
    parser.add_argument("--outcome",
                        default = "cvd"
                        )  
    parser.add_argument("--output_prefix", 
                    default = "./data/output/test/outcome_index"
                       )
    args = parser.parse_args()

    main(args)
