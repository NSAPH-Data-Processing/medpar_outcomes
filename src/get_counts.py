import pandas as pd
import duckdb
import argparse
import logging
import yaml

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_counts_query(denom_prefix, outcomes_prefix, outcome, year):
    ## Preparing query----
    query = f"""
        WITH outcome_counts AS (
            SELECT
                bene_id,
                COUNT(adm_id) AS outcomes
            FROM
                '{outcomes_prefix}_{year}.parquet'
            WHERE
                outcome = '{outcome}'
            GROUP BY
                bene_id
        ),
        denom AS (
            SELECT
                b.bene_id,
                b.zip,
                b.sex,
                b.dual,
                b.age_dob AS age,
                CASE
                    WHEN EXTRACT(YEAR FROM b.dod) == {year} THEN 1 
                    ELSE 0
                END AS dead,
                b.race,
                o.outcomes
            FROM
                '{denom_prefix}_{year}.parquet' b
            LEFT JOIN
                outcome_counts o
            ON
                b.bene_id = o.bene_id
            WHERE
                b.hmo_mo = 0
        )
        SELECT
            zip,
            COUNT(*) AS n_bene_ffs,
            SUM(dead) / COUNT(*) * 100 AS death_rate,
            AVG(CASE WHEN sex = 2 THEN 1.0 ELSE 0 END) * 100 AS female_percentage,
            AVG(CASE WHEN dual = 1 THEN 1.0 ELSE 0 END) * 100 AS dual_percentage,
            AVG(age) AS mean_age,
            AVG(CASE WHEN race = 4 THEN 1.0 ELSE 0 END) * 100 AS percentage_race_labelAsian,
            AVG(CASE WHEN race = 2 THEN 1.0 ELSE 0 END) * 100 AS percentage_race_labelBlack,
            AVG(CASE WHEN race = 3 THEN 1.0 ELSE 0 END) * 100 AS percentage_race_labelOther,
            AVG(CASE WHEN race = 1 THEN 1.0 ELSE 0 END) * 100 AS percentage_race_labelWhite,
            AVG(CASE WHEN race = 5 THEN 1.0 ELSE 0 END) * 100 AS percentage_race_labelHispanic,
            AVG(CASE WHEN race = 6 THEN 1.0 ELSE 0 END) * 100 AS percentage_race_labelNorth_American_Native, 
            SUM(outcomes) / COUNT(*) * 100 AS {outcome}_rate
            FROM denom
            GROUP BY zip
    """     
    return query

def main(args):
    conn = duckdb.connect()
    
    with open(args.icd_yml, 'r') as f:
        icd_dict = yaml.load(f, Loader=yaml.FullLoader)
    
    outcomes = list(icd_dict.keys())
    outcome_df_list = []
    for outcome in outcomes:
        LOGGER.info(f"preparing {outcome} ----")
        query = get_counts_query(args.denom_prefix, args.outcomes_prefix, outcome, args.year)
        df = conn.execute(query).fetchdf()
        LOGGER.info(f"{outcome} shape: {df.shape}")
        LOGGER.info(f"{outcome} head: {df.head()}")
        df = df.set_index([
            'zip', 
            'n_bene_ffs',
            'death_rate',
            'female_percentage',
            'dual_percentage',
            'mean_age',
            'percentage_race_labelAsian',
            'percentage_race_labelBlack',
            'percentage_race_labelOther',
            'percentage_race_labelWhite',
            'percentage_race_labelHispanic',
            'percentage_race_labelNorth_American_Native'
        ]) 
        outcome_df_list.append(df)

    ## join all outcomes
    df = outcome_df_list[0]
    for outcome_df in outcome_df_list[1:]:
        df = df.join(outcome_df)

    LOGGER.info("## Writing coutns denom ----")
    output_file = f"{args.output_prefix}_{args.year}.{args.output_format}"
    if args.output_format == "parquet":
        df.to_parquet(output_file)
    elif args.output_format == "feather":
        df.to_feather(output_file)
    elif args.output_format == "csv":
        df.to_csv(output_file)

    LOGGER.info(f"## Output file written to {output_file}")

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", 
                        default = 2000, 
                        type=int
                       )
    parser.add_argument("--icd_yml",
                        default = "./conf/icd_codes/icd_codes_6.yml"
                        )
    parser.add_argument("--denom_prefix", 
                        default = "./data/input/dw_legacy_medicare_00_16/bene"
                       ) 
    parser.add_argument("--outcomes_prefix", 
                        default = "./data/output/medpar_outcomes/icd_codes_6/outcomes"
                       )
    parser.add_argument("--output_format", 
                        default = "csv", 
                        choices=["parquet", "feather", "csv"]
                       )   
    parser.add_argument("--output_prefix", 
                    default = "./data/output/medpar_outcomes/output_counts"
                   )
    args = parser.parse_args()
    
    main(args)