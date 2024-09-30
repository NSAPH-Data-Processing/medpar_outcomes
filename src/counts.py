import duckdb
import argparse
import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_query(denom_prefix, outcomes_prefix, year):
    ## Preparing query----
    query = f"""
        WITH outcomes AS (
            SELECT
                o.bene_id,
                o.year,
                d.zcta,
                o.outcome
            FROM
                '{outcomes_prefix}_{year}.parquet' AS o
            INNER JOIN
                '{denom_prefix}_{year}.parquet' AS d
            ON
                o.bene_id = d.bene_id AND
                o.year = d.year
        )
        SELECT
            zcta,
            year,
            outcome,
            COUNT(*) AS n_outcomes
            FROM 
                outcomes
            GROUP BY 
                zcta, year, outcome
    """     
    return query

def main(args):
    conn = duckdb.connect()
    
    LOGGER.info(f"preparing counts ----")
    query = get_query(args.denom_prefix, args.outcomes_prefix, args.year)
    conn.execute(f"""
        CREATE TABLE zcta_counts AS
        {query}
    """)
    # TODO: fill with zero's all none observed year, zcta combinations
    zcta_counts = conn.table("zcta_counts")

    LOGGER.info(f"df shape: {zcta_counts.count('*').fetchone()}")
    LOGGER.info(f"df head: {zcta_counts.limit(5).fetchdf()}")

    LOGGER.info("## Writing counts denom ----")
    output_file = f"{args.output_prefix}_{args.year}.parquet"
    zcta_counts.write_parquet(output_file)
    LOGGER.info(f"## Output file written to {output_file}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", 
                        default = 2015, 
                        type=int
                       )
    parser.add_argument("--denom_prefix", 
                        default = "data/input/mbsf_medpar_denom/mbsf_medpar_denom" # "data/input/dw_legacy_medicare_00_16/bene"
                       ) 
    parser.add_argument("--outcomes_prefix", 
                        default = "data/output/medpar_outcomes/icd_codes_8/outcomes"
                       )  
    parser.add_argument("--output_prefix", 
                    default = "data/output/medpar_outcomes/icd_codes_8/zcta_yearly/counts"
                   )
    args = parser.parse_args()
    
    main(args)
