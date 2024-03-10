import duckdb
import argparse
import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def get_query(outcomes_prefix):
    ## Preparing query----
    query = f"""
            SELECT
                year,
                outcome,
                COUNT(adm_id) AS n
            FROM
                '{outcomes_prefix}_*.parquet'
            GROUP BY
                year, outcome
    """     
    return query

def main(args):
    conn = duckdb.connect()
    
    query = get_query(args.outcomes_prefix)
    
    df = conn.execute(query).fetchdf()
    df = df.pivot(index='year', columns='outcome', values='n')
    LOGGER.info(f"df shape: {df.shape}")
    LOGGER.info(f"df head: {df.head()}")

    LOGGER.info("## Writing counts denom ----")
    output_file = f"{args.output_prefix}.{args.output_format}"
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
    parser.add_argument("--outcomes_prefix", 
                        default = "./data/output/medpar_outcomes/icd_codes_6/outcomes"
                       )
    parser.add_argument("--output_format", 
                        default = "csv", 
                        choices=["parquet", "feather", "csv"]
                       )   
    parser.add_argument("--output_prefix", 
                    default = "./data/output/medpar_outcomes/icd_codes_6/outcome_counts"
                   )
    args = parser.parse_args()
    
    main(args)