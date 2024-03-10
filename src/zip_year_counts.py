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
                d.zip,
                o.outcome
            FROM
                '{outcomes_prefix}_{year}.parquet' AS o
            INNER JOIN
                '{denom_prefix}_{year}.parquet' AS d
            ON
                o.bene_id = d.bene_id
        )
        SELECT
            zip,
            year,
            outcome,
            COUNT(*) AS n
            FROM 
                outcomes
            GROUP BY 
                zip, year
    """     
    return query

def main(args):
    conn = duckdb.connect()
    
    LOGGER.info(f"preparing counts ----")
    query = get_query(args.denom_prefix, args.outcomes_prefix, args.year)
    df = conn.execute(query).fetchdf()
    df = df.pivot(index=['year', 'zip'], columns='outcome', values='n')
    LOGGER.info(f"{outcome} shape: {df.shape}")
    LOGGER.info(f"{outcome} head: {df.head()}")
    
    #df.set_index

    ## join all outcomes

    LOGGER.info("## Writing counts denom ----")
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
                        default = 2010, 
                        type=int
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
                    default = "./data/output/medpar_medpar_outcomes_denom/icd_codes_6/zip_year_counts"
                   )
    args = parser.parse_args()
    
    main(args)