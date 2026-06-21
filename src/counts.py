import duckdb
import argparse
import logging
import yaml

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# counts at the admission-zcta level

def get_zcta_yearly_counts(denom_prefix, outcome_prefix, outcome_name, year, unique_zcta_prefix):
    query = f"""
        CREATE OR REPLACE TABLE zcta_counts_{outcome_name} AS 
        WITH outcomes AS (
            SELECT 
                o.bene_id,
                o.year,
                d.zcta,
                '{outcome_name}' AS outcome
            FROM
                '{outcome_prefix}{outcome_name}_{args.year}.parquet' AS o
            INNER JOIN
                '{denom_prefix}_{args.year}.parquet' AS d
            ON
                o.bene_id = d.bene_id AND
                o.year = d.year
        ),
        summary_table AS (
            SELECT
                zcta,
                year,
                outcome,
                COUNT(*) AS n_outcomes
            FROM 
                outcomes
            GROUP BY 
                zcta, year, outcome
        )
        SELECT 
            z.zcta,
            z.year,
            s.outcome,
            COALESCE(s.n_outcomes,0) AS n_outcomes
        FROM '{unique_zcta_prefix}' as z
        FULL OUTER JOIN 
            summary_table AS s 
        ON 
            z.zcta = s.zcta AND
            z.year = s.year
        WHERE z.year = {year} 
    """     
    return query

def main(args):
    conn = duckdb.connect(f'duckpond_{args.year}.db')

    #Fetch distinct outcomes 
    # LOGGER.info(f"Fetching distinct outcome from {args.outcome_prefix} ----")
    # query_outcomes = f"""
    #         SELECT DISTINCT outcome 
    #         FROM '{args.outcome_prefix}*_{args.year}.parquet'
    # """

    # outcomes = conn.execute(query_outcomes).fetchall()
    # outcomes = [outcome[0] for outcome in outcomes]
    # LOGGER.info(f"Deteced outcomes: {outcomes}")


    #read outcome from yaml
    with open(args.icd_yml, 'r') as f:
        icd_dict = yaml.load(f, Loader=yaml.FullLoader)

    outcomes = list(icd_dict.keys())
    print(outcomes)

    # create intermediate tables for each outcome 
    for outcome in outcomes:
        query = get_zcta_yearly_counts(args.denom_prefix, args.outcome_prefix, outcome, args.year, args.unique_zcta_prefix)
        LOGGER.debug(f"Creating intermediate table for outcome {outcome} with query: {query}")
        conn.execute(query)
        LOGGER.info(f"Intermediate table created for outcome: {outcome}")

    # Combine results into a single table
    combined_query = f"""
        CREATE OR REPLACE TABLE zcta_yearly_counts AS
        SELECT 
            zcta,
            year,
            {', '.join([f"SUM(CASE WHEN outcome = '{outcome}' THEN n_outcomes ELSE 0 END) AS {outcome}" for outcome in outcomes])}
        FROM (
            { ' UNION ALL '.join([f"SELECT zcta, year, '{outcome}' AS outcome, n_outcomes FROM zcta_counts_{outcome}" for outcome in outcomes]) }
        ) summary
        GROUP BY zcta, year
"""

    conn.execute(combined_query)

    #get count of unique zctas in the final dataset
    LOGGER.info(f"Number of unique ZCTAs : {conn.execute('SELECT COUNT(DISTINCT zcta) FROM zcta_yearly_counts').fetchone()[0]}")

    #get total counts of all distinct outcomes 
    for outcome in outcomes:
        total_count = conn.execute(f"SELECT SUM({outcome}) FROM zcta_yearly_counts").fetchone()[0]
        LOGGER.info(f"Total count for outcome '{outcome} : {total_count}" )
    
    # Write the combined counts to a parquet file
    LOGGER.info("## Writing output -----")
    output_file = f'{args.output_prefix}_{args.year}.parquet'
    conn.execute(f"""
            COPY (SELECT * FROM zcta_yearly_counts) TO '{output_file}' (FORMAT PARQUET)
        """)
    LOGGER.info(f"Output file written to {output_file}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", 
                        default = 2010, 
                        type=int
                       )
    parser.add_argument("--icd_yml",
                        default = "./conf/icd_codes/ccw.yml"
                        )
    parser.add_argument("--denom_prefix", 
                        default =  "data/input/mbsf_medpar_denom/denom"  
                       ) 
    parser.add_argument("--outcome_prefix", 
                        default = "data/output/medpar_outcomes/ccw/"
                       )  
    parser.add_argument("--unique_zcta_prefix", 
                        default = "data/input/zip2zcta__uds/xwalk/unique_zcta.parquet"
                       ) 
    parser.add_argument("--output_prefix", 
                    default = "data/output/medpar_outcomes/ccw/zcta_yearly/counts"
                   )
    args = parser.parse_args()
    
    main(args)
