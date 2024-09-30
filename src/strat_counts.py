import duckdb 
import argparse
import logging 

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_zcta_strat_counts_query(denom_prefix, year, outcomes_prefix):
    """
    SQL query to generate stratified zcta-level outcome counts
    for a given year by bringing together ffs-denominator and outcomes data.

    query : str
        A SQL query that aggregates the following:
        - Counts of unique beneficiaries (`n_bene`) stratified by `year`, `zcta`, `age_grp`, `sex`, `race`, and `dual`.
        - Sum of deceased beneficiaries (`n_dead`) for the specified year.
        - Sum of hospitalizations (`n_hosp`) using the hospitalization data for the specified year.
        
        The query groups the data by `year`, `zcta`, `age_grp`, `sex`, `race`, 
        and `dual` to provide the total enrollment counts and hospitalizations.
    """
    ## Preparing query----
    query = f"""
        WITH denom AS (
            SELECT 
                bene_id,
                year,
                zcta,
                CASE 
                    WHEN age_dob >= 65 AND age_dob <= 75 THEN '[65,75]'
                    WHEN age_dob >= 76 AND age_dob <= 85 THEN '[76,85]'
                    WHEN age_dob >= 86 AND age_dob <= 95 THEN '[86,95]'
                    WHEN age_dob >= 96 AND age_dob <= 100 THEN '[96,100]'
                ELSE NULL
                END AS age_grp,
                sex,
                race,
                --race_rti,
                dual
            FROM '{denom_prefix}_{year}.parquet'
            ),
            outcomes AS (
                SELECT 
                    bene_id,
                    year,
                    outcome, 
                    count(bene_id) AS n_outcomes
                FROM '{outcomes_prefix}_{year}.parquet'
                GROUP BY 
                    bene_id, 
                    year, 
                    outcome
            )
            SELECT
                d.year,
                d.zcta,
                d.age_grp,
                d.sex,
                d.race,
                d.dual,
                SUM(o.n_outcomes) AS n_outcomes
            FROM denom d
            INNER JOIN outcomes o 
            ON d.bene_id = o.bene_id AND d.year = o.year
            GROUP BY 
                d.year,
                d.zcta,
                d.age_grp,
                d.sex,
                d.race,
                d.dual
            ORDER BY
                d.year,
                d.zcta,
                d.age_grp,
                d.sex,
                d.race,
                d.dual
    """
    return query 

def main(args) :

    conn = duckdb.connect()

    LOGGER.info("## Preparing counts ----")
    query = get_zcta_strat_counts_query(args.denom_prefix, args.year, args.outcomes_prefix)
    conn.execute(f"""
        CREATE TABLE zcta_strat_counts AS
        {query}
    """)
    zcta_strat_counts = conn.table("zcta_strat_counts")

    LOGGER.info(f"""
        counts shape: {zcta_strat_counts.count("*").fetchone()}
    """)

    LOGGER.info(f"""
        counts head : {zcta_strat_counts.limit(5).fetchdf()}
    """)

    LOGGER.info('## Writing counts denom ----')
    output_file = f"{args.output_prefix}_{args.year}.parquet"
    zcta_strat_counts.write_parquet(output_file)
    LOGGER.info(f"## Output file written to {output_file}")

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",
                        default = 2016,
                        type = int
                        )
    parser.add_argument("--denom_prefix",
                        default = "data/input/mbsf_medpar_denom/mbsf_medpar_denom"
                        )
    parser.add_argument("--outcomes_prefix",
                        default = "data/output/medpar_outcomes/icd_codes_8/outcomes")
    parser.add_argument("--output_prefix",
                        default = "data/output/medpar_outcomes/icd_codes_8/zcta_yearly/strat_counts"
                        )
    args = parser.parse_args()

    main(args)
