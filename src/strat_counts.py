import duckdb 
import argparse
import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def get_zcta_strat_counts_query(denom_prefix, outcome_prefix, outcome_name, year, unique_zcta_prefix):

    query = f"""
        CREATE OR REPLACE TABLE zcta_strat_{outcome_name} AS
        WITH
            denom AS (
                SELECT
                    bene_id,
                    year,
                    zcta,
                    CASE 
                        WHEN age_dob BETWEEN 65  AND 75  THEN '[65,75]'
                        WHEN age_dob BETWEEN 76  AND 85  THEN '[76,85]'
                        WHEN age_dob BETWEEN 86  AND 95  THEN '[86,95]'
                        WHEN age_dob BETWEEN 96  AND 100 THEN '[96,100]'
                        WHEN age_dob BETWEEN 101 AND 115 THEN '[101,115]'
                    END AS age_grp,
                    sex,
                    race,
                    dual
                FROM '{denom_prefix}_{year}.parquet'
            ),
            outcomes AS (
                SELECT
                    bene_id,
                    year,
                    '{outcome_name}' AS outcome
                FROM '{outcome_prefix}{outcome_name}_{year}.parquet'
            ),
            stratified_outcome_counts AS (
                SELECT
                    d.year,
                    d.zcta,
                    d.age_grp,
                    d.sex,
                    d.race,
                    d.dual,
                    o.outcome,
                    COUNT(*) AS n_outcomes
                FROM denom d
                INNER JOIN outcomes o
                    ON d.bene_id = o.bene_id
                   AND d.year    = o.year
                GROUP BY
                    d.year,
                    d.zcta,
                    d.age_grp,
                    d.sex,
                    d.race,
                    d.dual,
                    o.outcome
            ),
            all_strata_zcta AS (
                SELECT 
                    u.zcta,
                    y.year,
                    g.age_grp,
                    s.sex,
                    r.race,
                    du.dual
                FROM '{unique_zcta_prefix}' AS u
                CROSS JOIN (SELECT DISTINCT year FROM denom) AS y
                CROSS JOIN (VALUES('[65,75]'),('[76,85]'),('[86,95]'),('[96,100]'),('[101,115]')) g(age_grp)
                CROSS JOIN (VALUES(0),(1),(2)) s(sex)
                CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6)) r(race)
                CROSS JOIN (VALUES(0),(1)) du(dual)
            ),
            combined_data AS (
                SELECT
                    a.zcta,
                    a.year,
                    a.age_grp,
                    a.sex,
                    a.race,
                    a.dual,
                    '{outcome_name}' AS outcome,
                    COALESCE(oc.n_outcomes, 0) AS n_outcomes
                FROM all_strata_zcta a
                LEFT JOIN stratified_outcome_counts oc
                       ON a.zcta    = oc.zcta
                      AND a.year    = oc.year
                      AND a.age_grp = oc.age_grp
                      AND a.sex     = oc.sex
                      AND a.race    = oc.race
                      AND a.dual    = oc.dual
            )
        SELECT
            zcta,
            year,
            age_grp,
            sex,
            race,
            dual,
            outcome,
            n_outcomes
        FROM combined_data
        GROUP BY
            zcta, year, age_grp, sex, race, dual, outcome, n_outcomes
    """
    return query


def main(args):
    conn = duckdb.connect()

    LOGGER.info(f"Fetching distinct outcome from {args.outcome_prefix} for year {args.year} ...")
    query_outcomes = f"""
        SELECT DISTINCT outcome
        FROM '{args.outcome_prefix}*_{args.year}.parquet'
    """
    outcomes = [row[0] for row in conn.execute(query_outcomes).fetchall()]
    LOGGER.info(f"Detected outcomes: {outcomes}")

    for outcome_name in outcomes:
        query = get_zcta_strat_counts_query(args.denom_prefix,args.outcome_prefix,outcome_name,args.year,args.unique_zcta_prefix)
        LOGGER.info(f"Creating table zcta_strat_{outcome_name}")
        conn.execute(query)
        LOGGER.info(f"Intermediate table created for outcome: {outcome_name}")

    #combine intermediate tables into a single table
    union_all_query = " UNION ALL ".join(
        [f"SELECT * FROM zcta_strat_{outcome_name}" for outcome_name in outcomes]
    )
    combine_table_query = f"""
        CREATE OR REPLACE TABLE zcta_strat_counts AS
        SELECT * FROM (
            {union_all_query}
        )
    """
    conn.execute(combine_table_query)
    LOGGER.info("Combined zcta_strat_counts table generated.")

    pivot_outcomes = [
        f"SUM(CASE WHEN outcome = '{o}' THEN n_outcomes ELSE 0 END) AS {o}"
        for o in outcomes
    ]
    pivot_query = f"""
        CREATE OR REPLACE TABLE zcta_strat_counts AS
        SELECT
            zcta,
            year,
            age_grp,
            sex,
            race,
            dual,
            {', '.join(pivot_outcomes)}
        FROM zcta_strat_counts
        GROUP BY
            zcta, year, age_grp, sex, race, dual
    """
    conn.execute(pivot_query)

    LOGGER.info("Final zcta_strat_counts table created.")

    LOGGER.info("Exploration of the output file---")

    LOGGER.info(f"num_rows : {conn.execute('SELECT COUNT(*) AS num_rows FROM zcta_strat_counts').fetchone()}")

    LOGGER.info(f"df head : {conn.execute('SELECT * FROM zcta_strat_counts LIMIT 5').fetchdf()}")

    LOGGER.info(f"Unique ZCTAs Count : {conn.execute('SELECT COUNT(DISTINCT zcta) FROM zcta_strat_counts').fetchone()[0]}")

    #outcome counts 
    columns = conn.execute("PRAGMA table_info('zcta_strat_counts')").fetchdf()['name'].tolist()
    outcome_columns = [col for col in columns if col not in {'zcta', 'year', 'age_grp', 'sex', 'race', 'dual'}]

    for col in outcome_columns:
        total = conn.execute(f'SELECT sum({col}) FROM zcta_strat_counts').fetchone()[0]
        LOGGER.info(f"Total sum of'{col}': {total}")
                                                                             
    LOGGER.info('## Writing output ----')
    output_file = f"{args.output_prefix}_{args.year}.parquet"
    conn.execute(f"""
        COPY (SELECT * FROM zcta_strat_counts)
        TO '{output_file}' (FORMAT PARQUET)""")
    LOGGER.info(f"Output file written to: {output_file}")
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", 
                        default=2013, 
                        type=int
                        )
    parser.add_argument("--denom_prefix", 
                        default="data/input/mbsf_medpar_denom/denom"
                        )
    parser.add_argument("--outcome_prefix", 
                        default="data/output/medpar_outcomes/michelle_yongsoo_00/"
                        )
    parser.add_argument("--unique_zcta_prefix", 
                        default="data/input/zip2zcta__uds/xwalk/unique_zcta.parquet"
                        )
    parser.add_argument("--output_prefix", 
                        default="data/output/medpar_outcomes/michelle_yongsoo_00/zcta_yearly/strat_counts"
                        )

    args = parser.parse_args()
    main(args)