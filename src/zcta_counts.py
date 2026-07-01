import pandas as pd 
import pyarrow 
import duckdb
import argparse 
import yaml 
import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_zcta_counts(temporal_freq, year, denom_prefix, outcome_prefix, outcome):
    if temporal_freq == "yearly":
        return f"""
            CREATE OR REPLACE TEMP TABLE zcta_counts_{outcome} AS 
            WITH outcomes AS (
                SELECT o.bene_id, o.adm_id,
                       EXTRACT(YEAR FROM o.admission_date) AS admission_year,
                       d.zcta, d.state,
                       '{outcome}' AS outcome
                FROM read_parquet('{outcome_prefix}/{outcome}_*.parquet') AS o
                INNER JOIN read_parquet('{denom_prefix}_{year}.parquet') d
                    ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
                WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            summary_table AS (
                SELECT zcta, state, admission_year, outcome, COUNT(*) AS n_outcomes
                FROM outcomes
                GROUP BY zcta, state, admission_year, outcome
            )
            SELECT uc.zcta, uc.state, COALESCE(s.admission_year, uc.year) AS admission_year,
                   COALESCE(s.n_outcomes, 0) AS n_outcomes, '{outcome}' AS outcome
            FROM unique_zcta uc
            FULL OUTER JOIN summary_table s 
              ON uc.zcta = s.zcta AND uc.state = s.state AND uc.year = s.admission_year
            WHERE uc.zcta IS NOT NULL
            ORDER BY uc.zcta, s.admission_year 
        """

    elif temporal_freq == "weekly":
        return f"""
            CREATE OR REPLACE TEMP TABLE zcta_counts_{outcome} AS 
            WITH outcomes AS (
                SELECT o.bene_id, o.adm_id, EXTRACT(YEAR FROM o.admission_date) AS admission_year,
                       DATE_TRUNC('week', o.admission_date) AS week_start,
                       d.zcta, d.state, '{outcome}' AS outcome
                FROM read_parquet('{outcome_prefix}/{outcome}_*.parquet') AS o
                INNER JOIN read_parquet('{denom_prefix}_{year}.parquet') d
                    ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
                WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            summary_table AS (
                SELECT zcta, state, admission_year, week_start::DATE AS week_start, outcome, COUNT(*) AS n_outcomes
                FROM outcomes
                GROUP BY zcta, state, admission_year, week_start, outcome
            ),
            all_zcta_weeks AS (
                SELECT z.zcta, z.state, w.week_start, z.year
                FROM unique_zcta z
                CROSS JOIN all_weeks w
            )
            SELECT a.zcta, a.state, COALESCE(s.admission_year, a.year) AS admission_year,
                   a.week_start, EXTRACT(WEEK FROM a.week_start) AS week,
                   COALESCE(s.n_outcomes, 0) AS n_outcomes, '{outcome}' AS outcome
            FROM all_zcta_weeks a
            FULL OUTER JOIN summary_table s 
              ON a.zcta = s.zcta AND a.state = s.state AND a.year = s.admission_year AND a.week_start = s.week_start
            WHERE a.zcta IS NOT NULL
            ORDER BY a.zcta, a.state, a.week_start
        """

    elif temporal_freq == "daily":
        return f"""
            CREATE OR REPLACE TEMP TABLE zcta_counts_{outcome} AS 
            WITH outcomes AS (
                SELECT o.bene_id, o.adm_id, o.admission_date AS date,
                       EXTRACT(YEAR FROM o.admission_date) AS admission_year,
                       d.zcta, d.state, '{outcome}' AS outcome
                FROM read_parquet('{outcome_prefix}/{outcome}_*.parquet') AS o
                INNER JOIN read_parquet('{denom_prefix}_{year}.parquet') d
                    ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
                WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            summary_table AS (
                SELECT zcta, state, admission_year, date, outcome, COUNT(*) AS n_outcomes
                FROM outcomes
                GROUP BY zcta, state, admission_year, date, outcome
            ),
            all_zcta_dates AS (
                SELECT z.zcta, z.state, d.date, z.year
                FROM unique_zcta z
                CROSS JOIN all_dates d
            )
            SELECT a.zcta, a.state, COALESCE(s.admission_year, a.year) AS admission_year,
                   a.date, COALESCE(s.n_outcomes, 0) AS n_outcomes, '{outcome}' AS outcome
            FROM all_zcta_dates a
            FULL OUTER JOIN summary_table s 
              ON a.zcta = s.zcta AND a.state = s.state AND a.year = s.admission_year AND a.date = s.date
            WHERE a.zcta IS NOT NULL
            ORDER BY a.zcta, a.date
        """

def main(args):
    conn = duckdb.connect(f'duckpond_{args.year}_{args.temporal_freq}.db')

    LOGGER.info("Creating reusable unique_zcta temp table...")
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE unique_zcta AS 
        SELECT DISTINCT zcta, year, fips.state_abb AS state
        FROM read_parquet('{args.unique_zcta_prefix}_{args.year}.parquet') AS uz
        LEFT JOIN './data/input/fips2anci.parquet' AS fips
        ON uz.county_state = fips.state_fips
    """)

    if args.temporal_freq == "daily":
        LOGGER.info("Creating all_dates temp tables...")
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE all_dates AS 
            SELECT UNNEST(GENERATE_SERIES(DATE '{args.year}-01-01', DATE '{args.year}-12-31', INTERVAL '1 day'))::DATE AS date
        """)

    if args.temporal_freq == "weekly":
        LOGGER.info("Creating all_weeks temp table...")
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE all_weeks AS 
            SELECT gs.generate_series::DATE AS week_start
            FROM generate_series(DATE_TRUNC('week', DATE '{args.year}-01-01'), DATE '{args.year}-12-31', INTERVAL '1 week') AS gs(generate_series)
        """)

    # get distinct outcomes 
    LOGGER.info(f"Loading outcomes from {args.icd_yml}")
    with open(args.icd_yml, 'r') as f:
        icd_dict = yaml.load(f, Loader=yaml.FullLoader)
        outcomes = list(icd_dict.keys())

    # Create intermediate tables for each outcome 
    for outcome in outcomes:
        query = get_zcta_counts(args.temporal_freq, args.year, args.denom_prefix, args.outcome_prefix, outcome)
        LOGGER.debug(f"Creating intermediate table for outcome {outcome} with query: {query}")
        conn.execute(query)
        LOGGER.info(f"Intermediate table created for outcome: {outcome}")

    # Define group by columns based on temporal frequency
    if args.temporal_freq == 'yearly':
        groupby_clause = "zcta, state, admission_year"
    elif args.temporal_freq == 'weekly':
        groupby_clause = "zcta, state, admission_year, week_start, week"
    else:
        groupby_clause = "zcta, state, admission_year, date"

    # Build SELECT clause
    select_vars = groupby_clause + ''.join([
        f", SUM(CASE WHEN outcome = '{o}' THEN n_outcomes ELSE 0 END) AS {o}" for o in outcomes
    ])

    # Build union of all outcomes
    union_query = ' UNION ALL '.join([
        f"SELECT zcta, state, admission_year" +
        (', week_start, EXTRACT(WEEK FROM week_start)::INT AS week' if args.temporal_freq == 'weekly' else '') +
        (', date' if args.temporal_freq == 'daily' else '') +
        f", '{o}' AS outcome, n_outcomes FROM zcta_counts_{o}"
        for o in outcomes
    ])

    # Final SQL to create the summary table
    conn.execute(f"""
        CREATE OR REPLACE TABLE zcta_{args.temporal_freq}_counts AS
        SELECT {select_vars}
        FROM ({union_query}) summary
        GROUP BY {groupby_clause}
        ORDER BY {groupby_clause}
    """)

    LOGGER.info("## Writing output ----")
    output_folder = "zcta_yearly" if args.temporal_freq == "yearly" else "zcta_weekly" if args.temporal_freq == "weekly" else "zcta_daily"
    output_file = f"{args.output_prefix}{output_folder}/counts_test_{args.year}.parquet"
    conn.execute(f"""
            COPY (
                SELECT * FROM zcta_{args.temporal_freq}_counts
            ) TO '{output_file}' (FORMAT PARQUET)
        """)
    LOGGER.info(f"## Output file written to {output_file}")

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--temporal_freq", choices = ["yearly", "weekly", "daily"],default = "yearly")
    parser.add_argument("--year", default=2005, type=int)
    parser.add_argument("--denom_prefix", default="data/input/mbsf_medpar_denom/denom")
    parser.add_argument("--outcome_prefix", default="data/output/medpar_outcomes/michelle_tian_00/")
    parser.add_argument("--icd_yml", default="./conf/icd_codes/michelle_tian_00.yml")
    parser.add_argument("--unique_zcta_prefix", default="data/input/us_geoboundaries__census/us_uniqueid__census/zcta_yearly/us_uniqueid__census__zcta_yearly_") 
    parser.add_argument("--output_prefix", default="data/output/medpar_outcomes/michelle_tian_00/")
    args = parser.parse_args()
    
    main(args)
