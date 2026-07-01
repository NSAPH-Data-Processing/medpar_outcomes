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

def get_county_counts(temporal_freq, year,denom_prefix, outcome_prefix, outcome):
    if temporal_freq == "yearly":
        return f"""
            CREATE OR REPLACE TEMP TABLE county_counts_{outcome} AS 
            WITH outcomes AS (
                SELECT o.bene_id, o.adm_id,
                       EXTRACT(YEAR FROM o.admission_date) AS year,
                       d.county, d.state,
                       '{outcome}' AS outcome
                FROM read_parquet('{outcome_prefix}/{outcome}_*.parquet') AS o
                INNER JOIN read_parquet('{denom_prefix}_{year}.parquet') d
                    ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
                WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            summary_table AS (
                SELECT county, state, year, outcome, COUNT(*) AS n_outcomes
                FROM outcomes
                GROUP BY county, state, year, outcome
            )
            SELECT uc.county, uc.state, COALESCE(s.year, uc.year) AS year,
                   COALESCE(s.n_outcomes, 0) AS n_outcomes, '{outcome}' AS outcome
            FROM unique_county uc
            FULL OUTER JOIN summary_table s 
              ON uc.county = s.county AND uc.state = s.state AND uc.year = s.year
            WHERE uc.county IS NOT NULL
            ORDER BY uc.county, s.year 
        """

    elif temporal_freq == "monthly":
        return f"""
            CREATE OR REPLACE TEMP TABLE county_counts_{outcome} AS 
            WITH outcomes AS (
                SELECT o.bene_id, o.adm_id,
                       EXTRACT(YEAR FROM o.admission_date) AS year,
                       EXTRACT(MONTH FROM o.admission_date)::INT AS month,
                       d.county, d.state,
                       '{outcome}' AS outcome
                FROM read_parquet('{outcome_prefix}/{outcome}_*.parquet') AS o
                INNER JOIN read_parquet('{denom_prefix}_{year}.parquet') d
                    ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
                WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            summary_table AS (
                SELECT county, state, year, month, outcome, COUNT(*) AS n_outcomes
                FROM outcomes
                GROUP BY county, state, year, month, outcome
            ),
            all_county_months AS (
                SELECT uc.county, uc.state, uc.year, m.month
                FROM unique_county uc
                CROSS JOIN all_months m
            )
            SELECT a.county, a.state, a.year, a.month,
                COALESCE(s.n_outcomes, 0) AS n_outcomes,
                '{outcome}' AS outcome
            FROM all_county_months a
            LEFT JOIN summary_table s 
                ON a.county = s.county
                AND a.state = s.state
                AND a.year = s.year 
                AND a.month = s.month
            WHERE a.county IS NOT NULL
            ORDER BY a.county, a.state, a.month
        """

    elif temporal_freq == "weekly":
        return f"""
            CREATE OR REPLACE TEMP TABLE county_counts_{outcome} AS 
            WITH outcomes AS (
                SELECT o.bene_id, o.adm_id, EXTRACT(YEAR FROM o.admission_date) AS year,
                       DATE_TRUNC('week', o.admission_date) AS week_start,
                       d.county, d.state, '{outcome}' AS outcome
                FROM read_parquet('{outcome_prefix}/{outcome}_*.parquet') AS o
                INNER JOIN read_parquet('{denom_prefix}_{year}.parquet') d
                    ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
                WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            summary_table AS (
                SELECT county, state, year, week_start::DATE AS week_start, outcome, COUNT(*) AS n_outcomes
                FROM outcomes
                GROUP BY county, state, year, week_start, outcome
            ),
            all_county_weeks AS (
                SELECT z.county, z.state, w.week_start, z.year
                FROM unique_county z
                CROSS JOIN all_weeks w
            )
            SELECT a.county, a.state, COALESCE(s.year, a.year) AS year,
                   a.week_start, EXTRACT(WEEK FROM a.week_start) AS week,
                   COALESCE(s.n_outcomes, 0) AS n_outcomes, '{outcome}' AS outcome
            FROM all_county_weeks a
            FULL OUTER JOIN summary_table s 
              ON a.county = s.county AND a.state = s.state AND a.year = s.year AND a.week_start = s.week_start
            WHERE a.county IS NOT NULL
            ORDER BY a.county, a.state, a.week_start
        """

    elif temporal_freq == "daily":
        return f"""
            CREATE OR REPLACE TEMP TABLE county_counts_{outcome} AS 
            WITH outcomes AS (
                SELECT o.bene_id, o.adm_id, o.admission_date AS date,
                       EXTRACT(YEAR FROM o.admission_date) AS year,
                       d.county, d.state, '{outcome}' AS outcome
                FROM read_parquet('{outcome_prefix}/{outcome}_*.parquet') AS o
                INNER JOIN read_parquet('{denom_prefix}_{year}.parquet') d
                    ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
                WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            summary_table AS (
                SELECT county, state, year, date, outcome, COUNT(*) AS n_outcomes
                FROM outcomes
                GROUP BY county, state, year, date, outcome
            ),
            all_county_dates AS (
                SELECT z.county, z.state, d.date, z.year
                FROM unique_county z
                CROSS JOIN all_dates d
            )
            SELECT a.county, a.state, COALESCE(s.year, a.year) AS year,
                   a.date, COALESCE(s.n_outcomes, 0) AS n_outcomes, '{outcome}' AS outcome
            FROM all_county_dates a
            FULL OUTER JOIN summary_table s 
              ON a.county = s.county AND a.state = s.state AND a.year = s.year AND a.date = s.date
            WHERE a.county IS NOT NULL
            ORDER BY a.county, a.date
        """

def main(args):
    conn = duckdb.connect(f'duckpond_{args.year}_{args.temporal_freq}.db')

    LOGGER.info("Creating reusable unique_county temp table...")
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE unique_county AS 
        SELECT DISTINCT county, year, fips.state_abb AS state
        FROM read_parquet('{args.unique_county_prefix}_{args.year}.parquet') AS uc
        LEFT JOIN read_parquet('{args.fips2abbr}') AS fips
        ON uc.state = fips.state_fips
    """)

    if args.temporal_freq == "monthly":
        LOGGER.info("Creating reusable all_months temp table...")
        conn.execute(f""" 
            CREATE OR REPLACE TEMP TABLE all_months AS 
            SELECT UNNEST(GENERATE_SERIES(1,12))::INT AS month
        """)


    if args.temporal_freq == "daily":
        LOGGER.info("Creating reusable all_dates temp tables...")
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE all_dates AS 
            SELECT UNNEST(GENERATE_SERIES(DATE '{args.year}-01-01', DATE '{args.year}-12-31', INTERVAL '1 day'))::DATE AS date
        """)

    if args.temporal_freq == "weekly":
        LOGGER.info("Creating reusable all_weeks temp table...")
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
        query = get_county_counts(args.temporal_freq, args.year, args.denom_prefix, args.outcome_prefix, outcome)
        LOGGER.debug(f"Creating intermediate table for outcome {outcome} with query: {query}")
        conn.execute(query)
        LOGGER.info(f"Intermediate table created for outcome: {outcome}")

    # Define group by columns based on temporal frequency
    if args.temporal_freq == 'yearly':
        groupby_clause = "county, state, year"
    elif args.temporal_freq == 'monthly':
        groupby_clause = "county, state, year, month"
    elif args.temporal_freq == 'weekly':
        groupby_clause = "county, state, year, week_start, week"
    else:
        groupby_clause = "county, state, year, date"

    # Build SELECT clause
    select_vars = groupby_clause + ''.join([
        f", SUM(CASE WHEN outcome = '{o}' THEN n_outcomes ELSE 0 END) AS {o}" for o in outcomes
    ])

    # Build union of all outcomes
    union_query = ' UNION ALL '.join([
        f"SELECT county, state, year" +
        (', month' if args.temporal_freq == 'monthly' else '') +
        (', week_start, EXTRACT(WEEK FROM week_start)::INT AS week' if args.temporal_freq == 'weekly' else '') +
        (', date' if args.temporal_freq == 'daily' else '') +
        f", '{o}' AS outcome, n_outcomes FROM county_counts_{o}"
        for o in outcomes
    ])

    # Final SQL to create the summary table
    conn.execute(f"""
        CREATE OR REPLACE TABLE county_{args.temporal_freq}_counts AS
        SELECT {select_vars}
        FROM ({union_query}) summary
        GROUP BY {groupby_clause}
        ORDER BY {groupby_clause}
    """)

    LOGGER.info("## Writing output ----")
    output_folder = "county_yearly" if args.temporal_freq == "yearly" else "county_monthly" if args.temporal_freq == "monthly" else "county_weekly" if args.temporal_freq == "weekly" else "county_daily"
    output_file = f"{args.output_prefix}{output_folder}/counts_{args.year}.parquet"
    conn.execute(f"""
            COPY (
                SELECT * FROM county_{args.temporal_freq}_counts
            ) TO '{output_file}' (FORMAT PARQUET)
        """)
    LOGGER.info(f"## Output file written to {output_file}")

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", default=2008, type=int)
    parser.add_argument("--temporal_freq", choices = ["yearly", "monthly", "weekly", "daily"],default = "yearly")
    parser.add_argument("--icd_yml", default="./conf/icd_codes/michelle_yichen_00.yml")
    parser.add_argument("--unique_county_prefix", default="data/input/us_geoboundaries__census/us_uniqueid__census/county_yearly/us_uniqueid__census__county_yearly_") 
    parser.add_argument("--fips2abbr", default="data/input/us_geoboundaries__census/fips2abbr/fips_xwalk.parquet")
    parser.add_argument("--denom_prefix", default="data/input/mbsf_medpar_denom/denom")
    parser.add_argument("--outcome_prefix", default="data/output/medpar_outcomes/michelle_yichen_00/")
    parser.add_argument("--output_prefix", default="data/output/medpar_outcomes/michelle_yichen_00/")
    
    args = parser.parse_args()
    
    main(args)
