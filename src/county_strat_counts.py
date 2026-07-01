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

def get_county_strat_counts(temporal_freq, year, denom_prefix, outcome_prefix, outcome):
    if temporal_freq == "yearly":
        return f"""
            CREATE OR REPLACE TEMP TABLE county_counts_{outcome} AS 
            WITH outcomes AS (
                SELECT 
                    o.bene_id,
                    o.adm_id,
                    EXTRACT(YEAR FROM o.admission_date) AS year,
                    d.county,
                    d.state,
                    CASE 
                        WHEN d.age_dob BETWEEN 65 AND 75 THEN '[65,75]'
                        WHEN d.age_dob BETWEEN 76 AND 85 THEN '[76,85]'
                        WHEN d.age_dob BETWEEN 86 AND 95 THEN '[86,95]'
                        WHEN d.age_dob BETWEEN 96 AND 105 THEN '[96,105]'
                        ELSE '[106,115]'
                    END AS age_grp,
                    d.race,
                    d.sex,
                    d.dual,
                    '{outcome}' AS outcome
                FROM read_parquet('{outcome_prefix}/{outcome}_*.parquet', union_by_name=True) AS o
                INNER JOIN read_parquet('{denom_prefix}_{year}.parquet') AS d
                    ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
                WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            summary_table AS (
                SELECT
                    county,
                    state,
                    age_grp,
                    race,
                    sex,
                    dual,
                    year,
                    outcome,
                    COUNT(*) AS n_outcomes
                FROM outcomes
                GROUP BY county, state, age_grp, race, sex, dual, year, outcome
            )
            SELECT
                c.county,
                c.state,
                c.age_grp,
                c.race,
                c.sex,
                c.dual,
                c.year,
                COALESCE(s.n_outcomes, 0) AS n_outcomes,
                '{outcome}' AS outcome
            FROM all_combinations c
            LEFT JOIN summary_table s
                ON c.county = s.county 
                AND c.state = s.state 
                AND c.age_grp = s.age_grp 
                AND c.race = s.race 
                AND c.sex = s.sex 
                AND c.dual = s.dual 
                AND c.year = s.year 
            ORDER BY c.county, c.state, c.age_grp, c.race, c.sex, c.dual
        """

    elif temporal_freq == "monthly":
        return f"""
            CREATE OR REPLACE TEMP TABLE county_counts_{outcome} AS 
            WITH outcomes AS (
                SELECT 
                    o.bene_id,
                    o.adm_id,
                    EXTRACT(YEAR FROM o.admission_date) AS year,
                    EXTRACT(MONTH FROM o.admission_date)::INT AS month,
                    d.county,
                    d.state,
                    CASE 
                        WHEN d.age_dob BETWEEN 65 AND 75 THEN '[65,75]'
                        WHEN d.age_dob BETWEEN 76 AND 85 THEN '[76,85]'
                        WHEN d.age_dob BETWEEN 86 AND 95 THEN '[86,95]'
                        WHEN d.age_dob BETWEEN 96 AND 105 THEN '[96,105]'
                        ELSE '[106,115]'
                    END AS age_grp,
                    d.race,
                    d.sex,
                    d.dual,
                    '{outcome}' AS outcome
                FROM read_parquet('{outcome_prefix}/{outcome}_*.parquet', union_by_name=True) AS o
                INNER JOIN read_parquet('{denom_prefix}_{year}.parquet') AS d
                    ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
                WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            summary_table AS (
                SELECT
                    county,
                    state,
                    age_grp,
                    race,
                    sex,
                    dual,
                    year,
                    month,
                    outcome,
                    COUNT(*) AS n_outcomes
                FROM outcomes
                GROUP BY county, state, age_grp, race, sex, dual, year, month, outcome
            ),
            all_county_months AS (
                SELECT 
                    c.county,
                    c.state,
                    c.age_grp,
                    c.race,
                    c.sex,
                    c.dual,
                    c.year,
                    m.month
                FROM all_combinations c
                CROSS JOIN all_months m
            )
            SELECT
                c.county,
                c.state,
                c.age_grp,
                c.race,
                c.sex,
                c.dual,
                c.year,
                c.month,
                COALESCE(s.n_outcomes, 0) AS n_outcomes,
                '{outcome}' AS outcome
            FROM all_county_months c
            LEFT JOIN summary_table s
                ON c.county = s.county 
                AND c.state = s.state 
                AND c.age_grp = s.age_grp 
                AND c.race = s.race 
                AND c.sex = s.sex 
                AND c.dual = s.dual 
                AND c.year = s.year 
                AND c.month = s.month
            ORDER BY c.county, c.state, c.age_grp, c.race, c.sex, c.dual, c.month
        """

    elif temporal_freq == "weekly":
        return f"""
            CREATE OR REPLACE TEMP TABLE county_counts_{outcome} AS 
            WITH outcomes AS (
            SELECT 
                o.bene_id,
                o.adm_id,
                EXTRACT(YEAR FROM o.admission_date) AS year,
                DATE_TRUNC('week', o.admission_date) AS week_start,
                d.county,
                d.state,
                CASE 
                    WHEN d.age_dob BETWEEN 65 AND 75 THEN '[65,75]'
                    WHEN d.age_dob BETWEEN 76 AND 85 THEN '[76,85]'
                    WHEN d.age_dob BETWEEN 86 AND 95 THEN '[86,95]'
                    WHEN d.age_dob BETWEEN 96 AND 105 THEN '[96,105]'
                    ELSE '[106,115]'
                END AS age_grp,
                d.race,
                d.sex,
                '{outcome}' AS outcome
            FROM read_parquet('{outcome_prefix}/{outcome}_*.parquet', union_by_name=True) AS o
            INNER JOIN read_parquet('{denom_prefix}_{year}.parquet') d
                ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
            WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            county_merge AS (
                SELECT *
                FROM outcomes o
                INNER JOIN unique_county AS uc
                    ON o.county = uc.county
            )
            SELECT
                county,
                state,
                age_grp,
                race,
                sex,
                year,
                week_start,
                EXTRACT(WEEK FROM week_start) AS week,
                COUNT(*) AS n_outcomes,
                '{outcome}' AS outcome
            FROM county_merge
            GROUP BY county, state, age_grp, race, sex, year, week_start
            ORDER BY county, week_start
        """
    elif temporal_freq == "daily":
        return f"""
            CREATE OR REPLACE TEMP TABLE county_counts_{outcome} AS 
            WITH outcomes AS (
                SELECT 
                    o.bene_id,
                    o.adm_id,
                    EXTRACT(YEAR FROM o.admission_date) AS year,
                    o.admission_date AS date,
                    d.county,
                    d.state,
                    CASE 
                        WHEN d.age_dob BETWEEN 65 AND 75 THEN '[65,75]'
                        WHEN d.age_dob BETWEEN 76 AND 85 THEN '[76,85]'
                        WHEN d.age_dob BETWEEN 86 AND 95 THEN '[86,95]'
                        WHEN d.age_dob BETWEEN 96 AND 105 THEN '[96,105]'
                        ELSE '[106,115]'
                    END AS age_grp,
                    d.race,
                    d.sex,
                    '{outcome}' AS outcome
                FROM read_parquet('{outcome_prefix}/{outcome}_*.parquet') AS o
                INNER JOIN read_parquet('{denom_prefix}_{year}.parquet') d
                ON o.bene_id = d.bene_id 
                WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            county_merge AS (
                SELECT *
                FROM outcomes o
                INNER JOIN unique_county AS uc ON o.county = uc.county
            )
            SELECT
                county,
                state,
                age_grp,
                race,
                sex,
                year,
                date,
                CAST(COUNT(*) AS INTEGER) AS n_outcomes,
                '{outcome}' AS outcome
            FROM county_merge
            GROUP BY county, state, age_grp, race, sex, year, date 
            ORDER BY county, date
            """


def main(args):
    conn = duckdb.connect(f'duckpond_{args.year}_{args.temporal_freq}.db')

    # get unqiue county 
    if args.temporal_freq in ['yearly', 'monthly']:
        LOGGER.info("Creating cross join strat all_combinations temp table...")
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE all_combinations AS
            SELECT 
                uc.county AS county,
                fips.state_abb AS state,
                uc.year AS year,
                age_grps.age_grp,
                races.race,
                sexes.sex,
                duals.dual
            FROM read_parquet('{args.unique_county_prefix}_{args.year}.parquet') AS uc
            LEFT JOIN read_parquet('{args.fips2abbr}') AS fips
                ON uc.state = fips.state_fips
            CROSS JOIN (VALUES ('[65,75]'), ('[76,85]'), ('[86,95]'), ('[96,105]'), ('[106,115]')) AS age_grps(age_grp)
            CROSS JOIN (VALUES (0), (1), (2), (3), (4), (5), (6)) AS races(race)
            CROSS JOIN (VALUES (0), (1), (2)) AS sexes(sex)
            CROSS JOIN (VALUES (0), (1)) AS duals(dual)
        """)

    if args.temporal_freq == "monthly":
        LOGGER.info("Creating reusable all_months temp table..")
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE all_months AS 
            SELECT UNNEST(GENERATE_SERIES(1,12))::INT AS month
        """)

    if args.temporal_freq in ['daily', 'weekly']:
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE unique_county AS
            SELECT county, year
            FROM read_parquet('{args.unique_county_prefix}_{args.year}.parquet') z""")

    # get distinct outcomes 
    LOGGER.info(f"Fetching distinct outcomes from {args.outcome_prefix}*.parquet ...")
    outcomes_query = f"SELECT DISTINCT outcome FROM read_parquet('{args.outcome_prefix}*.parquet')"
    outcomes = conn.execute(outcomes_query).fetchdf()["outcome"].tolist()
    LOGGER.info(f"distinct outcomes: {outcomes}")

    # Create intermediate tables for each outcome 
    for outcome in outcomes:
        query = get_county_strat_counts(args.temporal_freq, args.year, args.denom_prefix, args.outcome_prefix, outcome)
        LOGGER.debug(f"Creating intermediate table for outcome {outcome} with query: {query}")
        conn.execute(query)
        LOGGER.info(f"Intermediate table created for outcome: {outcome}")

    # Define group by columns based on temporal frequency
    if args.temporal_freq == 'yearly':
        groupby_clause = "county, state, year, age_grp, sex, race, dual"
    elif args.temporal_freq == 'monthly':
        groupby_clause = "county, state, year, month, age_grp, sex, race, dual"
    elif args.temporal_freq == 'weekly':
        groupby_clause = "county, state, year, week_start, week, age_grp, sex, race"
    else:
        groupby_clause = "county, state, year, date, age_grp, sex, race"

    # Build SELECT clause
    select_vars = groupby_clause + ''.join([
    f", CAST(SUM(CASE WHEN outcome = '{o}' THEN n_outcomes ELSE 0 END) AS INTEGER) AS {o}"
    for o in outcomes])

    # Build union of all outcomes
    union_query = ' UNION ALL '.join([
        f"SELECT county, state, year, age_grp, sex, race, dual" +
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
    if args.temporal_freq in ["yearly", "monthly"]:
        filename = f"strat_counts_{args.year}.parquet"
    else:
        filename = f"sparse_strat_counts_{args.year}.parquet"
    output_file = f"{args.output_prefix}/{output_folder}/{filename}"
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
    parser.add_argument("--temporal_freq", choices = ["yearly","monthly", "weekly", "daily"],default = "daily")
    parser.add_argument("--fips2abbr", default="data/input/us_geoboundaries__census/fips2abbr/fips_xwalk.parquet")
    parser.add_argument("--denom_prefix", default="data/input/mbsf_medpar_denom/denom")
    parser.add_argument("--outcome_prefix", default="data/output/medpar_outcomes/michelle_yichen_00/")
    parser.add_argument("--output_prefix", default="data/output/medpar_outcomes/michelle_yichen_00")
    parser.add_argument("--unique_county_prefix", default="data/input/us_geoboundaries__census/us_uniqueid__census/county_yearly/us_uniqueid__census__county_yearly_") 
    args = parser.parse_args()
    
    main(args)