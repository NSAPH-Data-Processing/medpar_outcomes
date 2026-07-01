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

def get_zcta_strat_counts(temporal_freq, year, denom_prefix, outcome_prefix, outcome):
    if temporal_freq == "yearly":
        return f"""
            CREATE OR REPLACE TEMP TABLE zcta_counts_{outcome} AS 
            WITH outcomes AS (
                SELECT 
                    o.bene_id,
                    o.adm_id,
                    EXTRACT(YEAR FROM o.admission_date) AS admission_year,
                    d.zcta,
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
                INNER JOIN read_parquet('{denom_prefix}_{year}.parquet') AS d
                    ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
                WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            summary_table AS (
                SELECT
                    zcta,
                    state,
                    age_grp,
                    race,
                    sex,
                    admission_year,
                    outcome,
                    COUNT(*) AS n_outcomes
                FROM outcomes
                GROUP BY zcta, state, age_grp, race, sex, admission_year, outcome
            )
            SELECT
                c.zcta,
                c.state,
                c.age_grp,
                c.race,
                c.sex,
                c.admission_year,
                COALESCE(s.n_outcomes, 0) AS n_outcomes,
                '{outcome}' AS outcome
            FROM all_combinations c
            LEFT JOIN summary_table s
                ON c.zcta = s.zcta 
                AND c.state = s.state 
                AND c.age_grp = s.age_grp 
                AND c.race = s.race 
                AND c.sex = s.sex 
                AND c.admission_year = s.admission_year 
            ORDER BY c.zcta, c.state, c.age_grp, c.race, c.sex
        """

    elif temporal_freq == "weekly":
        return f"""
            CREATE OR REPLACE TEMP TABLE zcta_counts_{outcome} AS 
            WITH outcomes AS (
            SELECT 
                o.bene_id,
                o.adm_id,
                EXTRACT(YEAR FROM o.admission_date) AS admission_year,
                DATE_TRUNC('week', o.admission_date) AS week_start,
                d.zcta,
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
                ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
            WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            zcta_merge AS (
                SELECT *
                FROM outcomes o
                INNER JOIN unique_zcta AS uc
                    ON o.zcta = uc.zcta
            )
            SELECT
                zcta,
                state,
                age_grp,
                race,
                sex,
                admission_year,
                week_start,
                EXTRACT(WEEK FROM week_start) AS week,
                COUNT(*) AS n_outcomes,
                '{outcome}' AS outcome
            FROM zcta_merge
            GROUP BY zcta, state, age_grp, race, sex, admission_year, week_start
            ORDER BY zcta, week_start
        """

    elif temporal_freq == "daily":
        return f"""
            CREATE OR REPLACE TEMP TABLE zcta_counts_{outcome} AS 
            WITH outcomes AS (
                SELECT 
                    o.bene_id,
                    o.adm_id,
                    EXTRACT(YEAR FROM o.admission_date) AS admission_year,
                    o.admission_date AS date,
                    d.zcta,
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
                    ON o.bene_id = d.bene_id AND EXTRACT(YEAR FROM o.admission_date) = d.year
                WHERE EXTRACT(YEAR FROM o.admission_date) = {year}
            ),
            zcta_merge AS (
                SELECT *
                FROM outcomes o
                INNER JOIN unique_zcta AS uc ON o.zcta = uc.zcta
            )
            SELECT
                zcta,
                state,
                age_grp,
                race,
                sex,
                admission_year,
                date,
                COUNT(*) AS n_outcomes,
                '{outcome}' AS outcome
            FROM zcta_merge
            GROUP BY zcta, state, age_grp, race, sex,admission_year, date
            ORDER BY zcta, date
        """

def main(args):
    conn = duckdb.connect(f'duckpond_{args.year}_{args.temporal_freq}.db')

    # get unqiue zcta 
    if args.temporal_freq == 'yearly':
        LOGGER.info("Creating cross join strat all_combinations temp table...")
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE all_combinations AS
            SELECT 
                uz.zcta AS zcta,
                fips.state_abb AS state,
                uz.year AS admission_year,
                age_grps.age_grp,
                races.race,
                sexes.sex
            FROM read_parquet('{args.unique_zcta_prefix}_{args.year}.parquet') AS uz
            LEFT JOIN read_parquet('./data/input/fips2anci.parquet') fips
                ON uz.county_state = fips.state_fips
            CROSS JOIN (VALUES ('[65,75]'), ('[76,85]'), ('[86,95]'), ('[96,105]'), ('[106,115]')) AS age_grps(age_grp)
            CROSS JOIN (VALUES (0), (1), (2), (3), (4), (5), (6)) AS races(race)
            CROSS JOIN (VALUES (0), (1), (2)) AS sexes(sex)
        """)

    if args.temporal_freq in ['daily', 'weekly']:
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE unique_zcta AS
            SELECT zcta, year
            FROM read_parquet('{args.unique_zcta_prefix}_{args.year}.parquet') z""")

    # get distinct outcomes 
    LOGGER.info(f"Loading outcomes from {args.icd_yml}")
    with open(args.icd_yml, 'r') as f:
        icd_dict = yaml.load(f, Loader=yaml.FullLoader)
        outcomes = list(icd_dict.keys())

    # Create intermediate tables for each outcome 
    for outcome in outcomes:
        query = get_zcta_strat_counts(args.temporal_freq, args.year, args.denom_prefix, args.outcome_prefix, outcome)
        LOGGER.debug(f"Creating intermediate table for outcome {outcome} with query: {query}")
        conn.execute(query)
        LOGGER.info(f"Intermediate table created for outcome: {outcome}")

    # Define group by columns based on temporal frequency
    if args.temporal_freq == 'yearly':
        groupby_clause = "zcta, state, admission_year, age_grp, sex, race"
    elif args.temporal_freq == 'weekly':
        groupby_clause = "zcta, state, admission_year, week_start, week, age_grp, sex, race"
    else:
        groupby_clause = "zcta, state, admission_year, date, age_grp, sex, race"

    # Build SELECT clause
    select_vars = groupby_clause + ''.join([
        f", SUM(CASE WHEN outcome = '{o}' THEN n_outcomes ELSE 0 END) AS {o}" for o in outcomes
    ])

    # Build union of all outcomes
    union_query = ' UNION ALL '.join([
        f"SELECT zcta, state, admission_year, age_grp, sex, race" +
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
    if args.temporal_freq == "yearly":
        filename = f"strat_counts_{args.year}.parquet"
    else:
        filename = f"strat_sparse_counts_{args.year}.parquet"
    output_file = f"{args.output_prefix}/{output_folder}/{filename}"
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
    parser.add_argument("--year", default=2007, type=int)
    parser.add_argument("--denom_prefix", default="data/input/mbsf_medpar_denom/denom")
    parser.add_argument("--outcome_prefix", default="data/output/medpar_outcomes/michelle_tian_00/")
    parser.add_argument("--icd_yml", default="./conf/icd_codes/michelle_tian_00.yml")
    parser.add_argument("--output_prefix", default="data/output/medpar_outcomes/michelle_tian_00")
    parser.add_argument("--unique_zcta_prefix", default="data/input/us_geoboundaries__census/us_uniqueid__census/zcta_yearly/us_uniqueid__census__zcta_yearly_") 
    args = parser.parse_args()
    
    main(args)