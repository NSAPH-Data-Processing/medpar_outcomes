import duckdb
import argparse
import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_county_counts(year, zcta_prefix, zcta2county_prefix):

    conn = duckdb.connect()
    schema_info = conn.execute(f"DESCRIBE SELECT * FROM '{zcta_prefix}_{year}.parquet'").fetchall()
    cols = [row[0] for row in schema_info if row[0] not in ["zcta", "year"]]

    aggregations = [f"SUM(z.{col} * (zc.pop_pct / 100)) AS {col}" for col in cols]
    aggregations_str = ",\n        ".join(aggregations)

    # SQL query to aggregate county outcomes at the county level
    query = f"""
        WITH zcta_counts AS ( 
            SELECT *
            FROM '{zcta_prefix}_{year}.parquet'
        ),
        county_counts AS (
            SELECT
                zc.year,
                zc.county,
                {aggregations_str}
            FROM zcta_counts z
            FULL OUTER JOIN '{zcta2county_prefix}_{year}.parquet' zc
            ON z.zcta = zc.zcta
            GROUP BY zc.year, zc.county
        )
        SELECT 
            c.year,
            c.county,
            {', '.join([f"COALESCE(c.{col}, 0)::INT AS {col}" for col in cols])}
        FROM county_counts c
        WHERE county IS NOT NULL
        ORDER BY c.year, c.county
    """
    return query

def main(args):
    conn = duckdb.connect()

    LOGGER.info("## Preparing county-level counts ----")
    query = get_county_counts(args.year, args.zcta_prefix, args.zcta2county_prefix)
 
    conn.execute(f"""
        CREATE TABLE county_yearly_counts AS
        {query}
    """)

    county_yearly_counts = conn.table("county_yearly_counts")

    LOGGER.info(f"county_yearly_counts rows: {county_yearly_counts.count('*').fetchone()}")
    LOGGER.info(f"county_yearly_counts head:\n{county_yearly_counts.limit(5).fetchdf()}")

    LOGGER.info('## Writing output ----')
    output_file = f"{args.output_prefix}_{args.year}.parquet"
    county_yearly_counts.write_parquet(output_file)

    LOGGER.info(f"## Output file written to {output_file}")

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", 
                        default=2006, 
                        type=int
                        )
    parser.add_argument("--zcta_prefix", 
                        default="data/output/medpar_outcomes/michelle_yongsoo_00/zcta_yearly/counts"
                        )
    parser.add_argument("--outcome_prefix", 
                        default="data/output/medpar_outcomes/michelle_yongsoo_00/"
                        )
    parser.add_argument("--zcta2county_prefix", 
                        default="data/input/zcta2county_yearly/us_xwalks__census__zcta2county_yearly_"
                        )
    parser.add_argument("--output_prefix", 
                        default="data/output/medpar_outcomes/michelle_yongsoo_00/county_yearly/counts"
                        )
    args = parser.parse_args()

    main(args)
