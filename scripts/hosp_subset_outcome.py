import duckdb
import argparse
import logging
import pandas as pd

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main(args): 
    """
    Example script to filter all cause hospitalization records of beneficiaries with the specified outcome. 
    """

    conn = duckdb.connect('duckpond.db')
    conn.execute(f"""
    COPY (
        SELECT inpatient.*
        FROM '{args.inpatient_prefix}_*.parquet' AS inpatient
        INNER JOIN 
            (SELECT DISTINCT bene_id
             FROM'{args.outcome_prefix}_*.parquet') AS outcome
        ON inpatient.bene_id = outcome.bene_id
    ) TO '{args.output_prefix}.parquet' 
    """)

    LOGGER.info(f"# saved filtered data at {args.output_prefix}.parquet ----")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--inpatient_prefix", 
                        default =  "data/input/mbsf_medpar_denom/inpatient"  
                       ) 
    parser.add_argument("--outcome_prefix", 
                        default = "data/output/medpar_outcomes/michelle_garam_00/adrd"
                       )  
    parser.add_argument("--output_prefix", 
                    default = "data/output/medpar_outcomes/michelle_garam_00/hosp_subset_adrd"
                   )
    args = parser.parse_args()
    
    main(args)