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

def read_icd_codes(icd_yml, outcome):
    with open(icd_yml, 'r') as f:
        icd_dict = yaml.load(f, Loader=yaml.FullLoader)

    primary_icd9 = icd_dict[outcome].get("primary_icd9", [])
    primary_icd10 = icd_dict[outcome].get("primary_icd10", [])
    secondary_icd9 = icd_dict[outcome].get("secondary_icd9", [])
    secondary_icd10 = icd_dict[outcome].get("secondary_icd10", [])

    primary_icd = primary_icd9 + primary_icd10
    secondary_icd = secondary_icd9 + secondary_icd10

    # Convert to properly formatted SQL strings
    primary_icd_string = ",".join([f"'{code}'" for code in primary_icd])
    secondary_icd_string = ",".join([f"'{code}'" for code in secondary_icd])

    return primary_icd_string, secondary_icd_string

def get_outcome_query(outcome, icd_yml, hosp_prefix, year):
    LOGGER.info(f"Generating query for outcome: {outcome} for year: {year}")

    primary_icd, secondary_icd = read_icd_codes(icd_yml, outcome)

    file = f"{hosp_prefix}_{year}.parquet"

    max_secondary_pos = 25
    secondary_conditions = " OR ".join(
        f"diagnoses[{i}] IN ({secondary_icd})"
        for i in range(2, max_secondary_pos + 1)
    )
    query = f"""
    SELECT bene_id, adm_id,diagnoses, 
    diagnoses[1] AS primary_dx,
    LIST_SLICE(diagnoses, 2, {max_secondary_pos}) AS secondary_dx
    FROM '{file}'
    WHERE diagnoses[1] IN ({primary_icd})
      AND ({secondary_conditions})
    GROUP BY bene_id, adm_id, diagnoses
    """

    return query


def main(args):

    conn = duckdb.connect()

    LOGGER.info(f"Running query for year {args.year}...")

    with open(args.icd_yml, "r") as f:
        icd_dict = yaml.load(f, Loader=yaml.FullLoader)

    outcomes = list(icd_dict.keys())
    outcome_df_list = []

    for outcome in outcomes:
        LOGGER.info(f"Processing outcome: {outcome}")
        query = get_outcome_query(outcome, args.icd_yml, args.hosp_prefix, args.year)
        LOGGER.info(f"Generated SQL for outcome '{outcome}':\n{query}")
        
        outcome_df = conn.execute(query).fetchdf()
        outcome_df["outcome"] = outcome

        LOGGER.info(f"{outcome}: Retrieved {outcome_df.shape[0]} records.")
        outcome_df_list.append(outcome_df)

    df = pd.concat(outcome_df_list)

    LOGGER.info("Writing output file...")
    output_file = f"{args.output_prefix}_{args.year}.{args.output_format}"
    if args.output_format == "parquet":
        df.to_parquet(output_file,index=False)
    elif args.output_format == "csv":
        df.to_csv(output_file,index=False)

    LOGGER.info(f"Output file written to {output_file}")
    
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", 
                        default = 2000, 
                        type=int
                       )
    parser.add_argument("--icd_yml",
                        default = "./conf/icd_codes/xiao_00.yml"
                        )
    parser.add_argument("--hosp_prefix", 
                        default = "./data/input/mbsf_medpar_denom/inpatient"
                       )
    parser.add_argument("--output_format", 
                        default = "parquet", 
                        choices=["parquet", "csv"]
                       )           
    parser.add_argument("--output_prefix", 
                        default = "./data/output/medpar_outcomes/xiao_00/pd_lewy_outcomes"
                        )
    args = parser.parse_args()
    
    main(args)