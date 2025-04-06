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

def read_icd_string(icd_yml, outcome):
    with open(icd_yml, 'r') as f:
        icd_dict = yaml.load(f, Loader=yaml.FullLoader)

    icd_string = (
        ",".join([f"'{x}'" for x in icd_dict[outcome]["icd9"]]) + 
        "," +
        ",".join([f"'{x}'" for x in icd_dict[outcome]["icd10"]])
    )

    return icd_string

def get_outcome_query(outcome_criteria, icd_string, hosp_prefix, year, first_n = None):
    print("## Preparing query----")
    file = f"{hosp_prefix}_{year}.parquet"

    if outcome_criteria == 'all':
        query = f"""
        SELECT bene_id, adm_id
        FROM (SELECT bene_id, adm_id, UNNEST(diagnoses) AS diag FROM '{file}')
        WHERE diag IN ({icd_string})
        GROUP BY bene_id, adm_id
        """

    elif outcome_criteria == 'first_n' and first_n is not None:
        conditions = " OR ".join([f"diagnoses[{i}] IN ({icd_string})" for i in range(1, first_n +1)])
        query = f"""
        SELECT bene_id, adm_id
        FROM '{file}'
        WHERE {conditions}
        GROUP BY bene_id, adm_id
        """

    print(query)
    return query 

def main(args):

    conn = duckdb.connect()

    LOGGER.info(f"Running query for year {args.year}...")

    #read outcome from yaml
    with open(args.icd_yml, 'r') as f:
        icd_dict = yaml.load(f, Loader=yaml.FullLoader)

    outcomes = list(icd_dict.keys())
    print(outcomes)

    outcome_df_list = []
    for outcome in outcomes:
        print(f"Preparing {outcome}----")
        icd_string = read_icd_string(args.icd_yml, outcome)
        outcome_criteria = args.outcome_criteria

        query = get_outcome_query(outcome_criteria, icd_string, args.hosp_prefix, args.year, args.first_n)
        outcome_df = conn.execute(query).fetchdf()
        
        outcome_df['outcome'] = outcome

        if outcome_criteria == "all":
            outcome_df['outcome_criteria'] = "all"
        elif outcome_criteria == "first_n":
            outcome_df['outcome_criteria'] = f'first_{args.first_n}'
            
        print(outcome_df.shape)
        print(outcome_df.head())
        outcome_df_list.append(outcome_df)

    df = pd.concat(outcome_df_list)
    df['year'] = args.year

    print('##writing outcomes----')                                              

    output_file = f"{args.output_prefix}_{args.year}.{args.output_format}"
    if args.output_format == "parquet":
        df.to_parquet(output_file,index=False)
    elif args.output_format == "feather":
        df.to_feather(output_file, index = False)
    elif args.output_format == "csv":
        df.to_csv(output_file,index=False)

    LOGGER.info(f"Output file written to {output_file}")
    
    #close connection
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", 
                        default = 2000, 
                        type=int
                       )
    parser.add_argument("--icd_yml",
                        default = "./conf/icd_codes/michelle_tian_00.yml"
                        )
    parser.add_argument("--hosp_prefix", 
                        default = "./data/input/mbsf_medpar_denom/inpatient"
                       )
    parser.add_argument("--outcome_criteria",
                        default = "all",
                        choices = ["all", "first_n"]
                        )
    parser.add_argument("--first_n",
                        default = 1,
                        type = int,
                        help = "Number of diagnosis to consider for query"
                        ) 
    parser.add_argument("--output_format", 
                        default = "parquet", 
                        choices=["parquet", "feather", "csv"]
                       )           
    parser.add_argument("--output_prefix", 
                        default = "./data/intermediate/outcomes"
                        )
    args = parser.parse_args()
    
    main(args)

