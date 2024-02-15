import pandas as pd
import duckdb
import argparse
import yaml

def read_icd_string(icd_yml, outcome):
    with open(icd_yml, 'r') as f:
        icd_dict = yaml.load(f, Loader=yaml.FullLoader)

    icd_string = (
        ",".join([f"'{x}'" for x in icd_dict[outcome]["icd9"]]) + 
        "," +
        ",".join([f"'{x}'" for x in icd_dict[outcome]["icd10"]])
    )

    outcome_criteria = icd_dict[outcome].get("outcome_criteria", "all")

    return outcome_criteria, icd_string

def get_outcomes_query(outcome_criteria, icd_string, medpar_hospitalizations_prefix, year):
    print("## Preparing query----")
    file = f"{medpar_hospitalizations_prefix}_{year}.parquet"

    if outcome_criteria == 'all':
        query = f"""
        SELECT bene_id, adm_id 
        FROM (SELECT bene_id, adm_id, UNNEST(diagnoses) AS diag FROM '{file}')
        WHERE diag IN ({icd_string})
        """
    elif outcome_criteria == 'primary':
        query = f"""
        SELECT bene_id, adm_id 
        FROM '{file}'
        WHERE diagnoses[1] IN ({icd_string})
        """
    elif outcome_criteria == 'first_two':
        query = f"""
        SELECT bene_id, adm_id 
        FROM '{file}'
        WHERE diagnoses[1] IN ({icd_string}) OR diagnoses[2] IN ({icd_string})
        """
    print(query)
    return query

def main(args):
        
        conn = duckdb.connect()
        
        print("## Preparing outcomes ----")
        #read outcomes from yml
        with open(args.icd_yml, 'r') as f:
            icd_dict = yaml.load(f, Loader=yaml.FullLoader)
        
        outcomes = list(icd_dict.keys())
        print(outcomes)
        outcome_df_list = []
        for outcome in outcomes:
            print(f"preparing {outcome} ----")
            outcome_criteria, icd_string = read_icd_string(args.icd_yml, outcome)
            query = get_outcomes_query(outcome_criteria, icd_string, args.medpar_hospitalizations_prefix, args.year)
            outcome_df = conn.execute(query).fetchdf()
            
            outcome_df['outcome'] = outcome
            #print df shape and head
            print(outcome_df.shape)
            print(outcome_df.head())
            outcome_df_list.append(outcome_df)

        df = pd.concat(outcome_df_list)
    
        print("## Writing outcomes ----")
        df = df.set_index(['bene_id'])
        df['year'] = args.year
    
        output_file = f"{args.output_prefix}_{args.year}.{args.output_format}"
        if args.output_format == "parquet":
            df.to_parquet(output_file)
        elif args.output_format == "feather":
            df.to_feather(output_file)
        elif args.output_format == "csv":
            df.to_csv(output_file)
    
        print(f"## Output file written to {output_file}")
        
        #close connection
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", 
                        default = 2000, 
                        type=int
                       )
    parser.add_argument("--icd_yml",
                        default = "./conf/icd_codes/icd_codes_5.yml"
                        )
    parser.add_argument("--medpar_hospitalizations_prefix", 
                        default = "./data/input/mbsf_medpar_denom/medpar_hospitalizations"
                       )
    parser.add_argument("--output_format", 
                        default = "parquet", 
                        choices=["parquet", "feather", "csv"]
                       )           
    parser.add_argument("--output_prefix", 
                    default = "./data/output/medpar_outcomes/icd_codes_5/outcomes"
                   )
    args = parser.parse_args()
    
    main(args)
