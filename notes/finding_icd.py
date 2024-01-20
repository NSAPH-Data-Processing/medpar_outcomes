import pandas as pd
import duckdb
import argparse
import yaml
import os

conn = duckdb.connect()

# Specify the filename
filename9 = './data/input/Section111ValidICD9-Jan2024.csv'
filename10 = './data/input/Section111ValidICD10-Jan2024_edit.csv'


# Create a DuckDB table from the CSV file
conn.execute(f'CREATE TABLE valid_icd_9 AS SELECT * FROM read_csv_auto(\'{filename9}\', ALL_VARCHAR=1)')

# Create a DuckDB table from the CSV file
conn.execute(f'CREATE TABLE valid_icd_10 AS SELECT * FROM read_csv_auto(\'{filename10}\', ALL_VARCHAR=1)')


# Define the path to the YAML file
yaml_file_path = './conf/icd_codes/icd_codes_2.yml'

# Read YAML file into a dictionary
with open(yaml_file_path, 'r') as yaml_file:
    icd_codes = yaml.safe_load(yaml_file)

# Initialize empty dictionaries to store results
icd9_result_dict = {}
icd10_result_dict = {}

# Loop through each section in icd_codes dictionary
for key, values in icd_codes.items():
    icd9_result_list = []
    icd10_result_list = []

    for icd9_code in values["icd9"]:
        query = f"""
        SELECT *
        FROM valid_icd_9
        WHERE CODE LIKE '{icd9_code}%'
        ORDER BY CODE
        """
        result = conn.execute(query).fetchdf()
        icd9_result_list.extend(result.CODE.tolist())

    for icd10_code in values["icd10"]:
        query = f"""
        SELECT *
        FROM valid_icd_10
        WHERE column0 LIKE '{icd10_code}%'
        ORDER BY column0
        """
        result = conn.execute(query).fetchdf()
        icd10_result_list.extend(result.column0.tolist())

    # Add icd9 and icd10 result lists to respective dictionaries
    icd9_result_dict[key] = {"icd9": icd9_result_list}
    icd10_result_dict[key] = {"icd10": icd10_result_list}
