# Medicare Provider Analysis and Review (MedPAR) Outcomes 

This repository provides pipeline for generating preliminary nationwide counts of hospitalizations based on any diagnosis code within the MedPAR denominator file using ICD (International Classification of Diseases) code lists.

## About MedPAR dataset 

MedPAR dataset contains detailed records of hospital inpatient and skilled nursing facility (SNF) stays for Medicare beneficiaries in the United States. This dataset includes key information such as dates of admission and discharge, diagnoses, procedures, and billing details, all coded using the ICD codes. MedPAR data also provides demographic information like age, gender, and race.

## Table of Contents

- [Project Overview](#project-overview)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Usage](#usage)
  - [Step 1: Prepare Your Data](#step-1-prepare-your-data)
  - [Step 2: Define ICD Code Lists](#step-2-define-icd-code-lists)
  - [Step 3: Run the Processing Script](#step-3-run-the-processing-script)
  - [Step 4: Review the Output](#step-4-review-the-output)
- [ICD Code Lists](#icd-code-lists)
- [Output](#output)

## Project Overview

The MedPAR Outcomes Processing repository aims to provide a streamlined approach for hospital admissions in the MedPAR dataset using  ICD codes. This process is essential for researchers and healthcare analysts who need to filter and analyze hospital data based on certain medical conditions or procedures.

## Repository Structure

The repository is organized into the following directories and files:

- **data/**: Directory containing subfolders for input, and output files. This directory includes symlinks to the actual data files. It also contains documentation and notes for internal use.
   - input/: Contains symlinks to the raw MedPAR datasets that need to be processed.
   - output/: The directory where the processed datasets are saved. These datasets include original admission details along with the labels indicating the presence of conditions or procedures as defined by the ICD codes.
- **icd_codes/**: Contains YAML files that lists ICD codes used to label and categorize hospital admissions. Each file represents a specific condition or procedure.
- **scripts/**: Python scripts for processing the MEDPAR data and applying the ICD code lists.
- **notes/**: Includes python files related to data processing and project-specific details.
- **README.md**: Provides an overview of the project and instructions for usage.
- **requirements.txt**: Lists the Python packages required to run the scripts.

## Getting Started

 **Clone the repository:**

Clone the repository and create a conda environment.

   ```bash
   git clone <https://github.com/<user>/repo>
   cd <repo>

   conda env create -f requirements.yml
   conda activate <env_name>
   ```

## Usage
Step 1: Prepare Your Data
- Add symlinks to input, and output folders inside the corresponding /data subfolders.

For example:

```bash
export HOME_DIR=$(pwd)

cd $HOME_DIR/data/input/ .
ln -s <input_path> .

cd $HOME_DIR/data/output/
ln -s <output_path> .
```

The README.md files inside the /data subfolders contain path documentation for NSAPH internal purposes.

Step 2: Define ICD Code Lists
- The ICD code lists are central to labeling the admissions. These lists should be defined in YAML format and placed in the icd_codes/ directory. Each file in this directory should represent a specific condition or procedure and include the relevant ICD codes.

Step 3: Run the Processing Script
- To process the data and label the admissions based on the ICD code lists, the script reads ICD codes from a YAML file, constructs SQL queries to match these codes against the diagnoses in the MedPAR data, and then tags each hospitalization accordingly. The tagged data is then saved in a specified format (such as Parquet, Feather, or CSV) for further analysis. Run the following command:

```bash
python src/get_outcomes.py
```

In addition, .sbatch templates are provided for SLURM users. Be mindful that each HPC clusters has a different configuration and the .sbatch files might need to be modified accordingly.

Step 4: Review the Output
- The output of the processing script will be saved in the output/ directory. The labeled dataset will include original admission details along with additional columns indicating the presence of conditions or procedures as defined by the ICD code lists.

## ICD Code Lists

```bash
# Icd list created for project ....
# Example: Dementia with Lewy bodies
lewy:
  long_name: "Dementia with Lewy Bodies"
  icd9: ['33182']
  icd10: ['G3183']
```
**YAML Format for ICD Code Lists**
The ICD code lists are defined in YAML format to ensure they are easy to read and maintain. Each YAML file should have the following structure:
- #Icd list created for project: This comment should precede the listing of ICD codes to indicate the purpose of the file.
- lewy: This is a unique key representing the condition (in this case, Dementia with Lewy Bodies).
- long_name: A human-readable name for the condition.
- icd9: A list of ICD-9 codes associated with this condition.
- icd10: A list of ICD-10 codes associated with this condition.

**Adding New ICD Code Lists**

To add a new ICD code list:

- Create a New YAML File: In the icd_codes/ directory, create a new YAML file with a descriptive name (e.g., icd_code_<filenumber>.yml).
- Add ICD Codes: Define the condition and associated ICD codes in the YAML format, as shown above.
- Update the Script: Ensure that the processing script references this new ICD code list file if necessary.

## Output
- After processing, the output will be a labeled dataset saved in the output/ directory. The dataset includes all original hospital admission details along with labels indicating the presence of conditions or procedures based on the ICD code lists.



