# HDA Dataset Availability Checker

This project provides a modular and extendable Python utility to query and validate dataset availability from the Harmonised Data Access (HDA)

## Project Structure

project_root/
│
├── main.py                         # Entry point for running the dataset check
│
├── hda_utils/                      # Modular utility package
│   ├── __init__.py
│   ├── config.py                   # Initializes HDA client and configuration
│   ├── exceptions.py               # Regex-based dataset exception rules
│   ├── metadata.py                 # Extracts geographic and temporal info
│   ├── query_builder.py            # Builds API queries from metadata
│   └── helpers.py                  # Utility functions (timeouts, conversions)
│
└── data/
    └── Datasets_availability.csv   # Output CSV with dataset availability info

## Usage

1. Cloning repository
   git clone https://github.com/yourusername/hda-dataset-checker.git
   cd hda-dataset-checker

2. Install dependencies
Make sure you have Python 3.9+ installed, then install required libraries:

   pip install pandas hda
The hda package is required to interact with the Harmonised Data Access API.

3. Configure access
Ensure you have a valid .hdarc configuration file with your HDA credentials:

   username = your_username
   password = your_password
   url = https://hda-api-url/

Place it one directory above the project root (default expected location: ../.hdarc),
or update the path in hda_utils/config.py if you prefer a custom location.

4. Run the script
python main.py

The script will:

-Retrieve available datasets.
-Fetch and parse metadata.
-Apply any exception rules.
-Check dataset availability.
-Save results to data/Datasets_availability.csv.

## Output
The output CSV contains the following columns:

| Column                                   | Description                      |
| ---------------------------------------- | -------------------------------- |
| `Dataset_id`                             | Dataset identifier               |
| `Available`                              | Boolean flag for availability    |
| `Error`                                  | Error message (if any)           |
| `Minimum Longitude`, `Maximum Longitude` | Geographic extent                |
| `Minimum Latitude`, `Maximum Latitude`   | Geographic extent                |
| `Start date`, `End date`                 | Temporal coverage                |
| `Downloadable volume (Gb)`               | Estimated data volume            |
| `Query`                                  | Query parameters sent to the API |

## Extending the Project

1. Automating the run
2. Publishing results
3. Adding a documentation
4. Adding a database for observability
5. Adding download and not just request
6. Adding metadata tests as well


