# PySpark Crime Data Analysis Project

## Overview

This project aims to analyze crime data in Los Angeles using PySpark. It includes implementations of various join strategies (broadcast join and repartition join) to combine crime data with police station data and compute distances between crime incidents and police stations.

## Project Structure


- `code/`: Directory containing the PySpark scripts organized by different approaches.
  - `DataFrame/`: Scripts using the DataFrame API.
    - `Q1_CSV.py`: Script for analyzing data with CSV format.
    - `Q1_PARQ.py`: Script for analyzing data with Parquet format.
    - `Q3_ALL.py`: Script for executing the main analysis.
    - `Q3_CSV.py`: Script for analyzing data with CSV format.
    - `Q4.py`: Script for additional analysis.
  - `RDD/`: Scripts using the RDD API.
    - `Q2_CSV.py`: Script for analyzing data with CSV format.
    - `Q4_JOIN.py`: Script for performing joins using RDDs.
  - `SQL/`: Scripts using the SQL API.
    - `Q1_CSV.py`: Script for analyzing data with CSV format.
    - `Q1_PARQ.py`: Script for analyzing data with Parquet format.
    - `Q2_CSV.py`: Script for analyzing data with CSV format.
    - `csv_to_parquet.py`: Utility script for converting CSV to Parquet.
- `.gitattributes`: Git attributes configuration.
- `README.md`: This readme file.

## Setup

### Prerequisites

- Python 3.8
- PySpark
- Hadoop (for HDFS)

### Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/crime-data-analysis.git
cd crime-data-analysis

```

2. Install the required packages:

```bash
pip install -r requirements.txt
```

3. Download the crime data and police station data from the following links:
* [Crime Data](https://data.lacity.org/Public-Safety/Crime-Data-from-2010-to-2019/63jg-8b9z)
* [Police Station Data](https://geohub.lacity.org/datasets/lahub::lapd-police-stations/explore)
* [Median Household Income Data](https://www.laalmanac.com/employment/em12c_2015.php)


### Detailed Instructions

For more detailed instructions on how to run the scripts, please refer to the project report.


## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.