# Final Project

This project processes and analyzes user search data using PySpark. The data is stored in various CSV and JSON files and is processed to find the most searched keywords and their categories.


## Files

- **Dataset/**: Contains the raw data files in JSON and CSV formats.
- **Final Project Product/**: Contains the main project files including Jupyter notebooks and Python scripts.

## Main Scripts

### [finalproject.py](Final%20Project%20Product/finalproject.py)

This script contains the main functions for processing the data:

- `read_data()`: Reads the largest data files for each month.
- `load_data()`: Loads the data for months 6 and 7 along with their categories.
- `process_data(ds)`: Processes the dataset to find the most searched keywords.
- `final_data(ds, category_ds)`: Joins the processed data with the category data.
- `change_columname(ds, i)`: Renames the columns for the final dataset.
- `find_trending(ds1, ds2)`: Finds the trending categories between two datasets.
- `main()`: Main function to execute the data processing and analysis.

### [final project.ipynb](Final%20Project%20Product/final%20project.ipynb)

This Jupyter notebook contains the step-by-step data processing and analysis using PySpark.

## How to Run

1. Ensure you have PySpark installed.
2. Place the data files in the appropriate directories as shown in the project structure.
3. Run the `finalproject.py` script to process and analyze the data:

## Data
The data consists of user search logs and keyword categories. The search logs are stored in the log_search directory, and the keyword categories are stored in CSV files.

## Output
The output of the project includes the processed data with the most searched keywords and their categories, as well as the trending categories between two months.
