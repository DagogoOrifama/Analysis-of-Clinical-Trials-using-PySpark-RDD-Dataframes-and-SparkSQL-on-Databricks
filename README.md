# Data Exploration and Preparation Using RDD, DataFrames, and SQL

This part of the project focuses on data exploration and preparation using RDD, DataFrames, and SQL. The goal is to perform extensive data analysis on clinical trial and pharmaceutical datasets to extract valuable insights.

## Setup and Environment

- **Platform**: Databricks
- **Cluster**: Created using the latest Databricks runtime (Scala 2.12 and Spark 3.3.1)
- **Datasets**: `clinicaltrial_2021.csv` and `pharma.csv`

## Data Cleaning and Preparation

1. **Uploading and Unzipping Datasets**: 
    - Uploaded `clinicaltrial_2021.csv` and `pharma.csv` to the Databricks file system.
    - Unzipped the files using command line operations in Databricks notebooks.

2. **Importing Data into RDDs and DataFrames**: 
    - Imported the datasets into RDDs using `sc.textFile`.
    - Converted RDDs to DataFrames for easier manipulation and analysis.
    - Created temporary views for SQL analysis using `createOrReplaceTempView`.

## Problem-Specific Implementations

### Problem One: Count Unique IDs
- **Objective**: Determine the count of unique IDs in the dataset.
- **RDD Implementation**: Used `map` and `count` functions to process the data.
- **DataFrame Implementation**: Utilized `select` and `distinct` methods followed by `count`.
- **SQL Implementation**: Executed SQL queries to count distinct IDs.

### Problem Two: Count Occurrences of Each Type
- **Objective**: Calculate the number of occurrences for each type of prescription.
- **RDD Implementation**: Applied `countByValue` and sorted the results.
- **DataFrame Implementation**: Used `groupBy` and `count` methods, then ordered by count.
- **SQL Implementation**: Executed `GROUP BY` and `ORDER BY` SQL queries.

### Problem Three: Extract and Count Conditions
- **Objective**: Identify and count the unique conditions listed in the dataset.
- **RDD Implementation**: Split conditions by commas, removed whitespace, and counted occurrences.
- **DataFrame Implementation**: Used `withColumn`, `split`, and `explode` to handle conditions.
- **SQL Implementation**: Created a lateral view with `explode` and `split` functions for counting.

### Problem Four: Identify Sponsors Not Associated with Pharmaceutical Companies
- **Objective**: Determine sponsors that are not pharmaceutical companies.
- **RDD Implementation**: Mapped and filtered sponsors, excluding known pharmaceutical companies.
- **DataFrame Implementation**: Filtered and grouped DataFrame by sponsors, excluding pharmaceutical companies.
- **SQL Implementation**: Nested subqueries to exclude pharmaceutical companies and count remaining sponsors.

### Problem Five: Analyze Completed Studies by Month and Year
- **Objective**: Analyze the number of completed studies per month in a given year.
- **RDD Implementation**: Filtered RDD for completed studies, extracted month and year, and counted by month.
- **DataFrame Implementation**: Filtered DataFrame, extracted month, and grouped by month for counting.
- **SQL Implementation**: Used SQL queries to filter by status and year, then grouped by month for counting.

### Further Analysis
- **Top 10 Medications**: 
    - **RDD Implementation**: Mapped interventions, counted occurrences, and displayed top 10.
- **Penalized Companies**: 
    - **DataFrame Implementation**: Filtered DataFrame for False Claims Act violations, grouped by company, and counted occurrences.
- **Top Sponsors**: 
    - **SQL Implementation**: Executed subqueries to list top sponsors not being pharmaceutical companies.

## Data Visualization

- Utilized Matplotlib to create visual representations of the results from the various analyses.
- Plotted the distribution of completed studies, occurrences of conditions, and sponsor analysis.

## Conclusion

This task demonstrates the use of RDD, DataFrames, and SQL for comprehensive data analysis and preparation. The project successfully provided valuable insights into clinical trial and pharmaceutical data, showcasing the capabilities of Spark for large-scale data processing and analysis.
