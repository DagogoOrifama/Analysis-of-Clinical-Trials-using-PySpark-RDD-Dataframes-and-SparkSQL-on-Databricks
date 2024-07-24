# RDD, DataFrames, SQL, and Machine Learning Data Science Project

This project involves data exploration and machine learning using RDD, DataFrames, SQL, and various machine learning models. It is structured into two main tasks and demonstrates comprehensive data analysis, preprocessing, and model development on the Databricks platform.

## Task One: Data Exploration and Preparation

### Description of Required Setup
- Set up a Databricks account and created a cluster using the latest runtime.
- Uploaded and unzipped datasets (`clinicaltrial_2021.csv` and `pharma.csv`) into the Databricks file system.

### Data Cleaning and Preparation
- Imported datasets into RDD and DataFrame formats.
- Created views for SQL analysis.
- Performed initial data cleaning to prepare for analysis.

### Problem-Specific Implementations
1. **Problem One**: Implemented solutions using RDD, DataFrame, and SQL to count unique IDs.
2. **Problem Two**: Counted occurrences of each type using RDD, DataFrame, and SQL.
3. **Problem Three**: Extracted and counted conditions from datasets using RDD, DataFrame, and SQL.
4. **Problem Four**: Identified sponsors not associated with pharmaceutical companies using RDD, DataFrame, and SQL.
5. **Problem Five**: Analyzed completed studies by month and year using RDD, DataFrame, and SQL.
6. **Further Analysis**: 
    - Listed top 10 medications using RDD.
    - Identified companies penalized for False Claims Act using DataFrame.
    - Analyzed top sponsors not being pharmaceutical companies using SQL.

## Task Two: Machine Learning Model Development

### Setup Environment
- Switched to Databricks Machine Learning platform.
- Created a new cluster and Jupyter notebook for the project.

### Importing and Description of Dataset
- Uploaded `faultDataset.csv` containing sensor data from an industrial vibration sensor.
- The dataset includes 20 sensor readings and a `fault_detected` column indicating machine faults.

### Data Exploration and Cleaning
- Imported the dataset into RDD and DataFrame formats.
- Created views for SQL analysis.
- Performed data exploration to evaluate minimum, average, and maximum values.

### Data Preprocessing
- Transformed data into a format suitable for machine learning using `RFormula`.
- Split data into training and test sets.

### Developing Machine Learning Models
- Implemented five machine learning models: Decision Tree, Linear SVC, Logistic Regression, Random Forest, and Gradient-Boosted Tree.
- Evaluated models using accuracy metrics.

#### Decision Tree Classification Model
- Achieved 95.55% accuracy with default hyperparameters.
- Improved accuracy to 95.56% through hyperparameter tuning.

#### Other Classification Models
- Linear SVC, Logistic Regression, Random Forest, and Gradient-Boosted Tree models were implemented.
- Gradient-Boosted Tree achieved the highest accuracy of 99.67%.

### Data Privacy, Ethical, and Legal Issues
- Used publicly available datasets licensed for educational and research purposes.

## Conclusion

This project demonstrates the use of RDD, DataFrames, SQL, and machine learning models for comprehensive data analysis and model development. The Gradient-Boosted Tree model achieved the highest accuracy of 99.67% for predicting machine maintenance needs based on sensor data.

For more detailed information, please refer to the project report included in the repository.
