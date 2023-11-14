-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Task 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import countDistinct
-- MAGIC 
-- MAGIC # Read data into DataFrame
-- MAGIC data_file = "clinicaltrial_2021"
-- MAGIC clinicaltrialDF = spark.read.options(delimiter ="|").csv("/FileStore/tables/" + data_file + ".csv",
-- MAGIC                             header = "true",
-- MAGIC                             inferSchema="true")
-- MAGIC clinicaltrialDF.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrialDF.createOrReplaceTempView("ClinicalTrialView")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read data into DataFrame
-- MAGIC data_file2 = "pharma"
-- MAGIC pharmaDF = spark.read.options(delimiter =",").csv("/FileStore/tables/" + data_file2 + ".csv",
-- MAGIC                             header = "true",
-- MAGIC                             inferSchema="true")
-- MAGIC pharmaDF.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Create a temporary view for the Pharmaceuticals dataset
-- MAGIC pharmaDF.createOrReplaceTempView("PharmaceuticalView")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 1

-- COMMAND ----------

SELECT COUNT(DISTINCT Id) 
AS NumberOfStudies 
FROM ClinicalTrialView;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 2

-- COMMAND ----------

SELECT Type, COUNT(*) AS frequency
FROM ClinicalTrialView
GROUP BY Type
ORDER BY frequency DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 3

-- COMMAND ----------

SELECT trim(explodedCondition) as Condition, count(*) as Frequency
FROM ClinicalTrialView
LATERAL VIEW explode(split(Conditions, ',')) explodedVirtualTable as explodedCondition
GROUP BY trim(explodedCondition)
ORDER BY Frequency DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 4

-- COMMAND ----------

SELECT Sponsor, COUNT(*) AS NumberOfTrials
FROM (
  SELECT *
  FROM ClinicalTrialView
  WHERE Sponsor NOT IN (
    SELECT DISTINCT Parent_Company
    FROM PharmaceuticalView
    WHERE Parent_Company IS NOT NULL
  )
) AS NonPharmaTrials
GROUP BY Sponsor
ORDER BY NumberOfTrials DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 5

-- COMMAND ----------

SELECT
  LEFT(TRIM(Completion), 3) AS CompletionMonth, 
  COUNT(Completion) AS CompletedStudiesPerMonth
FROM 
  ClinicalTrialView
WHERE 
  Completion LIKE '%2021' 
  AND Status = 'Completed' 
GROUP BY 
  Completion
ORDER BY 
  CASE 
    WHEN CompletionMonth = 'Jan' THEN 1
    WHEN CompletionMonth = 'Feb' THEN 2
    WHEN CompletionMonth = 'Mar' THEN 3
    WHEN CompletionMonth = 'Apr' THEN 4
    WHEN CompletionMonth = 'May' THEN 5
    WHEN CompletionMonth = 'Jun' THEN 6
    WHEN CompletionMonth = 'Jul' THEN 7
    WHEN CompletionMonth = 'Aug' THEN 8
    WHEN CompletionMonth = 'Sep' THEN 9
    WHEN CompletionMonth = 'Oct' THEN 10
    WHEN CompletionMonth = 'Nov' THEN 11
    ELSE 12
  END 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 5 - Visualization

-- COMMAND ----------

--Exporting the result of the above query to a csv file
INSERT OVERWRITE DIRECTORY
    USING CSV
    OPTIONS ('path' "dbfs:/FileStore/tables/Question5SQL/", header "true", inferSchema "true")
SELECT
  LEFT(TRIM(Completion), 3) AS CompletionMonth, 
  COUNT(Completion) AS CompletedStudiesPerMonth
FROM 
  ClinicalTrialView
WHERE 
  Completion LIKE '%2021' 
  AND Status = 'Completed' 
GROUP BY 
  Completion
ORDER BY 
  CASE 
    WHEN CompletionMonth = 'Jan' THEN 1
    WHEN CompletionMonth = 'Feb' THEN 2
    WHEN CompletionMonth = 'Mar' THEN 3
    WHEN CompletionMonth = 'Apr' THEN 4
    WHEN CompletionMonth = 'May' THEN 5
    WHEN CompletionMonth = 'Jun' THEN 6
    WHEN CompletionMonth = 'Jul' THEN 7
    WHEN CompletionMonth = 'Aug' THEN 8
    WHEN CompletionMonth = 'Sep' THEN 9
    WHEN CompletionMonth = 'Oct' THEN 10
    WHEN CompletionMonth = 'Nov' THEN 11
    ELSE 12
  END 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import pandas as pd
-- MAGIC 
-- MAGIC 
-- MAGIC # Read the CSV file into a DataFrame
-- MAGIC StudiesPerMonthCount = spark.read.options(delimiter =",").csv("/FileStore/tables/Question5SQL/*.csv",
-- MAGIC                             header = "true",
-- MAGIC                             inferSchema="true")
-- MAGIC                             
-- MAGIC df = StudiesPerMonthCount.toPandas()
-- MAGIC                              
-- MAGIC # Create a Pandas DataFrame from the list of tuples
-- MAGIC # df = pd.DataFrame(result, columns=['Months', 'NumberOfCompletedStudies'])
-- MAGIC 
-- MAGIC # Create a bar chart using matplotlib
-- MAGIC # Plot the data
-- MAGIC plt.figure(figsize=(15, 7))
-- MAGIC plt.bar(df['CompletionMonth'], df['CompletedStudiesPerMonth'])
-- MAGIC plt.xticks(rotation=90)
-- MAGIC plt.xlabel('Months', fontsize=18)
-- MAGIC plt.ylabel('Number Of Completed Studies', fontsize=18)
-- MAGIC plt.title('Number of Completed Studies Per Month', fontsize=25)
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Further analysis 3
-- MAGIC Assuming that the Company and Parent Company column contains all the possible pharmaceutical companies, get the 10 most common sponsors that are not pharmaceutical companies, along
-- MAGIC with the number of clinical trials they have sponsored.

-- COMMAND ----------

SELECT Sponsor, COUNT(*) AS NumberOfTrials
FROM (
  SELECT *
  FROM ClinicalTrialView
  WHERE Sponsor NOT IN (
    SELECT DISTINCT Company
    FROM PharmaceuticalView
    WHERE Company IS NOT NULL
    UNION
    SELECT DISTINCT Parent_Company
    FROM PharmaceuticalView
    WHERE Parent_Company IS NOT NULL
  )
) AS NonPharmaTrials
GROUP BY Sponsor
ORDER BY NumberOfTrials DESC
LIMIT 10

-- COMMAND ----------

--Exporting the result of the above query to a csv file
INSERT OVERWRITE DIRECTORY
    USING CSV
    OPTIONS ('path' "dbfs:/FileStore/tables/FurtherAnalysisSQL/", header "true", inferSchema "true")
SELECT Sponsor, COUNT(*) AS NumberOfTrials
FROM (
  SELECT *
  FROM ClinicalTrialView
  WHERE Sponsor NOT IN (
    SELECT DISTINCT Company
    FROM PharmaceuticalView
    WHERE Company IS NOT NULL
    UNION
    SELECT DISTINCT Parent_Company
    FROM PharmaceuticalView
    WHERE Parent_Company IS NOT NULL
  )
) AS NonPharmaTrials
GROUP BY Sponsor
ORDER BY NumberOfTrials DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Further analysis 3 - Visualization

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import pandas as pd
-- MAGIC 
-- MAGIC # Read the CSV file into a Spark DataFrame
-- MAGIC StudiesPerMonthCount = spark.read.options(delimiter =",").csv("/FileStore/tables/FurtherAnalysisSQL/*.csv",
-- MAGIC                             header = "true",
-- MAGIC                             inferSchema="true")
-- MAGIC 
-- MAGIC # Convert Spark DataFrame into Pandas DataFrame                            
-- MAGIC df = StudiesPerMonthCount.toPandas()
-- MAGIC 
-- MAGIC # Create a bar chart using matplotlib
-- MAGIC # Set the axis lable and plot the data
-- MAGIC plt.figure(figsize=(15, 7))
-- MAGIC plt.bar(df['Sponsor'], df['NumberOfTrials'])
-- MAGIC plt.xticks(rotation=90)
-- MAGIC plt.xlabel('Sponsor', fontsize=20)
-- MAGIC plt.ylabel('Number of trials', fontsize=20)
-- MAGIC plt.title('Top 10 sponsors with the most non-pharmaceutical trials', fontsize=25)
-- MAGIC plt.show()
