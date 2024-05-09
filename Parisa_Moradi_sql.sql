-- Databricks notebook source
-- MAGIC %python
-- MAGIC sqlrdd = sc.textFile("/FileStore/tables/clinicaltrial_2023")
-- MAGIC sqlrdd.take(2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sqlrdd1=sqlrdd.map(lambda line: line.replace(',,', ''))
-- MAGIC sqlrdd1.take(2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sqlrdd2=sqlrdd1.map(lambda line: line.replace('"', ''))
-- MAGIC sqlrdd2.take(3)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sqlrdd3=sqlrdd2.map(lambda x: x.split('\t'))
-- MAGIC sqlrdd3.take(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def adjust_row_length(row, length):
-- MAGIC     return row + [None] * (length - len(row))
-- MAGIC     
-- MAGIC rddNew = sqlrdd3.map(lambda row: adjust_row_length(row, 14))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.types import *
-- MAGIC spark = SparkSession.builder.appName("Example").getOrCreate()
-- MAGIC mySchema = StructType([
-- MAGIC     StructField("Id", StringType()),
-- MAGIC     StructField("StudyTitle", StringType()),
-- MAGIC     StructField("Acronym", StringType()),
-- MAGIC     StructField("Status", StringType()),
-- MAGIC     StructField("Conditions", StringType()),
-- MAGIC     StructField("Interventions", StringType()),
-- MAGIC     StructField("Sponsor", StringType()),
-- MAGIC     StructField("Collaborators", StringType()),
-- MAGIC     StructField("Enrollment", StringType()),
-- MAGIC     StructField("FunderType", StringType()),
-- MAGIC     StructField("Type", StringType()),
-- MAGIC     StructField("StudyDesign", StringType()),
-- MAGIC     StructField("Start", StringType()),
-- MAGIC     StructField("Completion", StringType())
-- MAGIC ])
-- MAGIC
-- MAGIC sqldf = spark.createDataFrame(rddNew, mySchema)
-- MAGIC print("Original DataFrame:")
-- MAGIC sqldf.show()
-- MAGIC header = sqldf.first()
-- MAGIC filtered_rdd = sqldf.rdd.filter(lambda line: line != header)
-- MAGIC sqldf1 = spark.createDataFrame(filtered_rdd, mySchema)
-- MAGIC print("DataFrame without header:")
-- MAGIC sqldf1.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sqldf1.createOrReplaceTempView("clinicaltrial")

-- COMMAND ----------

SELECT * FROM clinicaltrial LIMIT 20

-- COMMAND ----------

/*Question 1 SQL implemetation*/
SELECT count(distinct `StudyTitle`)from clinicaltrial

-- COMMAND ----------

/*Question 2*/
SELECT Type, COUNT(*) AS count
FROM clinicaltrial
WHERE Type IS NOT NULL AND Type != 'Type'
GROUP BY Type
ORDER BY count DESC;

-- COMMAND ----------

/*Question 3*/
SELECT condition, COUNT(*) AS Frequency
FROM (
    SELECT EXPLODE(SPLIT(Conditions, '\\|')) AS condition
    FROM clinicaltrial
    WHERE Conditions IS NOT NULL
)
GROUP BY condition
ORDER BY Frequency DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC file_path = "/FileStore/tables/pharma"
-- MAGIC pharma_df = spark.read.csv(file_path, header=True, inferSchema=True)
-- MAGIC pharma_df.show()
-- MAGIC Parent_Company_Column_Index = 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma_df.createOrReplaceTempView("pharma")

-- COMMAND ----------

SELECT * FROM pharma LIMIT 20

-- COMMAND ----------

SELECT sponsor,count(sponsor) from clinicaltrial DESC GROUP BY sponsor

-- COMMAND ----------

/*Question 4 SQL implementation*/
SELECT sponsor,count(*) as sponsorcountnotpharma
FROM clinicaltrial 
WHERE sponsor NOT IN (SELECT Parent_company FROM pharma )
GROUP BY sponsor
ORDER BY sponsorcountnotpharma DESC LIMIT 10

-- COMMAND ----------

/*Question 5*/
SELECT 
    SUBSTRING(Completion, 1, 7) AS Month,
    COUNT(*) AS Completed_Studies_Count
FROM 
    clinicaltrial
WHERE 
    Status = 'COMPLETED' AND Completion LIKE '2023-%'
GROUP BY 
    SUBSTRING(Completion, 1, 7)
ORDER BY 
    Month;


-- COMMAND ----------

/*Query 3 for further analysis*/
SELECT StudyTitle
FROM clinicaltrial
WHERE status = 'RECRUITING';
