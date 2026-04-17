-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON 

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM json.`/Volumes/demo_prep_associate/demo_datasets/bookstore_data/customers-json/export_001.json`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.json(f"{dataset_bookstore}/customers-json/export_001.json")
-- MAGIC display(df)

-- COMMAND ----------

SELECT * FROM json.`/Volumes/demo_prep_associate/demo_datasets/bookstore_data/customers-json/export_*.json`

-- COMMAND ----------

SELECT * FROM json.`/Volumes/demo_prep_associate/demo_datasets/bookstore_data/customers-json`

-- COMMAND ----------

SELECT count(*) FROM json.`/Volumes/demo_prep_associate/demo_datasets/bookstore_data/customers-json`

-- COMMAND ----------

-- MAGIC  %skip
-- MAGIC  --input_file_name are not supported in Unity Catalog
-- MAGIC  SELECT *,
-- MAGIC     input_file_name() source_file
-- MAGIC   FROM json.`/Volumes/demo_prep_associate/demo_datasets/bookstore_data/customers-json`;

-- COMMAND ----------

--add source_file column to the dataframe
SELECT *,
    _metadata.file_path source_file
    FROM json.`/Volumes/demo_prep_associate/demo_datasets/bookstore_data/customers-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying text Format

-- COMMAND ----------

SELECT * FROM text.`/Volumes/demo_prep_associate/demo_datasets/bookstore_data/customers-json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying binaryFile Format

-- COMMAND ----------

SELECT * FROM binaryFile.`/Volumes/demo_prep_associate/demo_datasets/bookstore_data/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying CSV 

-- COMMAND ----------

SELECT * FROM csv.`/Volumes/demo_prep_associate/demo_datasets/bookstore_data/books-csv`

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC -- LOCATION não é suportado no databricks free edition sem conta cloud(ex:azure)
-- MAGIC CREATE TABLE books_csv
-- MAGIC USING csv
-- MAGIC OPTIONS (
-- MAGIC   header = "true",
-- MAGIC   delimiter = ";"
-- MAGIC )LOCATION "/Volumes/demo_prep_associate/demo_datasets/bookstore_data/books-csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # API Spark Dataframe

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read\
-- MAGIC         .format("csv")\
-- MAGIC         .option("header", "true")\
-- MAGIC         .option("delimiter", ";")\
-- MAGIC         .load(f"{dataset_bookstore}/books-csv")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

--SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitations of Non-Delta Tables

-- COMMAND ----------

--DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC         .table("books_csv")
-- MAGIC       .write
-- MAGIC         .mode("append")
-- MAGIC         .format("csv")
-- MAGIC         .option('header', 'true')
-- MAGIC         .option('delimiter', ';')
-- MAGIC         .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC REFRESH TABLE books_csv

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`/Volumes/demo_prep_associate/demo_datasets/bookstore_data/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`/Volumes/demo_prep_associate/demo_datasets/bookstore_data/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "/Volumes/demo_prep_associate/demo_datasets/bookstore_data/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books AS
  SELECT * FROM books_tmp_vw;
  
SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Simplified File Querying
-- MAGIC Databricks recently introduced a new function called read_files that makes it easier to query CSV files and other file formats directly, without needing to first create a temporary view.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS books_new AS
SELECT * FROM read_files('/Volumes/demo_prep_associate/demo_datasets/bookstore_data/books-csv/export_*.csv',
  format=>'csv',
  header=>'true',
  delimiter=>';');

SELECT * FROM books_new;
