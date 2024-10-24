# Databricks notebook source
# DBTITLE 1,Import file into DBFS
# MAGIC  %sh
# MAGIC  rm -r /dbfs/delta_lab
# MAGIC  mkdir /dbfs/delta_lab
# MAGIC  wget -O /dbfs/delta_lab/products.csv https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/products.csv

# COMMAND ----------

# DBTITLE 1,Ingest file into DataFrame
ProductsDataFrame = spark.read.load('/delta_lab/products.csv', format = 'csv', header = True)
display(ProductsDataFrame.limit(10))

# COMMAND ----------

# DBTITLE 1,Create an empty Delta table
# MAGIC %sql
# MAGIC create TABLE ProductsDeltaTable(
# MAGIC     ProductID integer,
# MAGIC     ProductName string,
# MAGIC     Category  string,
# MAGIC     ListPrice float
# MAGIC )
# MAGIC using delta
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail ProductsDeltaTable

# COMMAND ----------

# DBTITLE 1,Move data from DataFrame into existing Delta table
ProductsDataFrame.write.format('delta').mode('append').save('dbfs:/user/hive/warehouse/productsdeltatable')
