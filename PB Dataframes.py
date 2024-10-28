# Databricks notebook source
data = [("India",1947),("Bangladesh",1971),("South Korea",1948)]
columns = ["CountryName", "YearOfIndependence"]
CountriesDataFrame = spark.createDataFrame(data,columns)
CountriesDataFrame.display()

# COMMAND ----------

CountriesDataFrame.createOrReplaceTempView("CountriesTable")
ReducedCountriesDataFrame = spark.sql("select * from CountriesTable where YearOfIndependence > 1950")
ReducedCountriesDataFrame.display()

ReducedCountriesDataFrame2 = CountriesDataFrame.filter(CountriesDataFrame["YearOfIndependence"]>1970)
ReducedCountriesDataFrame2.display()


# COMMAND ----------

ReducedCountriesDataFrame2.show()

ReducedCountriesDataFrame2.display()

# COMMAND ----------

ReducedCountriesDataFrame2Pandas = ReducedCountriesDataFrame2.toPandas()

# COMMAND ----------

ReducedCountriesDataFrame2Pandas.display()


# COMMAND ----------


