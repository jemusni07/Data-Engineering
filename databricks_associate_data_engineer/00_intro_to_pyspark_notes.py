# Databricks notebook source
# MAGIC %md
# MAGIC #### What is Spark? Why do I have to use it over other platform?
# MAGIC
# MAGIC TODO: follow this learning path:
# MAGIC https://www.databricks.com/learn/certification/apache-spark-developer-associate

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Spark SQL module interaction in two ways.
# MAGIC * using Basic SQL syntax
# MAGIC   * %sql at the beginning on the notebook
# MAGIC * using spark dataframe api
# MAGIC   * display(spark
# MAGIC         .table("products")
# MAGIC         .select("name", "price")
# MAGIC         .where("price < 200")
# MAGIC         .orderBy("price")
# MAGIC        )
# MAGIC   * use SparkSession class as entry point to Dataframe API
# MAGIC     - in databricks, it is stored in a variable 'Spark'
# MAGIC     - sample methods are sql, table, read, rance, createDataFrame
# MAGIC     - SparkSession can also be used to run sql
# MAGIC       - result_df = spark.sql("""
# MAGIC                                 SELECT name, price
# MAGIC                                 FROM products
# MAGIC                                 WHERE price < 200
# MAGIC                                 ORDER BY price
# MAGIC                                 """)
# MAGIC   * Dataframes
# MAGIC     - schema for column names and type
# MAGIC     - printSchema() allows a nicer output
# MAGIC   * Transformations lazily evaluates the chain methods in creating new dataframes Actions triggers the transformation computation
# MAGIC     - (products_df
# MAGIC   .select("name", "price")
# MAGIC   .where("price < 200")
# MAGIC   .orderBy("price")
# MAGIC   .show())
# MAGIC     - select, where, orderBy are the transformations
# MAGIC     - .show() is the action
# MAGIC * dataframes can be converted to a sql, i.e. view
# MAGIC     - createOrReplaceTempView - budget_df.createOrReplaceTempView("budget")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Column Transformations Application
# MAGIC
# MAGIC #### Column Operators and Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | \*, + , <, >= | Math and comparison operators |
# MAGIC | ==, != | Equality and inequality tests (Scala operators are **`===`** and **`=!=`**) |
# MAGIC | alias | Gives the column an alias |
# MAGIC | cast, astype | Casts the column to a different data type |
# MAGIC | isNull, isNotNull, isNan | Is null, is not null, is NaN |
# MAGIC | asc, desc | Returns a sort expression based on ascending/descending order of the column |
# MAGIC
# MAGIC #### DataFrame Transformation Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`limit`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |
# MAGIC
# MAGIC
# MAGIC ####Creating columns in a dataframe
# MAGIC ```
# MAGIC rev_df = (events_df
# MAGIC          .filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
# MAGIC          .withColumn("purchase_revenue", (col("ecommerce.purchase_revenue_in_usd") * 100).cast("int"))
# MAGIC          .withColumn("avg_purchase_revenue", col("ecommerce.purchase_revenue_in_usd") / col("ecommerce.total_item_quantity"))
# MAGIC          .sort(col("avg_purchase_revenue").desc())
# MAGIC         )
# MAGIC ```
# MAGIC
# MAGIC ####Selecting columns
# MAGIC ```
# MAGIC devices_df = events_df.select("user_id", "device")
# MAGIC ```
# MAGIC
# MAGIC ####Col function
# MAGIC ```
# MAGIC from pyspark.sql.functions import col
# MAGIC ```
# MAGIC
# MAGIC ####Selecting columns with aliasing
# MAGIC ```
# MAGIC locations_df = events_df.select(
# MAGIC     "user_id", 
# MAGIC     col("geo.city").alias("city"), 
# MAGIC     col("geo.state").alias("state")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ####Selecting columns and applying expressions
# MAGIC ```
# MAGIC apple_df = events_df.selectExpr("user_id", "device in ('macOS', 'iOS') as apple_user")
# MAGIC ```
# MAGIC
# MAGIC ####Dropping a columns
# MAGIC ```
# MAGIC anonymous_df = events_df.drop("user_id", "geo", "device")
# MAGIC no_sales_df = events_df.drop(col("ecommerce"))
# MAGIC ```
# MAGIC
# MAGIC ####Adding a column
# MAGIC ```
# MAGIC mobile_df = events_df.withColumn("mobile", col("device").isin("iOS", "Android"))
# MAGIC purchase_quantity_df = events_df.withColumn("purchase_quantity", col("ecommerce.total_item_quantity").cast("int"))
# MAGIC ```
# MAGIC
# MAGIC ####Renaming a column
# MAGIC ```
# MAGIC location_df = events_df.withColumnRenamed("geo", "location")
# MAGIC ```
# MAGIC
# MAGIC #### Filtering by a column/s
# MAGIC ```
# MAGIC purchases_df = events_df.filter("ecommerce.total_item_quantity > 0")
# MAGIC revenue_df = events_df.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
# MAGIC android_df = events_df.filter((col("traffic_source") != "direct") & (col("device") == "Android"))
# MAGIC ```
# MAGIC
# MAGIC #### Dropping duplicated rows
# MAGIC ```
# MAGIC events_df.distinct()
# MAGIC ```
# MAGIC
# MAGIC #### Dropping duplicates based on a specific row
# MAGIC ```
# MAGIC distinct_users_df = events_df.dropDuplicates(["user_id"])
# MAGIC ```
# MAGIC
# MAGIC ####Limiting rows
# MAGIC ```
# MAGIC limit_df = events_df.limit(100)
# MAGIC ```
# MAGIC
# MAGIC ####Sorting rows
# MAGIC ```
# MAGIC increase_timestamps_df = events_df.sort("event_timestamp")
# MAGIC decrease_timestamp_df = events_df.sort(col("event_timestamp").desc())
# MAGIC increase_sessions_df = events_df.orderBy(["user_first_touch_timestamp", "event_timestamp"])
# MAGIC decrease_sessions_df = events_df.sort(col("user_first_touch_timestamp").desc(), col("event_timestamp"))
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Aggregations
# MAGIC
# MAGIC #### Grouped data methods
# MAGIC Various aggregation methods are available on the <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank">GroupedData</a> object.
# MAGIC
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | agg | Compute aggregates by specifying a series of aggregate columns |
# MAGIC | avg | Compute the mean value for each numeric columns for each group |
# MAGIC | count | Count the number of rows for each group |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | min | Compute the min value for each numeric column for each group |
# MAGIC | pivot | Pivots a column of the current DataFrame and performs the specified aggregation |
# MAGIC | sum | Compute the sum for each numeric columns for each group |
# MAGIC
# MAGIC #### Grouping by column/s
# MAGIC ```
# MAGIC df.groupBy("event_name")
# MAGIC df.groupBy("geo.state", "geo.city")
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC #### Grouped Data Methods
# MAGIC ```
# MAGIC event_counts_df = df.groupBy("event_name").count()
# MAGIC avg_state_purchases_df = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
# MAGIC city_purchase_quantities_df = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity", "ecommerce.purchase_revenue_in_usd")
# MAGIC ```
# MAGIC
# MAGIC #### Built-In Functions
# MAGIC In addition to DataFrame and Column transformation methods, there are a ton of helpful functions in Spark's built-in <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html" target="_blank">SQL functions</a> module.
# MAGIC
# MAGIC #### Aggregate Functions
# MAGIC
# MAGIC Here are some of the built-in functions available for aggregation.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | approx_count_distinct | Returns the approximate number of distinct items in a group |
# MAGIC | avg | Returns the average of the values in a group |
# MAGIC | collect_list | Returns a list of objects with duplicates |
# MAGIC | corr | Returns the Pearson Correlation Coefficient for two columns |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | stddev_samp | Returns the sample standard deviation of the expression in a group |
# MAGIC | sumDistinct | Returns the sum of distinct values in the expression |
# MAGIC | var_pop | Returns the population variance of the values in a group |
# MAGIC
# MAGIC Use the grouped data method <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg" target="_blank">**`agg`**</a> to apply built-in aggregate functions
# MAGIC
# MAGIC This allows you to apply other transformations on the resulting columns, such as <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html" target="_blank">**`alias`**</a>.
# MAGIC
# MAGIC ```
# MAGIC from pyspark.sql.functions import sum
# MAGIC
# MAGIC state_purchases_df = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
# MAGIC
# MAGIC
# MAGIC from pyspark.sql.functions import avg, approx_count_distinct
# MAGIC
# MAGIC state_aggregates_df = (df
# MAGIC                        .groupBy("geo.state")
# MAGIC                        .agg(avg("ecommerce.total_item_quantity").alias("avg_quantity"),
# MAGIC                             approx_count_distinct("user_id").alias("distinct_users"))
# MAGIC                       )
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC #### Math Functions
# MAGIC Here are some of the built-in functions for math operations.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | ceil | Computes the ceiling of the given column. |
# MAGIC | cos | Computes the cosine of the given value. |
# MAGIC | log | Computes the natural logarithm of the given value. |
# MAGIC | round | Returns the value of the column e rounded to 0 decimal places with HALF_UP round mode. |
# MAGIC | sqrt | Computes the square root of the specified float value. |
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC from pyspark.sql.functions import cos, sqrt
# MAGIC
# MAGIC display(spark.range(10)  # Create a DataFrame with a single column called "id" with a range of integer values
# MAGIC         .withColumn("sqrt", sqrt("id"))
# MAGIC         .withColumn("cos", cos("id"))
# MAGIC        )
# MAGIC ```
# MAGIC
