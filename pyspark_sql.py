# You can then use the DataFrame to run SQL queries, 
# apply transformations, and perform other operations.

#--------> PySPark SQL
# df.createOrReplaceTempView("mytable")
# results = spark.sql("SELECT * FROM mytable")
# results.show()

# --> Veiws
# Create a view
# spark.sql("CREATE VIEW myview AS SELECT * FROM mytable WHERE column2 > 5")

# --> With 
# with_query = spark.sql("WITH cte1 AS (SELECT column1 FROM mytable WHERE column2 > 5), cte2 AS (SELECT column2 FROM mytable WHERE column3 < 10) SELECT * FROM cte1 JOIN cte2 ON cte1.column1 = cte2.column2")
# with_query.show()

# Use the "WITH" clause to define a temporary result set
# with_query = spark.sql("WITH cte AS (SELECT column1, column2 FROM mytable WHERE column2 > 5) SELECT * FROM cte")
# with_query.show()

# Query the view
# view_query = spark.sql("SELECT * FROM myview")
# view_query.show()

# --> Subquery
# Perform a subquery
# sub_query = spark.sql("SELECT column1 FROM (SELECT column1, column2 FROM mytable) WHERE column2 > 5")
# sub_query.show()

# --> Nested Query
# Perform a nested query
# nested_query = spark.sql("SELECT * FROM mytable WHERE column1 IN (SELECT column1 FROM mytable WHERE column2 > 5)")
# nested_query.show()

# --> UDF
# from pyspark.sql.functions import udf

# # define the function
# def my_udf(x):
#     return x.upper()

# # register the UDF
# udf_example = udf(my_udf)

# # use the UDF in a query
# df.select(udf_example("column1").alias("Uppercase Column1")).show()