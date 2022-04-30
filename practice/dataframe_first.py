from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]") \
        .appName("Dataframe Intro") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.ui.port", "4050") \
        .getOrCreate()

    schema5 = StructType([StructField("name", StringType(), True),
                          StructField("dept_name", StringType(), True),
                          StructField("city", StringType(), True),
                          StructField("salary", IntegerType(), True),
                          StructField("id", StringType(), True)
                          ])

    csvdf1 = spark.read.csv(path=r"D:\gitclone\sparklearning\practice\input_data\employee1.csv", schema=schema5)
    # ithColumn() is a transformation function of DataFrame which is used to change the value,
    # convert the datatype of an existing column,
    # create a new column, and many more.
    csvdf1.printSchema()
    csvdf1.show()

    # change datatype using withcolumn()
    csvdf1.withColumn("id", col("id").cast("Integer")).printSchema()

    # update the value of existing column
    csvdf1.withColumn("salary", col("salary") * 1000).show()

    # adding new column to dataframe
    # lit() function: used to add constant value to dataframe
    csvdf1.withColumn("state", lit("MH")).show()

    # renaming column
    csvdf1.withColumnRenamed("name", "firstname").printSchema()

    # drop column
    csvdf1.drop("state").show()

    # filter
    csvdf1.filter(col("city") == "Pune").show()
    # filter with multiple conditions
    csvdf1.filter((col("city") == "Pune") & (col("salary") > 5000)).show()

    # filter based on input list
    lst = ["Pune","Nagar"]
    csvdf1.filter(csvdf1.city.isin(lst)).show()