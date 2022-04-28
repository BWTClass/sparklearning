from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    import os, sys

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession \
        .builder \
        .appName("Spark UDF") \
        .master("local[*]") \
        .getOrCreate()

    arraycol = ArrayType(StringType(), False)
    data = [
        ("Aditya,,Tambe", ["Python", "HTML", "CSS"], "Pune", "MH"),
        ("Ajay,,Wagh", ["Dart", "CSS"], "Mumbai", "MH"),
        ("Manoj,,Wakade", ["Dart", "HTML", "CSS"], "Pune", "MH")
    ]
    schema = StructType([
        StructField("name", StringType()),
        StructField("language", arraycol),
        StructField("city", StringType()),
        StructField("state", StringType())
    ])

    df = spark.createDataFrame(data, schema)
    # df.printSchema()
    # df.show()

    # explode function
    # Use explode() function to create a new row for each element in the given array column.
    df.select(df.name, explode(df.language)).show()

    # split() sql function returns an array type after splitting the string column by delimiter
    df.select(split(df.name, ",").alias("nameasarray")).show()
    df.select(split(df.name, ",").alias("nameasarray")).printSchema()

    # array() function to create a new array column by merging the data from multiple columns
    df.select(df.name, array(df.city, df.state).alias("address")).show()

    # array_contains() sql function is used to check if array column contains a value.
    df.select(df.name, array_contains(df.language, "HTML").alias("array_contains")).show()

    # Map Type
    schema1 = StructType([
        StructField("name", StringType()),
        StructField("address", MapType(StringType(), StringType()))
    ])
    data1 = [
        ("Ajay", {"city": "Pune", "state": "MH"}),
        ("Ashish", {"city": "Mumbai", "state": "MH"}),
        ("Ashwini", {"city": "Mumbai", "state": "MH"})
    ]

    df1 = spark.createDataFrame(data1, schema1)
    df1.printSchema()
    df1.show()

    #
    df2 = df1.rdd.map(lambda x: (x.name, x.address["city"], x.address["state"])).toDF(["name", "city", "state"])
    df2.printSchema()
    df2.show()

    df1.withColumn("city", df1.address.getItem("city")) \
        .withColumn("state", df1.address["state"]) \
        .drop("address") \
        .show()

    df1.select(df1.name,explode(df.address)).show()
