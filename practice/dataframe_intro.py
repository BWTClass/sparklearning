from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]") \
        .appName("Dataframe Intro") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.ui.port", "4050") \
        .getOrCreate()

    inputdata = Row(1, 2, 3)
    inputrdd = spark.sparkContext.parallelize(inputdata)

    # create dataframe from rdd
    inputdf = inputrdd.map(lambda x: (x,)).toDF()
    inputdf.printSchema()
    inputdf.show()

    inputdata1 = [("Java", "80"), ("Math", "75")]
    inputrdd1 = spark.sparkContext.parallelize(inputdata1)
    inputdf1 = inputrdd1.toDF(["subject", "mark"])
    # inputdf1.show()
    # inputdf1.printSchema()

    # create a dataframe using createDataframe()
    # dataframe from rdd
    inputdf2 = spark.createDataFrame(inputrdd1).toDF(*["subject", "mark"])
    # inputdf2.show()

    # dataframe from inputdata instead of rdd
    inputdf3 = spark.createDataFrame(inputdata1).toDF(*["subject", "mark"])
    # inputdf3.show()

    # dataframe from row type
    inputdata2 = [Row("Java", "80"), Row("Math", "75")]
    inputdf4 = spark.createDataFrame(inputdata2, ["subject", "mark"])
    # inputdf4.show()

    # create a dataframe with schema
    inputdata5 = [("Aditya", "IT", "Pune", 2000),
                  ("Ram", "HR", "Mumbai", 1500),
                  ("Shyam", "IT", "Pune", 9000)]

    schema5 = StructType([StructField("name", StringType(), True),
                          StructField("dept_name", StringType(), True),
                          StructField("city", StringType(), True),
                          StructField("salary", IntegerType(), True)])

    df5 = spark.createDataFrame(data=inputdata5, schema=schema5)
    # df5.printSchema()
    # df5.show()

    # create dataframe from datasource
    # create dataframe from csv file
    csvdf = spark.read.csv(path="C:\\Users\\tadit\\PycharmProjects\\sparklearning\\practice\\input_data\\zipcode_withoutheader.csv")
    # csvdf.printSchema()
    # csvdf.show()

    csvdf1 = spark.read.csv(path="C:\\Users\\tadit\\PycharmProjects\\sparklearning\\practice\\input_data\\employee.csv",schema=schema5)
    # csvdf1.show()
    # csvdf1.printSchema()
    #
    # input("Press enter to terminate")
    # spark.stop()

