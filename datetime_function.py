from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    import sys, os

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Datetime function") \
        .getOrCreate()

    data = [
        ["1", "2022-04-01"],
        ["2", "2022-04-02"],
        ["3", "2022-04-03"],
        ["4", "2022-04-04"]
    ]

    df = spark.createDataFrame(data, ["id", "input"])
    # df.printSchema()
    # df.show()

    schema_data = StructType([
        StructField("id",StringType()),
        StructField("input",DateType())
    ])

    from datetime import datetime
    data1 = [
        ["1", datetime.strptime("2022-04-01","%Y-%m-%d")],
        ["2", datetime.strptime("2022-04-02","%Y-%m-%d")],
        ["3", datetime.strptime("2022-04-03","%Y-%m-%d")],
        ["4", datetime.strptime("2022-04-04","%Y-%m-%d")]
    ]
    df1 = spark.createDataFrame(data1,schema_data)
    # df1.printSchema()
    # df1.show()

    # date_format()
    # Converts a date/timestamp/string to a value of string
    # in the format specified by the date format
    # given by the second argument.
    df.select(date_format(col("input"),"dd-MM-yyyy")).show()
    df1.select(date_format("input","dd-MM-yyyy")).show()

    # to_date()
    df.select(to_date(col("input"))).show()

    # add_months(), date_add() and date_sub()
    df.select(add_months("input",2)).show()
    df.select(add_months(col("input"),-2)).show()
    df.select(date_add("input",2)).show()
    df.select(date_sub("input",2)).show()

    # datediff - output in days
    df.select(datediff(lit(datetime.strptime("2022-04-06","%Y-%m-%d")),col("input"))).show()