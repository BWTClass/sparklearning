from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == '__main__':

    def convertCase(element):
        lst = element.split(" ")
        str = []
        for i in lst:
            if type(i) == str:
                str.append(i.upper())
            else:
                str.append(i)
        return ' '.join(str)


    import os, sys

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession \
        .builder \
        .appName("Spark UDF") \
        .master("local[*]") \
        .getOrCreate()

    data = [(1, "Ajay Jadhav"),
            (2, "Satish Patil"),
            (3, "Manish Babale")]
    df1 = spark.createDataFrame(data, ["id", "name"])
    # df1.printSchema()
    # df1.show()

    convertUDF = udf(lambda x: convertCase(x))

    df1.select(col("id"), convertUDF(col("name")).alias("name")).show()

    spark.udf.register("convertUDF", convertCase, StringType())
    df1.createOrReplaceTempView("NAME_TABLE")
    spark.sql("select id, convertUDF(Name) as Name from NAME_TABLE") \
        .show(truncate=False)
