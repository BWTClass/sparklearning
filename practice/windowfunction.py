from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == '__main__':
    import os, sys

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession \
        .builder \
        .appName("Spark UDF") \
        .master("local[*]") \
        .getOrCreate()

    simpleData = (("James", "Sales", 3000),
                  ("Michael", "Sales", 4600),
                  ("Robert", "Sales", 4100),
                  ("Maria", "Finance", 3000),
                  ("James", "Sales", 3000),
                  ("Scott", "Finance", 3300),
                  ("Jen", "Finance", 3900),
                  ("Jeff", "Marketing", 3000),
                  ("Kumar", "Marketing", 2000),
                  ("Saif", "Sales", 4100)
                  )

    columns = ["employee_name", "department", "salary"]
    df = spark.createDataFrame(data=simpleData, schema=columns)
    df.printSchema()
    df.show(truncate=False)

    # row_number() window function is used to give the sequential row number starting from 1 to the result of each window partition.
    windowspec = Window.partitionBy("department").orderBy("salary")
    df.withColumn("rownumber",row_number()
                  .over(windowspec)).show()

    # rank() window function is used to provide a rank to the result within a window partition
    df.withColumn("ran",rank().over(windowspec)).show()

    # dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps.
    df.withColumn("denserank",dense_rank().over(windowspec)).show()

    # Lag
    df.withColumn("lag",lag("salary",2).over(windowspec)).show()

    # lead
    df.withColumn("lead",lead("salary",2).over(windowspec)).show()

    # windows specification
    windowspecagg = Window.partitionBy("department")
    df.withColumn("row",row_number().over(windowspecagg))\
        .withColumn("avg",avg(col("salary")).over(windowspec))\
        .withColumn("sum",sum(col("salary")).over(windowspec))\
        .select("department","avg","sum").show()


    # aggregate functions
    # approx_count_distinct()
    df.select(approx_count_distinct("salary")).show()