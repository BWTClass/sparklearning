from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]") \
        .appName("Broadcast variable") \
        .getOrCreate()

    accvar = spark.sparkContext.accumulator(1)  # declare

    rdd = spark.sparkContext.parallelize([1, 2, 3])


    def func(x):
        accvar.add(x)

        """
        accvar = 1
        2
        4
        7
        """


    rdd.foreach(func)
    print("acc value: " + str(accvar.value))
