from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    sparkconf = SparkConf().setAppName("PYSPARK_RDD").setMaster("local[*]")
    sc = SparkContext(conf=sparkconf)
    # print(sc)

    # creating Spark Session
    spark = SparkSession.builder.master("").appName("Python Spark RDD basic example").getOrCreate()
    # print(spark)

    # RDD (Resilient Distributed Datasets)
    # Parallized Collection:
    inputdata = [1, 2, 3, 4, 5]
    inputrdd = sc.parallelize(inputdata)
    # print(inputrdd.collect())
    sparkinputrdd = spark.sparkContext.parallelize(inputdata)
    # print(sparkinputrdd.collect())

    # External Datasets
    inputrdd1 = sc.textFile("input_data/data.txt")
    # print(inputrdd1.collect())
    sparkinputrdd = spark.sparkContext.textFile("input_data/data.txt")
    # print(sparkinputrdd.collect())

    # RDD using wholeTextFile
    inputrdd = sc.wholeTextFiles("input_data/")
    # print(inputrdd.collect())

    sparkinputrdd = spark.sparkContext.wholeTextFiles("input_data/")
    # print(sparkinputrdd.collect())

    # empty RDD
    emptyrdd = spark.sparkContext.emptyRDD
    # print(emptyrdd.collect())

    # get number of partition
    # print("sparkinputrdd num partition : " + str(sparkinputrdd.getNumPartitions()))
    # print("inputrdd1 num partition : " + str(inputrdd1.getNumPartitions()))
    ## print("emptyrdd num partition : " + str(emptyrdd.getNumPartitions()))

    # repartition and coalesce
    # repartition() method which shuffles data from all nodes also called full shuffle and
    # second coalesce() method which shuffle data from minimum nodes
    # Note: repartition() or coalesce() methods also returns a new RDD.

    reparsparkinputrdd = sparkinputrdd.repartition(4)
    # print("after repartition : sparkinputrdd num partition : " + str(reparsparkinputrdd.getNumPartitions()))

    # RDD Transformation
    # flatMap(): Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.
    rddflatmap = inputrdd1.flatMap(lambda x: x.split(" "))
    # print(rddflatmap.collect())

    # map(): transformation is used the apply any complex operations like adding a column, updating a column e.t.c,
    # the output of map transformations would always have the same number of records as input.
    rddmap = rddflatmap.map(lambda x: (x, 1))
    # print(rddmap.collect())

    # reduceByKey(): merges the values for each key with the function specified.
    rddreducebykey = rddmap.reduceByKey(lambda x, y: x + y)
    # print(rddreducebykey.collect())

    # sortByKey(): sort RDD elements based on key
    rddsortbykey = rddreducebykey.map(lambda x: (x[1], x[0])).sortByKey()
    # print(rddsortbykey.collect())
    # print(rddreducebykey.sortByKey().collect())

    # filter()
    filterrdd = rddsortbykey.filter(lambda x: 'o' in x[1])
    # print(filterrdd.collect())

    # union():
    unionrdd = rddmap.union(rddreducebykey)
    # print(unionrdd.collect())

    # intersection(): Returns the dataset which contains elements in both source dataset and an argument
    intersecrdd = unionrdd.intersection(rddsortbykey)
    # print(intersecrdd.collect())

    # distinct()
    distinctrdd = rddflatmap.distinct()
    # print(rddflatmap.collect())
    # print(distinctrdd.collect())

    # cache(): Persist this RDD with the default storage level (MEMORY_ONLY).
    # print(distinctrdd.cache().collect())

    # persist(): is used to store the RDD to one of the storage levels
    # MEMORY_ONLY,MEMORY_AND_DISK, MEMORY_ONLY_SER, MEMORY_AND_DISK_SER, DISK_ONLY,
    # MEMORY_ONLY_2,MEMORY_AND_DISK_2
    import pyspark
    perdistrdd = intersecrdd.persist(pyspark.StorageLevel.MEMORY_ONLY)
    # print(perdistrdd.collect())
    perdistrdd1 = perdistrdd.unpersist()

    # cogroup(): For each key k in self or other,
    # return a resulting RDD that contains a tuple with the list of values for that key in self as well as other.
    x = sc.parallelize([("a", 1), ("b", 4)])
    y = sc.parallelize([("a", 2)])
    # print([(x, tuple(map(list, y))) for x, y in sorted(list(x.cogroup(y).collect()))])

    # def test(rdd1, rdd2):
    #     for a, b in rdd1.cogroup(rdd2).collect():
    #         print(a)
    #         print(b)
    #         print(tuple(map(list, b)))
    #
    # test(x, y)

    # groupByKey(): Group the values for each key in the RDD into a single sequence.
    # Hash-partitions the resulting RDD with numPartitions partitions
    print("================GroupByKey========================")
    print(rddmap.groupByKey().mapValues(list).collect())
    print("===================================================")

    # join
    x = spark.sparkContext.parallelize([("a", 1), ("b", 4)])
    y = spark.sparkContext.parallelize([("a", 2), ("a", 3)])
    print("======================Join========================")
    print(sorted(x.join(y).collect()))
    print("==================================================")

    # RDD Action:
    # count(): Returns the number of records in an RDD
    print("count: " + str(rddsortbykey.count()))

    # first()
    print("first element in RDD: " + str(rddsortbykey.first()))

    # max(): Return max record
    print("max record : " + str(rddsortbykey.max()))

    # reduce(): Reduces the records to single, we can use this to count or sum
    print(rddsortbykey.reduce(lambda a, b: (a[0] + b[0], a[1])))
    from operator import add
    print(spark.sparkContext.parallelize([1, 2, 3, 4, 5]).reduce(add))

    # take(): Returns the record specified as an argument.
    print(rddsortbykey.collect())
    print(rddsortbykey.take(2))

    # please refer broadcast_variable.py