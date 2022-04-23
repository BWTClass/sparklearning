from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("First RDD").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # RDDs -  resilient distributed dataset
    # Parallelized Collections:
    data = [1, 2, 3, 4]
    parallelized_rdd = sc.parallelize(data)
    print("===== Parallelized RDD =========")
    print(parallelized_rdd.collect())
    print(parallelized_rdd.getNumPartitions())
    print("================================")

    # Existing Dataset RDD
    line = sc.textFile("C:\\Users\\tadit\\PycharmProjects\\sparklearning\\RDD\\input_test.txt")
    print("===== Existing Dataset RDD =========")
    print(line.collect())
    print(line.getNumPartitions())
    print("================================")



