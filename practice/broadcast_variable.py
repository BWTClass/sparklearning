from pyspark.sql import SparkSession

# broadcast_variable:  Readonly variable
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]") \
        .appName("Broadcast Variable Example") \
        .getOrCreate()

    states = {"MH": "Maharashtra", "MP": "Madhyapradesh", "GJ": "Gujarat"}
    broadcaststate = spark.sparkContext.broadcast(states)

    inputdatardd = spark.sparkContext.textFile("input_data/employee_data.txt")
    print(inputdatardd.getNumPartitions())

    def changestateval(x):
        # 101,Aditya,Tambe,India,MH
        xsplit = x.split(",")  # list [101,Aditya,Tambe,India,MH]
        testlist = []
        for i in xsplit:
            try:
                i = broadcaststate.value[i]
            except:
                pass
            testlist.append(i)
        return ','.join(testlist) # string comma

    print(inputdatardd.map(lambda x: changestateval(x)).collect())


    # next : accumulator_variable.py
