from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext



if __name__ == "__main__":

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    counts = lines.map(lambda x: x.split(",")).map(lambda x:(x[0],x[1])).distinct()
    reducedRDD = counts.map(lambda x: (x[0], 1)).reduceByKey(add)
    a = reducedRDD.top(10, key = lambda x : x[1])
    b= sc.parallelize(a)
    b.coalesce(1,True).saveAsTextFile(sys.argv[2])
    sc.stop()
