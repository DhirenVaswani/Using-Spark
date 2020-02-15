from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext



if __name__ == "__main__":
    
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.map(lambda x: x.split(","))
    lines = lines.filter(lambda x: float(x[5])>0)
    hourscount = lines.map(lambda x: (x[2][11:13],1)).reduceByKey(add)
    hoursprofitratio = lines.map(lambda x: (x[2][11:13],float(x[12])/float(x[5]))).reduceByKey(add)
    profitandcount= hoursprofitratio.join(hourscount)
    averageprofitratio=profitandcount.map(lambda x: (x[0],(x[1][0]/x[1][1])))
    first = averageprofitratio.top(1,lambda x: x[1])
    first = sc.parallelize(first)
    first.coalesce(1,True).saveAsTextFile(sys.argv[2])
    sc.stop()
