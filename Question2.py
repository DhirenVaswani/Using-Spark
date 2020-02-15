from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext



if __name__ == "__main__":
    
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.map(lambda x: x.split(","))
    lines = lines.filter(lambda x: float(x[4])>0)
    lines = lines.filter(lambda x: float(x[16])>0)
    minutes = lines.map(lambda x: (x[1],int(x[4])/60)).reduceByKey(add)
    totalamount = lines.map(lambda x: (x[1],float(x[16]))).reduceByKey(add)
    minutesandamount = minutes.join(totalamount)
    minutesandamount = minutesandamount.filter(lambda x: x[1][0]>0)
    minutesandamount2 = minutesandamount.map(lambda x:(x[0],x[1][1]/x[1][0]))
    a = minutesandamount2.top(10, key = lambda x : x[1])
    b= sc.parallelize(a)
    b.coalesce(1,True).saveAsTextFile(sys.argv[2])
    sc.stop()
