#SPARK
##Description
The goal of this assignment is to implement a set of Spark programs in python (using Apache
Spark). Specifically, your Spark jobs will analyzing a data set consisting of New York City Taxi trip
reports in the Year 2013. The dataset was released under the FOIL (The Freedom of Information Law)
and made public by Chris Whong (https://chriswhong.com/open-data/foil_nyc_taxi/).
##Dataset
The data set itself is a simple text file. Each taxi trip report is a different line in the file. Among other
things, each trip report includes the starting point, the drop-off point, corresponding timestamps, and
information related to the payment. The data are reported by the time that the trip ended, i.e., upon
arrive in the order of the drop-off timestamps.
The attributes present on each line of the file are, in order:
Attribute Description
0 medallion an md5sum of the identifier of the taxi - vehicle bound (Taxi ID)
1 hack_license an md5sum of the identifier for the taxi license (Driver ID)
2 pickup_datetime time when the passenger(s) were picked up
3 dropoff_datetime time when the passenger(s) were dropped off
4 trip_time_in_secs duration of the trip
5 trip_distance trip distance in miles
6 pickup_longitude longitude coordinate of the pickup location
7 pickup_latitude latitude coordinate of the pickup location
8 dropoff_longitude longitude coordinate of the drop-off location
9 dropoff_latitude latitude coordinate of the drop-off location
10 payment_type the payment method -credit card or cash
11 fare_amount fare amount in dollars
12 surcharge surcharge in dollars
13 mta_tax tax in dollars
14 tip_amount tip in dollars
15 tolls_amount bridge and tunnel tolls in dollars
16 total_amount total paid amount in dollars

##Obtaining the Dataset
We are providing two versions of the data set. The first is a small one that you can use for debugging.
We strongly recommend that you write your code and execute it on your laptop using this data set first.
Only once you are sure that your implementation is working on your laptop should you try the larger
data set. This is going to save money, but more importantly, it is going to save time!
- Small data set. (93 MB compressed, uncompressed 384 MB) for implementation and
testing purposes (roughly 2 million taxi trips).
This is available at Amazon S3: https://s3.amazonaws.com/metcs777/taxi-data-sorted-small.csv.bz2
or as direct S3 address, so you can use it in a MapReduce job:
s3://metcs777/taxi-data-sorted-small.csv.bz2
- Larger data set. (8.8 GB compressed, uncompressed 33.3 GB) for your final data
analyzing (roughly 173 million taxi trips).
This is available at Amazon S3: https://s3.amazonaws.com/metcs777/taxi-data-sorted-large.csv.bz2
or as direct S3 address, so you can use it in a MapReduce job:
s3://metcs777/taxi-data-sorted-large.csv.bz2


##Assignment Tasks:
###Task 1
Many different taxis have had multiple drivers. Write and execute a Spark Python program that
computes the top ten taxis that have had the largest number of drivers. Your output should be a set of
(medallion, number of drivers) pairs.
NOTE - 1: You should consider that this is a real world data set that might include wrongly formatted
data lines. You should clean up the data before the main processing, a line might not include all of the
fields. If a data line is not correctly formatted, you should drop that line and do not consider it.
###Task 2 
We would like to figure out who the top 10 best drivers are in terms of their average earned money per
minute spent carrying a customer. The total amount field is the total money earned on a trip. In the end,
we are interested in computing a set of (driver, money per minute) pairs.
###Task 3
We would like to know which hour of the day is the best time for drivers that has the highest profit per
miles. Consider the surcharge amount in dollar for each taxi ride (without tip amount) and the distance in
miles, and sum up the rides for each hour of the day (24 hours) – consider the pickup time for your calculation.
The profit ratio is the ration surcharge in dollar divided by the travel distance in miles for each specific time of
the day.
Profit Ratio = (Surcharge Amount in US Dollar) / (Travel Distance in miles)
We are interested to know the time of the day that has the highest profit ratio.

