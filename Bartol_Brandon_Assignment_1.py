from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *

sc = SparkContext("local")
spark = SparkSession.builder.getOrCreate()

#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles, 
# fare amount and total amount are more than 0.1 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p
'''
# Testing the helper code
# Set your file path here 
path="file:///C:/Users/brand/OneDrive/Documents/CS777/HW1/Data/"
testFile= path + "taxi-data-sorted-small.csv"

# Read the data in Spark DataFrame
testDataFrame = spark.read.format('csv').options(header='false', inferSchema='true',  sep =",").load(testFile)
#testDataFrame.show()

# Convert data from DataFrame to RDD
testRDD = testDataFrame.rdd.map(tuple)
# Filter the only the valid records using the predefined filter() function
taxilinesCorrected = testRDD.filter(correctRows)

taxilinesCorrected_df = taxilinesCorrected.toDF()
# Group by medallion and count distinct drivers
taxilinesCorrected_grouped = taxilinesCorrected_df.groupBy("_c0").agg(countDistinct("_c1").alias("driver_count"))
# Get the top ten taxis with the highest count of distinct drivers
top_ten_taxis = taxilinesCorrected_grouped.orderBy(col("driver_count").desc()).limit(10)
'''
#Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    

    sc = SparkContext(appName="Assignment-1")
    spark = SparkSession.builder.getOrCreate()
    # Helper code: read the data in Spark DataFrame
    df = spark.read.format('csv').options(header='false', inferSchema='true',  sep =",").load(sys.argv[1])
    # Convert data from DataFrame to RDD
    RDD = df.rdd.map(tuple)
    # Filter the only the valid records using the predefined filter() function
    taxilinesCorrected = RDD.filter(correctRows)
    # Convert back to DF
    taxilinesCorrected_df = taxilinesCorrected.toDF()
    rdd = sc.textFile(sys.argv[1])

    #Task 1
    #Your code goes here
    #Group by medalion number (c0) and count the number of drivers (c1) this will rename the count as driver_count
    results_1 = taxilinesCorrected_df.groupBy("_c0").agg(countDistinct("_c1").alias("driver_count"))
    # Return 10 highest driver counts
    results_1 = results_1.orderBy(col("driver_count").desc()).limit(10)
    results_1.coalesce(1).saveAsTextFile(sys.argv[2])


    #Task 2
    #Your code goes here
     # Divide fare amount by duration (in seconds/60) to get cost per minute for each driver
     # Column is callled mone per minute
    taxilinesCorrected_df = taxilinesCorrected_df.withColumn("minute_rate", col("_c11") / (col("_c4") / 60))
    # Group by the driver ID and take the average rate for each driver
    results_2 = taxilinesCorrected_df.groupBy("_c1").agg(avg("minute_rate").alias("avg_minute_rate"))
    # Get the top ten drivers with the highest average earnings per minute
    results_2 = results_2.orderBy(col("avg_minute_rate").desc()).limit(10)

    #savings output to argument
    results_2.coalesce(1).saveAsTextFile(sys.argv[3])


    #Task 3 - Optional 
    #Your code goes here

    #Task 4 - Optional 
    #Your code goes here


    sc.stop()