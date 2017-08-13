# Name : Ashwini Kadam
# Student ID: 800967986
# mail: akadam3@uncc.edu
#
# Assignment 4: linreg.py
# Standalone Python/Spark program to perform linear regression.
# Performs linear regression by computing the summation form of the
# closed form expression for the ordinary least squares estimate of beta.
# 
# Takes the yx file as input, where on each line y is the first element and the remaining elements constitute the x.
#
# Usage: spark-submit linreg.py <inputdatafile> <outputfile>
#

import sys
import numpy as np

from pyspark import SparkContext


if __name__ == "__main__":
  if len(sys.argv) !=3:		# 3 arguments - first - program file name, 2nd input file name
    print >>"Usage: linreg <datafile> <outputfile>"
    exit(-1)

sc = SparkContext(appName="LinearRegression")

def mapAb(line):
    # Split data input into values y : x1 x2 x3..... xn (for multiple linear regression)
    # Compute matricies A and b and return them in list of tuples with related key.
    # Here, A = summation of (xi * transpose(xi)) for i = 1...n
    # and   b = summation of (yi * transpose(xi)) for i = 1...n

    x = np.matrix([1.0]+[float(val) for val in line[1:]])
    y = np.matrix([float(line[0])])
    return [('a', x.T * x),('b', x.T * y)]

# Input yx file has y_i as the first element of each line 
# and the remaining elements constitute x_i
yxinputFile = sc.textFile(sys.argv[1])
yxlines = yxinputFile.map(lambda line: line.split(','))
#beta = np.zeros(len(yxlines.collect()), dtype=float)

# - flatMap maps input into list of tuples (K, V)
# - reduceByKey sorts and reduces the list
# - collect is a simple action that forces execution of flatMap and reduceByKey,
# which are transforamtion (executed lazy)
ab = yxlines.flatMap(mapAb).reduceByKey(lambda total, mat: total + mat).collect()

A = ab[0][1]  # first element of the list, second element in the tuple
b = ab[1][1]  # second element of the list, second element in the tuple

# Calcutating beta coeffients here, beta0 is intercept and b1, b2... are x-coeffients
beta = A.I * b

# Printing all beta coefficients on the console
for coeff in beta:
      print coeff

# save result into HDFS output file
result = sc.parallelize(beta)
out_path = sys.argv[2]
result.saveAsTextFile(out_path)
sc.stop()
