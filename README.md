# MultipleLinearRegression
Implementation of multiple linear regression using spark. 

I am using the closed form expression for the ordinary least squares estimate of the linear regression coefficients computed using summation.

I am providing a standalone python spark (pyspark) program here which assumes that input data is in HDFS. I am also providing you with a few example input data files to test your code. The input files will be in CSV (comma separated values) format, with each line having the y value followed by the corresponding x values. Program should output the linear coefficients of the linear regression model (including that for the intercept). 

To run a program fom command line: 

$ spark-submit <mysparkprogram.py> <inputdatafile>
