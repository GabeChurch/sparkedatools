# sparkedatools
The missing SparklyR EDA toolkit (for use in R). Quick, efficient, and easy to use. 

Wrap and graph the outputs from the [SparkEDA package](https://github.com/GabeChurch/sparkeda) for Spark.

You will need a few things to get started. 

1) Grab the SparkEDA Jar from my website

2) Download this repository to your R home or other location
 Note: You will need devtools installed in R (as this package is not yet up on CRAN 
 ```r
 install.packages("devtools")
 library("devtools")
 ```
 Then 
 ```r
 devtools::install_github('GabeChurch/sparkedatools')
 ```
 
 3) Edit your SparklyR Configuration (in R)
 
You need to add the SparkEDA jar for the package to work in R. 

```r
conf = spark_config()
```
This is the important line
```r
#This is the configuration option
conf$'sparklyr.jars.default'= "/system/path/to/sparkeda_2.11-2.07.jar"
```
```r
sc = spark_connect(master = "yarn-client", config = conf, version = '2.3.2')
```
The ORDER IS IMPORTANT. Must be BEFORE you have connected and AFTER you have instantiated your spark_config in R.

  4) Enjoy being able to visualize and understand your giant data-sets like never before! 
  
  
