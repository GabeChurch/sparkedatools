% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/spark_hist.R
\name{spark_hist}
\alias{spark_hist}
\title{A Histogram Function for SparklyR UpdatedAgain \cr}
\usage{
spark_hist(sparklyr_table, num_buckets = 10L, include_null = FALSE,
  print_plot = TRUE, decimal_places = 2L)
}
\arguments{
\item{sparklyr_table}{is the spark table you will pass to the function. You can pass using a dplyr spark table (tbl).}

\item{num_buckets}{(default=10L) will set the number of buckets for the Spark Histograms (on each numeric column). The default is 10 buckets  (set with 10L)}

\item{include_null}{(default=FALSE) if TRUE will include a column with the null counts for each field in the histograms}

\item{print_plot}{(default=FALSE) if set to TRUE by default, you can return the ggplots in a list for furthur manipulation or modification if you set to false. (dashboards, theme changes, converting to plotly charts, etc) See details for more info/ideas.}

\item{decimal_places}{(default = 2L) controls the number of decimals values to round for histograms bucketed (if any)}
}
\description{
This function  is especially useful for EDA on large Spark/Hive tables, it is designed to resemble the hist() function in native R. It should be noted that this implementation does differ from native R, and will "bucket" the data-points. \cr
All computation is efficient and distributed in native scala/Spark \cr
\cr
Automatic categorical/continuous variable inference. Type agnostic. ie (String/Numeric type inference is also built-in) \cr
It is adivsed to drop time/array/other columns (or those with nested datatypes) before running.
}
\details{
Important package requirements: \cr
\itemize{
\item{}{You must pass the com.gabechurch.sparkeda.jar to the spark configuration before initializing the connection. \cr
\cr
Add to your project like:  \code{conf$'sparklyr.jars.default'= "/system/path/to/sparkeda_2.11-2.07.jar"} }
\cr
\item{}{You must have an active sparkContext (sc) before using spark_hist()}
} \cr
Download the required jar at \href{www.gabechurch.com/sparkEDA}{www.gabechurch.com/sparkEDA}  (default future integration is in the works) \cr
\cr
Example selection of a spark table and graph\cr
\code{spark_table = tbl(sc, sql("select * from db.stock_samples_20m limit 100"))} \cr
\code{spark_hist(spark_table, 20L)}
}
