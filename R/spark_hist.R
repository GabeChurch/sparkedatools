#' A Histogram Function for SparklyR \cr
#' @description 
#' This function is especially useful for EDA on large Spark/Hive tables, it is designed to resemble the hist() function in native R. It should be noted that this implementation does differ from native R, and will "bucket" the data-points. \cr
#' All computation is efficient and distributed in native scala/Spark \cr
#' \cr
#' Automatic categorical/continuous variable inference. Type agnostic. ie (String/Numeric type inference is also built-in) \cr
#' It is adivsed to drop time/array/other columns (or those with nested datatypes) before running.  
#' @details
#' Important package requirements: \cr
#' \itemize{
#' \item{}{You must pass the com.gabechurch.sparkeda.jar to the spark configuration before initializing the connection. \cr
#' \cr
#' Add to your project like:  \code{conf$'sparklyr.jars.default'= "/system/path/to/sparkeda_2.11-2.07.jar"} }
#' \cr
#' \item{}{You must have an active sparkContext (sc) before using spark_hist()}
#' } \cr
#' Download the required jar at \href{www.gabechurch.com/sparkEDA}{www.gabechurch.com/sparkEDA}  (default future integration is in the works) \cr
#' \cr
#' Example selection of a spark table and graph\cr
#' \code{spark_table = tbl(sc, sql("select * from db.stock_samples_20m limit 100"))} \cr
#' \code{spark_hist(spark_table, 20L)}
#' 
#' @param sparklyr_table  is the spark table you will pass to the function. You can pass using a dplyr spark table (tbl).
#' @param num_buckets  will set the number of buckets for the Spark Histograms (on each numeric column). The default is 10 buckets 
#' @export 
spark_hist = function(sparklyr_table, num_buckets = 10L){
  library(ggplot2)
  library(purrr)
  library(dplyr)
  library(lazyeval)
  library(sparklyr)
  raw_df = spark_dataframe(sparklyr_table)
  collected = sparklyr::invoke_new(sc, "com.gabechurch.sparklyRWrapper") %>% 
    sparklyr::invoke("histBackEnd", raw_df, num_buckets) %>%
      sparklyr::invoke("collect")
  for (i in 1:length(collected)){
    current_column = collected[i] %>% flatten
    column_name = paste((current_column[2] %>% flatten), collapse=', ')
    barValues = current_column[3] %>% flatten %>% map(function(x) {
      cur_hist_bar = x
      names(cur_hist_bar)[1] = paste0(column_name)
      names(cur_hist_bar)[2] = "Counts"
      cur_hist_bar
    })
    prepped = barValues %>% map(flatten) %>% bind_rows() 
    require(scales)
    print(ggplot(data=prepped, aes_string(x=paste0(column_name), y='Counts')) + 
            geom_bar(stat="identity", width=0.5, col="darkgreen", fill="green", alpha=.2) + 
            scale_y_continuous(labels=comma, breaks = scales::pretty_breaks(n = 6)) +
            theme(axis.text.x = element_text(angle=60, hjust=1, vjust=0.5),
                  axis.title.x = element_text(margin = margin(t = 20, r = 0, b = 0, l = 0), size=15),
                  axis.title.y = element_text(margin = margin(t = 0, r = 15, b = 0, l = 0), size=15)
            )
    )
  }
}