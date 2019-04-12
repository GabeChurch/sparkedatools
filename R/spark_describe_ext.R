#' A more descriptive version of spark describe including distinct counts \cr
#' @description 
#' This function  is especially useful for EDA on large Spark/Hive tables, it is designed to resemble the hist() function in native R. It should be noted that this implementation does differ from native R, and will "bucket" the data-points. \cr
#' All computation is efficient and distributed in native scala/Spark \cr
#' \cr
#' It is adivsed to drop time/array/other columns (or those with nested datatypes) before running.  
#' @details
#' Important package requirements: \cr
#' Download the required jar at \href{www.gabechurch.com/sparkEDA}{www.gabechurch.com/sparkEDA}  (default future integration is in the works) \cr
#' \cr
#' Example selection of a spark table and graph\cr
#' \code{spark_table = tbl(sc, sql("select * from db.stock_samples_20m limit 100"))} \cr
#' \code{spark_hist(spark_table, 20L)}
#' @param sparklyr_table  is the spark table you will pass to the function. You can pass using a dplyr spark table (tbl).
#' @param round_at (default = 2L) controls the number of decimals values to round output counts to (for long outputs)
#' @export 
spark_describe_ext = function(sparklyr_table, round_at=2L){
  library(sparklyr)
  library(dplyr)
  library(purrr)
  library(lazyeval)
  summed = sdf_describe(sparklyr_table) %>% sdf_collect()
  collected = sparklyr::invoke_new(sc, 'com.gabechurch.sparklyRWrapper') %>% 
    sparklyr::invoke('getDistinctCounts', spark_dataframe(sparklyr_table), FALSE) %>% 
    sdf_collect()
  column_names = colnames(collected)
  descriptions = column_names %>% map(function(column_name){
    #Flatten  
    testout = summed %>% select(summary, paste0(column_name))
    df = add_rownames(testout) %>% gather(var, value, -rowname) %>% spread(rowname, value)
    newDF = df %>% filter(var != "summary")
    name_vals = df %>% filter(var == "summary") %>% unlist %>% unname
    colnames(newDF) = name_vals
    #colnames(df) = as.character(unlist(df[1,]))
    #df = df[-1,]
    
    #Combine with Flattened
    summary = as.data.frame(collected) %>% select(paste0(column_name))
    newDF["count_distinct"] = summary[,paste0(column_name)]
    newDF[c('summary','count','count_distinct','mean','stddev','min','max')]
  }) %>% rbind_all
  descriptions$mean = round(as.numeric(descriptions$mean), round_at)
  descriptions$stddev = round(as.numeric(descriptions$stddev), round_at)
  descriptions$max = round(as.numeric(descriptions$max), round_at)
  descriptions$min = round(as.numeric(descriptions$min), round_at)
  descriptions
}
