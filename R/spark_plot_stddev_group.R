#' A SparklyR Simple Plotting Method for comparing Data Deviance in Standard Deviations across groups of data \cr
#' @description 
#' This function creates an easily displayed bar-graph for examining data deviance 
#' @details
#' Important package requirements: \cr
#' You must have ggplot2 installed \cr
#' \cr
#' Example selection of a spark table and graph\cr
#' \code{spark_table = tbl(sc, sql("select * from sample_data.iris limit 100"))} \cr
#' \code{outputs = spark_plot_stddev_group(spark_table, predictor='Species')}
#' @param sparklyr_table  is the spark table you will pass to the function. You can pass using a dplyr spark table (tbl). YOU MAY ONLY INCLUDE 2 COLUMNS
#' @param group_col  is the target column to predict
#' @export 
spark_plot_stddev_group = function(sparklyr_table, group_col){
  inputCols = sparklyr_table %>% colnames() 
  otherCol = inputCols[!inputCols %in% group_col][1]
  sparklyr_table = sparklyr_table %>% sdf_register('sparklyr_table')
  
  initial_sql = stringr::str_interp("
    SELECT 
      STDDEV_POP(${otherCol}) AS stddev,
      AVG(${otherCol}) AS avg
    FROM
      sparklyr_table
  ")
  initial_tbl = tbl(sc, sql(initial_sql)) %>% as.data.frame()
  stddev = initial_tbl$stddev
  avg = initial_tbl$avg
  initial_sql2 = stringr::str_interp("
    SELECT 
      ${group_col},
      AVG((${otherCol} - ${avg})/${stddev}) AS z_score,
      CASE WHEN AVG((${otherCol} - ${avg})/${stddev}) < 0
        THEN 'below'
        ELSE 'above'
      END AS field_type
    FROM 
      sparklyr_table
    GROUP BY 
      ${group_col}
  ")
  collected = tbl(sc, sql(initial_sql2)) %>% collect() %>% as.data.frame()
  collected <- collected[order(collected$z_score), ]  # sort
  collected = collected[order(collected$z_score), ]  # sort
  collected[[paste0(group_col)]] = factor(collected[[paste0(group_col)]], levels=collected[[group_col]]) # required to maintain sortted order in plot
  
  ggplot(collected, aes_string(x=paste0(group_col), y='z_score', label='z_score')) + 
    geom_bar(stat='identity', aes(fill=field_type), width=.5)  +
    scale_fill_manual(name=paste0(otherCol), 
                      labels = c("Above Average", "Below Average"), 
                      values = c("above"="#00ba38", "below"="#f8766d")) + 
    labs(subtitle=paste0("Normalized ", otherCol), 
         title= "Diverging Bars") + 
    coord_flip()
}