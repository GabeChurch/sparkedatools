#' A Histogram Function for SparklyR UpdatedAgain \cr
#' @description 
#' This function  is especially useful for EDA on large Spark/Hive tables, it is designed to resemble the hist() function in native R. It should be noted that this implementation does differ from native R, and will "bucket" the data-points. \cr
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
#' @param num_buckets  (default=10L) will set the number of buckets for the Spark Histograms (on each numeric column). The default is 10 buckets  (set with 10L)
#' @param include_null  (default=FALSE) if TRUE will include a column with the null counts for each field in the histograms
#' @param print_plot (default=FALSE) if set to TRUE by default, you can return the ggplots in a list for furthur manipulation or modification if you set to false. (dashboards, theme changes, converting to plotly charts, etc) See details for more info/ideas.
#' @param decimal_places (default = 2L) controls the number of decimals values to round for histograms bucketed (if any)
#' @export 
spark_hist = function(sparklyr_table, num_buckets = 10L, include_null = FALSE, print_plot = TRUE, decimal_places = 2L){
  library(ggplot2)
  library(purrr)
  library(dplyr)
  library(lazyeval)
  library(sparklyr)
  raw_df = spark_dataframe(sparklyr_table)
  collected = sparklyr::invoke_new(sc, "com.gabechurch.sparklyRWrapper") %>% 
    sparklyr::invoke("histBackEnd", raw_df, num_buckets, decimal_places, include_null) %>%
    sparklyr::invoke("collect")
  plots = (1:length(collected)) %>% map(function(i){
    current_column = collected[i] %>% flatten
    column_name = paste((current_column[2] %>% flatten), collapse=', ')
    barValues = if(current_column[1] == "numerical"){
      current_column[3] %>% flatten %>% map(function(row_list) {
        cur_hist_bar = (row_list %>% flatten)
        bar_name = cur_hist_bar[1]
        bar_counts = cur_hist_bar[2]
        roundedIfNumeric = if (bar_name == "nulls"){
          cur_hist_bar
        }else{
          list(as.character(round(as.double(bar_name), digits=decimal_places)), bar_counts[[1]])
        }
        names(roundedIfNumeric)[1] = paste0(column_name)
        names(roundedIfNumeric)[2] = "Counts"
        roundedIfNumeric
      })
    }else{
      current_column[3] %>% flatten %>% map(function(row_list) {
        cur_hist_bar = (row_list %>% flatten)
        bar_name = cur_hist_bar[1]
        bar_counts = cur_hist_bar[2]
        names(cur_hist_bar)[1] = paste0(column_name)
        names(cur_hist_bar)[2] = "Counts"
        cur_hist_bar
      })
    }
    prepped = barValues %>% map(flatten) %>% bind_rows() 
    require(scales)
    target_order = unlist((prepped[,1]), use.names=FALSE)
    Counts='Counts'
    plot = if (include_null == TRUE){
      ggplot(data=prepped, 
             aes(x=!!ensym(column_name), 
                 y=!!ensym(Counts),
                 fill=factor({ifelse(!!ensym(column_name)=="nulls","Highlighted","Normal")}),
                 col=factor({ifelse(!!ensym(column_name)=="nulls","Highlighted","Normal")})
             )
      )+ 
        geom_bar(stat="identity", width=0.75, alpha=.2, show.legend=FALSE) + 
        scale_fill_manual(name = paste0(column_name), values=c("red","green")) +
        scale_color_manual(name = paste0(column_name), values=c("darkred","darkgreen")) +
        scale_y_continuous(labels=comma, breaks = scales::pretty_breaks(n = 6)) + 
        scale_x_discrete(limits=target_order) +
        theme(axis.text.x = element_text(angle=60, hjust=1, vjust=0.5),
              axis.title.x = element_text(margin = margin(t = 20, r = 0, b = 0, l = 0), size=15),
              axis.title.y = element_text(margin = margin(t = 0, r = 15, b = 0, l = 0), size=15)
        )
    }else{
      ggplot(data=prepped, 
             aes_string(x=paste0(column_name), 
                        y='Counts')
      ) + 
        geom_bar(stat="identity", width=0.75, col="darkgreen", fill="green", alpha=.2) + 
        scale_y_continuous(labels=comma, breaks = scales::pretty_breaks(n = 6)) + 
        scale_x_discrete(limits=target_order) +
        theme(axis.text.x = element_text(angle=60, hjust=1, vjust=0.5),
              axis.title.x = element_text(margin = margin(t = 20, r = 0, b = 0, l = 0), size=15),
              axis.title.y = element_text(margin = margin(t = 0, r = 15, b = 0, l = 0), size=15)
        )
      
    }
    if (print_plot == TRUE){
      print(plot)
    }else {
      outObject = list(column_name, plot)
      names(outObject[1]) = "chartName"
      names(outObject[2]) = "plot"
      outObject
    }
  })
  plots
}
