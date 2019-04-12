#' An EDA Dashboard Builder for SparklyR updated \cr
#' @description 
#' It is adivsed to drop time/array/other columns (or those with nested datatypes) before running.  
#' @details
#' Important package requirements: \cr
#' Download the required jar at \href{www.gabechurch.com/sparkEDA}{www.gabechurch.com/sparkEDA}  (default future integration is in the works) \cr
#' \cr
#' Example selection of a spark table and graph\cr
#' \code{spark_table = tbl(sc, sql("select * from db.stock_samples_20m limit 100"))} \cr
#' \code{spark_hist(spark_table, 20L)}
#' @param sparklyr_table  is the spark table you will pass to the function. You can pass using a dplyr spark table (tbl).
#' @param hist_num_buckets  (default=10L) will set the number of buckets for the Spark Histograms (on each numeric column). The default is 10 buckets  (set with 10L)
#' @param hist_include_null  (default=FALSE) if TRUE will include a column with the null counts for each field in the histograms
#' @param hist_decimal_places (default = 2L) controls the number of decimals values to round for histograms bucketed (if any)
#' @param desc_decimal_places (default = 2L) controls the number of decimals
#' @export 
spark_eda_dash = function(sparklyr_table, hist_num_buckets = 10L, hist_include_null = FALSE, hist_decimal_places = 2L, desc_decimal_places=2L){
  
  library(flexdashboard)
  library(sparklyr)
  library(tidyverse)
  library(dplyr)
  #library(plotly)
  library(tidyr)
  library(DT)
  library(ggplot2)
  library(purrr)
  library(lazyeval)
  
  extra_described  = sparkedatools::spark_describe_ext(sparklyr_table, desc_decimal_places)
  returned = sparkedatools::spark_hist(sparklyr_table, hist_num_buckets, hist_include_null, print_plot=FALSE, hist_decimal_places) 
  
  # Grouping the produced plots into equal length lists 
  subgroups = 2 
  n = length(returned)
  #must use <<- to create a "global" variable for knitR
  grouped_plots <<- split(returned, rep(1:ceiling(n/subgroups), each=subgroups)[1:n])
  # Grouping the description tables into the same subgroups, ordered so that we can extract them in the same manner. 
  grpd_ordrd_descriptions <<- grouped_plots %>% map(function(plotGroup){
    plotAName = plotGroup[[1]][[1]]
    plotADescOrig = extra_described %>% filter(summary == paste0(plotAName)) %>% select(-summary)
    plotADescription = plotADescOrig %>% 
      datatable(plotADescOrig, rownames = FALSE, options = list(
        dom = 't',
        columnDefs = list(list(className='dt-center', targets = "_all")),
        autoWidth = TRUE,
        initComplete = JS(
          "function(settings, json) {",
          "$(this.api().table().header()).css({'font-size': '12px', 'background-color': '#3579DC', 'opacity':'0.75', 'color': '#fff'});",
          "}"))) %>%  formatStyle(columns = colnames(plotADescOrig), 'font-size' = '13px')
    outputGroup = if(length(plotGroup) == 1){
      list(plotADescription)
    }else{
      plotBName = plotGroup[[2]][[1]]
      plotBDescOrig = extra_described %>% filter(summary == paste0(plotBName)) %>% select(-summary)
      plotBDescription = datatable(plotBDescOrig, rownames=FALSE, options = list(
        dom = 't',
        columnDefs = list(list(className='dt-center', targets = "_all")),
        autoWidth = TRUE,
        initComplete = JS(
          "function(settings, json) {",
          "$(this.api().table().header()).css({'font-size':'12px','background-color':'#3579DC','opacity':'0.75','color':'#fff'});",
          "}")
      )) %>%  formatStyle(columns = colnames(plotBDescOrig), 'font-size' = '13px')
      list(plotADescription, plotBDescription)
    }
  }) 
  #Knitting the flexdashboard 
  final_output = (1:length(grouped_plots)) %>% map(function(i){
    plotGroup = grouped_plots[[i]]
    plotA = plotGroup[[1]]
    plotName = plotA[[1]]
    a1 = knitr::knit_expand(text = "\nRow \n--------------------------------------------")
    a2 <- knitr::knit_expand(text = sprintf("\n### %s Distribution \n```{r %s}", plotName, plotName)) # start r chunk
    a3 <- knitr::knit_expand(text = sprintf("\n grouped_plots[[%d]][[1]][[2]]", i)) 
    a4 <- knitr::knit_expand(text = "\n```\n") # end r chunk
    plotAOut = (paste(a1, a2, a3, a4, collapse = '\n'))

    ## Returning the matched table Information 
    #plotADescription = grpd_ordrd_descriptions[[i]][[1]]
    c1 = knitr::knit_expand(text = "\nRow { .colored } \n--------------------------------------------")
    c2 <- knitr::knit_expand(text = sprintf("\n### %s Summary \n```{r %s Info}", plotName, plotName)) # start r chunk
    c3 <- knitr::knit_expand(text = sprintf("\n grpd_ordrd_descriptions[[%d]][[1]]", i)) 
    c4 <- knitr::knit_expand(text = "\n```\n") # end r chunk
    descAOut  = (paste(c1, c2, c3, c4, collapse = '\n'))
    
    outputCombined = if(length(plotGroup) == 1){
      list(plotAOut, descAOut)
    }else{
      plotB = plotGroup[[2]]
      plotNameB = plotB[[1]]
      b1 = knitr::knit_expand(text = "\nRow \n--------------------------------------------")
      b2 <- knitr::knit_expand(text = sprintf("\n### %s Distribution \n```{r %s}", plotNameB, plotNameB)) # start r chunk
      b3 <- knitr::knit_expand(text = sprintf("\n grouped_plots[[%d]][[2]][[2]]", i)) 
      b4 <- knitr::knit_expand(text = "\n```\n") # end r chunk
      plotBOut = (paste(b2, b3, b4, collapse = '\n'))
      
      #plotBDescription = grpd_ordrd_descriptions[[i]][[2]]
      d2 <- knitr::knit_expand(text = sprintf("\n### %s Summary \n```{r %s Info}", plotNameB, plotNameB)) # start r chunk
      d3 <- knitr::knit_expand(text = sprintf("\n grpd_ordrd_descriptions[[%d]][[2]]", i)) 
      d4 <- knitr::knit_expand(text = "\n```\n") # end r chunk
      descBOut = (paste(d2, d3, d4, collapse = '\n'))
      list(plotAOut, plotBOut, descAOut, descBOut)
    }
    outputCombined
  }) %>% flatten 
  final_output
}