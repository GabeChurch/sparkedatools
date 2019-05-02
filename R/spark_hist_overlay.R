#' A histogram with reponse function for a sparklyr table \cr
#' @description 
#' A histogram for plotting response ove the variables in a table. You will need to reduce the range to around 200 per variable to get effective plots for this method at the moment, as bucketing is not supported (yet).  
#' @details
#' You must have sparklyr and ggplot2 installed \cr
#' You must also have the sparkeda jar installed and referenced the same way as spark_hist \cr
#' You can change the plot output sizes with the chunk settings using knitR like {r fig.height=8, fig.width=20}
#' \cr
#' Example selection of a spark table and plot generation\cr
#' \code{adult_df = tbl(sc, sql("select * from sample_data.adult_dataset"))} \cr
#' \code{spark_hist_overlay(adult_df, "income"))}
#' @param sparklyr_table is the sparklyr table to pass to the function 
#' @param response_var is the string response variable you want to overlay the histograms with.
#' @param max_numeric_ticks 40 is the default, using over 40 is fine but you should increase the output width using knitR. 
#' @export 
spark_hist_overlay = function(sparklyr_table, response_var, max_numeric_ticks = 40){
  library(ggplot2)
  library(purrr)
  library(sparklyr)
  numericcharacters = function(x) {
    !any(is.na(suppressWarnings(as.numeric(x)))) & is.character(x)
  }
  
  decimalplaces <- function(x) {
    if ((x %% 1) != 0) {
      nchar(strsplit(sub('0+$', '', as.character(x)), ".", fixed=TRUE)[[1]][[2]])
    } else {
      return(0)
    }
  }
  
  scale_cont_k = function(k_ticks, x) {
    mini = min(x)
    maxi = max(x)
    seq_range = seq(from = mini, to = maxi, by = (maxi - mini)/k_ticks)
    max_decimals = (x %>% map(function(z){decimalplaces(z)}) %>% as.numeric() %>% max())
    print(max_decimals)
    final = seq_range %>% round(max_decimals)
    #print(final %>% paste(collapse=","))
    final
  }
  
  raw_df = sparklyr_table %>% spark_dataframe()
  collected = sparklyr::invoke_new(sc, "com.gabechurch.sparklyRWrapper") %>% 
    sparklyr::invoke("histResponse", raw_df, response_var) %>%
    sparklyr::invoke("collect")
  
  (1:length(collected)) %>% map(function(i){
    current_column = collected[i] %>% flatten
    column_name = paste((current_column[2] %>% flatten), collapse=', ')
    target_name = paste((current_column[3] %>% flatten), collapse=', ')
    column_type = paste((current_column[4] %>% flatten), collapse=', ')
    barValues = current_column[5] %>% flatten %>% map(function(row_list) {
      cur_hist_bar = (row_list %>% flatten)
      bar_name = if (column_type == 'numeric'){
        as.numeric(cur_hist_bar[1]) %>% as.list()
      }else{
        cur_hist_bar[1]
      }
      tar_bar_name = cur_hist_bar[2]
      bar_counts = cur_hist_bar[3]
      names(cur_hist_bar)[1] = paste0(column_name)
      names(cur_hist_bar)[2] = paste0(target_name)
      names(cur_hist_bar)[3] = "Counts"
      cur_hist_bar
    })
    prepped = barValues %>% map(flatten) %>% bind_rows() %>% as.data.frame() %>% 
      mutate_if(numericcharacters,as.numeric) #%>%

    require(scales)
    target_order = unlist((prepped[,1]), use.names=FALSE)
    prepped[,paste0(target_name)] = as.factor(prepped[,paste0(target_name)])
    valueCounts = length(rle(sort(prepped[,paste0(column_name)]))$values)
    
    if (prepped[,paste0(column_name)] %>% is.numeric & valueCounts < max_numeric_ticks){
      #print(paste0("keeping original numeric ticks for ", column_name))
      ggplot(data=prepped, 
             aes_string(x=paste0(column_name), 
                        y='Counts',
                        fill=paste0(target_name))
      ) + 
        geom_bar(stat="identity", width=0.75) + #, width=0.75, col="darkgreen", fill="green", alpha=.2) #+ 
        scale_y_continuous(labels=comma, breaks = scales::pretty_breaks()) + 
        scale_x_discrete(limits=target_order) +
        theme(axis.text.x = element_text(angle=60, hjust=1, vjust=0.5, size=11),
              axis.text.y = element_text(size=11),
              axis.title.x = element_text(margin = margin(t = 20, r = 0, b = 0, l = 0), size=15),
              axis.title.y = element_text(margin = margin(t = 0, r = 15, b = 0, l = 0), size=15)
        ) 
      
    } else if (prepped[,paste0(column_name)] %>% is.numeric & valueCounts > max_numeric_ticks ){
      #print(paste("rounding ticks shown to numeric_ticks", column_name))
      ggplot(data=prepped, 
             aes_string(x=paste0(column_name), 
                        y='Counts',
                        fill=paste0(target_name))
      ) + 
        geom_bar(stat="identity", width=0.75) + #, width=0.75, col="darkgreen", fill="green", alpha=.2) #+ 
        scale_y_continuous(labels=comma, breaks = scales::pretty_breaks()) + 
        scale_x_continuous(breaks = scale_cont_k(max_numeric_ticks, prepped[,paste0(column_name)])) + # breaks=pretty(prepped[,column_name], n=as.numeric(paste0(numeric_ticks)))) +
        theme(axis.text.x = element_text(angle=60, hjust=1, vjust=0.5, size=11),
              axis.text.y = element_text(size=11),
              axis.title.x = element_text(margin = margin(t = 20, r = 0, b = 0, l = 0), size=15),
              axis.title.y = element_text(margin = margin(t = 0, r = 15, b = 0, l = 0), size=15)
        ) 
    }
    else{
      ggplot(data=prepped, 
             aes_string(x=paste0(column_name), 
                        y='Counts',
                        fill=paste0(target_name))
      ) + 
        geom_bar(stat="identity", width=0.75) + #, width=0.75, col="darkgreen", fill="green", alpha=.2) #+ 
        scale_y_continuous(labels=comma, breaks = scales::pretty_breaks()) + 
        #scale_x_discrete(limits=target_order) +
        theme(axis.text.x = element_text(angle=60, hjust=1, vjust=0.5, size=11),
              axis.text.y = element_text(size=11),
              axis.title.x = element_text(margin = margin(t = 20, r = 0, b = 0, l = 0), size=15),
              axis.title.y = element_text(margin = margin(t = 0, r = 15, b = 0, l = 0), size=15)
        ) 
    }
  })
}