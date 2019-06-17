#' A pairs plot generation tool for sparklyr tables \cr
#' @description 
#' This is a lightweight and simple wrapper around GGally for SparklyR tables. It will randomly sample 80,000 records
#' @details
#' You must have  GGally, sparklyr, dplyr, and ggplot2 installed \cr
#' Suggested to store the outputs, and check the chunk settings (as follows) on the stored output to avoid re-running.
#' You can change the plot output sizes with the chunk settings using knitR like {r fig.height=12, fig.width=12}
#' \cr
#' Example selection of a spark table and plot generation\cr
#' \code{iris_df = copy_to(sc, iris, name="iris_df")} \cr
#' \code{spark_plot_pairs(sparklyr_table = iris_df, label_col = "Species")}
#' @param sparklyr_table is the sparklyr table to pass to the function 
#' @param label_col is the column you want to label in the plot.
#' @param sample_size 80000 is the default, you can increase this if you wish however it may lead to poor performance. 
#' @param my_title is the tile of your pairs plot.
#' @param progress is FALSE by default, is you change it will produce status bars to let you know the speed. 
#' @export 
spark_plot_pairs = function(sparklyr_table, 
                            label_col,
                            sample_size = 80000,
                            my_title = "Pairs Plot", 
                            progress = FALSE
) {
  library(ggplot2)
  library(GGally)
  df_length = sparklyr_table %>% count() %>% collect() %>% as.double()
  local_df = if (df_length <= sample_size){
    sparklyr_table %>% collect() %>% as.data.frame()
  }else {
    sparklyr_table %>% sdf_sample(sample_size/df_length, replacement = FALSE) %>% collect() %>% as.data.frame()
  }
  input_cols = (local_df %>% colnames) %>% as.list()
  selected_cols = input_cols[(input_cols %in% label_col)==FALSE]
  
  ggpairs(local_df, columns=paste0(selected_cols), aes_string(color=paste0(label_col)), progress=progress) + 
    ggtitle(paste0(my_title))
  
}