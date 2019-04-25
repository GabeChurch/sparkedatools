#' A Correlation Matrix Plotting Function for SparklyR \cr
#' @description 
#' Useful for EDA plotting of ml_on large Spark/Hive tables, it is designed to resemble the ggcorplot2() function in native R. \cr
#' @details
#' Example corrplot using sparklyR \cr
#' \code{spark_cars = copy_to(sc, mtcars)} \cr
#' \code{s_corr_mat = ml_corr(spark_cars, columns=colnames(spark_cars), method="pearson")} \cr
#' \code{spark_plot_corr(s_corr_mat)} 
#' @param sparklyr_corr_matrix  is the sparklyr ml_corr() output you will pass to the function. 
#' @export 
spark_plot_corr = function(sparklyr_corr_matrix){
  library(dplyr)
  library(reshape2)
  library(ggplot2)
  
  rownames(sparklyr_corr_matrix) = colnames(sparklyr_corr_matrix)
  
  # Get lower triangle of the correlation matrix
  get_lower_tri<-function(corr_mat){
    corr_mat[upper.tri(corr_mat)] <- NA
    return(corr_mat)
  }
  # Get upper triangle of the correlation matrix
  get_upper_tri <- function(corr_mat){
    corr_mat[lower.tri(corr_mat)]<- NA
    return(corr_mat)
  }
  
  s_corr_melt = sparklyr_corr_matrix %>% as.matrix() %>% get_lower_tri() %>% melt(na.rm = TRUE)
  
  ## Plotting the Outputs
  library(ggplot2)
  ggplot(data = s_corr_melt %>% melt(), aes(x=Var1, y=Var2, fill=value)) + 
    geom_tile(color = "white") + 
    theme_minimal() +
    theme(axis.text.x = element_text(angle=60, hjust=1, size=11) #, hjust=1, vjust=0.5)
          ,axis.text.y = element_text(size=11)
          ,axis.title.x = element_text(margin = margin(t = 0, r = 0, b = 0, l = 0), size=10)
          ,axis.title.y = element_text(margin = margin(t = 0, r = 0, b = 0, l = 0), size=10)
    ) +
    scale_fill_gradient2(low = "blue", high = "red", mid = "white", 
                         midpoint = 0, limit = c(-1,1), space = "Lab", 
                         name="Correlation") + 
                         coord_fixed()
}