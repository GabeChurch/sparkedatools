#' A PCA Plotting Tool for SparklyR ml_pca \cr
#' @description 
#' PCA Plot output with column names. Gives only the top two dimensions  
#' @details
#' You must have ggrepel installed \cr
#' \cr
#' Example selection of a spark table and graph\cr
#' \code{pca = ml_pca(tbl(sc, sql("select * from db.stock_samples_20m limit 100")))} \cr
#' \code{spark_plot_pca(pca))}
#' @param ml_pca_model is the spark pca model outputs table you will pass to the function.
#' @export 
spark_plot_pca = function(ml_pca_model){
  library(tibble)
  library(ggplot2)
  library(ggrepel)
  library(tidyr)
  library(dplyr)
  
  cust_theme <- function(base_size = 12, base_family = "sans"){
    theme_minimal(base_size = base_size, base_family = base_family) +
      theme(
        axis.text = element_text(size = 12),
        axis.title = element_text(size = 14),
        panel.grid.major = element_line(color = "grey"),
        panel.grid.minor = element_blank(),
        panel.background = element_rect(fill = "aliceblue"),
        strip.background = element_rect(fill = "lightgrey", color = "grey", size = 1),
        strip.text = element_text(face = "bold", size = 12, color = "black"),
        legend.position = "right",
        legend.justification = "top", 
        panel.border = element_rect(color = "grey", fill = NA, size = 0.5)
      )
  }
  
  as.data.frame(ml_pca_model$pc) %>%
    rownames_to_column(var = "labels") %>%
    mutate(x_2 = gsub("_trans", "", labels)) %>%
    mutate(x_2 = gsub("_", " ", x_2)) %>%
    mutate(x_2 = gsub("(^|[[:space:]])([[:alpha:]])", "\\1\\U\\2", x_2, perl = TRUE)) %>%
    mutate(x_2 = gsub("And", "and", x_2)) %>%
    ggplot(aes(x = PC1, y = PC2, color = x_2, label = x_2)) + 
    geom_point(size = 2, alpha = 0.6) +
    geom_text_repel() +
    labs(x = paste0("PC1: ", round(pca$explained_variance[1], digits=4) *100, "% variance"),
         y = paste0("PC2: ", round(pca$explained_variance[2], digits=4) *100, "% variance")) +
    cust_theme() + 
    guides(fill = FALSE, color = FALSE)
}