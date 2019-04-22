#' A SparklyR Kmeans Cluster Plotting Function \cr
#' @description 
#' The function can be used to generate 2D or 3D plots to visualize and understand kmean clusters  
#' @details
#' Important package requirements: \cr
#' You must have ggplot2 installed, and if you want the 3D output you must have plotly installed \cr
#' \cr
#' Example selection of a spark table and graph\cr
#' \code{spark_table = tbl(sc, sql("select * from db.stock_samples_20m limit 100"))} \cr
#' \code{outputs = spark_plot_kmeans(inputDF, kmean_model, plotMode="both")}
#' @param sparklyr_table  is the spark table you will pass to the function. You can pass using a dplyr spark table (tbl).
#' @param ml_kmean_model  is the ml_kmean model outputs to pass to the function
#' @param plotMode (default=2d this will generate the output visualization with ggplot, if set to 3d it will generate a 3d plot with plotly, if set to both it will output both. You should create some variable like both_plot = .... then access for plotting like both_plot$`2d_plot` and both_plot$`3d_plot`
#' @param optional_pca_model (default = "None") You can plug the existing pca model you have run on the dataframe with ml_pca and it will avoid re-running. By default the PCA selects k=2 for 2-dimension and k=3 for 3-dimension so if you use a different k in your model you may be missing out on dimensionality. (Not always a bad thing)
#' @param local_selection (default = 80000L) This is the randomly selected number of points that will ultimately be collected and plotted. The 3D model can handle up to 250,000 points (sometimes) and the 2D can handle more like 350-400,000. The default of 80,000 is set for browser performance (especially with the 3D plot).
#' @param combination (default = "experimental") This uses a custom version of sdf_bind_cols that is faster and solves errors that I have encountered with sdf_bind_cols (called indexJoin) note it does not have support for nested columns yet
#' @export 
spark_plot_kmeans = function(sparklyr_table, ml_kmean_model, plotMode="2d", optional_pca_model = "None", local_selection=80000L, combination="experimental"){
  library(sparklyr)
  library(dplyr)
  library(ggplot2)
  pca_dims = if (plotMode=="2d"){
    2L
  }else{
    3L
  }
  pca = if (optional_pca_model=="None"){
    ml_pca(sparklyr_table, features = colnames(sparklyr_table), k=pca_dims) 
  }else{
    optional_pca_model
  }
  projected_pca = pca %>% sdf_project(sparklyr_table,features=rownames(pca$pc))
  
  ## Getting the Cluster Centers from the PCA model and the Clusters for the graph
  spark_kmeans_centers = copy_to(sc, ml_kmean_model$centers)
  centerPreds = ml_predict(ml_kmean_model, spark_kmeans_centers)
  ClusterCenters = pca %>% sdf_project(centerPreds, features = rownames(pca$pc)) %>% collect() %>% as.data.frame()
  
  km_clusts = ml_predict(ml_kmean_model, sparklyr_table)
  km_cluster_preds = km_clusts %>% select(prediction)
  
  km_pca_combined = if(combination =="experimental"){
    km_cluster_preds_df = km_cluster_preds %>% spark_dataframe()
    projected_pca_df = projected_pca %>% spark_dataframe()
    joined_df = sparklyr::invoke_new(sc, "com.gabechurch.sparklyRWrapper") %>%
      sparklyr::invoke("indexJoin", projected_pca_df, km_cluster_preds_df)
    sdf_copy_to(sc, joined_df, "km_pca_combined")
  }else{
    projected_pca %>% sdf_bind_cols(km_cluster_preds)
  }
  
  df_length = sparklyr_table %>% count() %>% collect() %>% as.double()
  
  if (plotMode=="2d"){
    local = if (df_length < local_selection) {
      km_pca_combined %>% select(PC1, PC2, prediction) %>% collect() %>% as.data.frame()
    } else {
      km_pca_combined %>% select(PC1, PC2, prediction) %>% sdf_sample(local_selection/df_length, replacement = FALSE) %>% collect() %>% as.data.frame()
    }
    
    local %>%
      ggplot(aes(x= PC1, y=PC2)) +
      geom_point(data = ClusterCenters, aes(PC1, PC2, color = factor(prediction), label = prediction), size = 80, alpha = 0.3) +
      geom_point(aes(PC1, PC2, color = factor(prediction), label = prediction), size = 2, alpha = 0.5) +
      labs(x = paste0("PC1: ", round(pca$explained_variance[1], digits=4) *100, "% variance"),
           y = paste0("PC2: ", round(pca$explained_variance[2], digits=4) *100, "% variance")) +
      guides(fill = FALSE, color = FALSE) 
  }else if(plotMode=="3d"){
    library(plotly)
    local = if (df_length < local_selection) {
      km_pca_combined %>% select(PC1, PC2, PC3, prediction) %>% collect() %>% as.data.frame()
    } else {
      km_pca_combined %>% select(PC1, PC2, PC3, prediction) %>% sdf_sample(local_selection/df_length, replacement = FALSE) %>% collect() %>% as.data.frame()
    }
    
    plot_ly(local, x = ~PC1, y = ~PC2, z = ~PC3, color = ~as.factor(prediction), marker = list(
      opacity = 0.5,
      colorscale = 'Viridis',
      size = 5 
    ), 
    showlegend = F
    ) %>%
      layout(scene = list(xaxis = list(title = paste0("PC1: ", round(pca$explained_variance[1],digits=4)*100, "% variance")),
                          yaxis = list(title = paste0("PC2: ", round(pca$explained_variance[2], digits=4) *100, "% variance")),
                          zaxis = list(title = paste0("PC3: ", round(pca$explained_variance[3], digits=4) *100, "% variance"))))
    
  }else{
    library(plotly)
    local = if (df_length < local_selection) {
      km_pca_combined %>% select(PC1, PC2, PC3, prediction) %>% collect() %>% as.data.frame()
    } else {
      km_pca_combined %>% select(PC1, PC2, PC3, prediction) %>% sdf_sample(local_selection/df_length, replacement = FALSE) %>% collect() %>% as.data.frame()
    }
    
    output_plots = list(local %>%
                          ggplot(aes(x= PC1, y=PC2)) +
                          geom_point(data = ClusterCenters, aes(PC1, PC2, color = factor(prediction), label = prediction), size = 80, alpha = 0.3) +
                          geom_point(aes(PC1, PC2, color = factor(prediction), label = prediction), size = 2, alpha = 0.5) +
                          labs(x = paste0("PC1: ", round(pca$explained_variance[1], digits=4) *100, "% variance"),
                               y = paste0("PC2: ", round(pca$explained_variance[2], digits=4) *100, "% variance")) +
                          guides(fill = FALSE, color = FALSE) 
                        ,plot_ly(local, x = ~PC1, y = ~PC2, z = ~PC3, color = ~as.factor(prediction), marker = list(
                          opacity = 0.5,
                          colorscale = 'Viridis',
                          size = 5 
                        ), 
                        showlegend = F
                        ) %>%
                          layout(scene = list(xaxis = list(title = paste0("PC1: ", round(pca$explained_variance[1],digits=4)*100, "% variance")),
                                              yaxis = list(title = paste0("PC2: ", round(pca$explained_variance[2], digits=4) *100, "% variance")),
                                              zaxis = list(title = paste0("PC3: ", round(pca$explained_variance[3], digits=4) *100, "% variance"))))
                        
    )
    names(output_plots) = list("2d_plot", "3d_plot")
    output_plots
  }
}