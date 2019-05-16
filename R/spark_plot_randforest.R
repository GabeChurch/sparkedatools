#' A SparklyR Random Forest Classification Model Plotting Function \cr
#' @description 
#' This function can be used to generate plots of the underlying decision trees used in the spark random forest classification model
#' @details
#' Important package requirements: \cr
#' You MUST have the sparklyr, igraph, and purrr packages installed to use this function \cr
#' You MUST have an active spark_context named "sc" \cr
#' \cr
#' Example selection of a spark table and graph\cr
#' \code{spark_table = tbl(sc, sql("select * from db.stock_samples_20m limit 100"))} \cr
#' \code{outputs = spark_plot_kmeans(inputDF, kmean_model, plotMode="both")}
#' @param sparklyr_table  is the spark table you will pass to the function. You can pass using a dplyr spark table (tbl) This could be the test or train set you want to use for prediction generation.
#' @param ml_rf_model  is the ml_random_forest model output you pass to this function
#' @param show_stats  (default=TRUE)  This will include the metrics in each 
#' @param plot_treeIDs (default="all") You can plot specific Trees like plot_treeIDs = list(1,4,5) where 1,4,5 are the target treeIDs you want to plot
#' @param hdfs_temp_path (default = "/tmp/RandomForestClassificationModels/") You should change this path to another location if you do not have permission to write in the hdfs or local /tmp directory. This function must write the spark RandomForestRegressionModel to hdfs temporarily to access certain model specs needed. 
#' @export 
spark_plot_randforest = function(sparklyr_table, ml_rf_model, show_stats=TRUE, plot_treeIDs="all", hdfs_temp_path = "/tmp/RandomForestClassificationModels"){
  library(purrr)
  library(sparklyr)
  library(igraph)
  
  preds = ml_predict(ml_rf_model, sparklyr_table)
  
  time = round(as.numeric(Sys.time())*1000, 0)
  
  path_date = paste0(hdfs_temp_path, '/', time)
  
  ml_rf_model$model %>% spark_jobj() %>% sparklyr::invoke("save", path_date)
  
  rf_spec <- spark_read_parquet(sc,name="rf_spec", path=paste0(path_date, "/data"))
  
  meta <- preds %>% 
    select(features) %>% 
    spark_dataframe() %>% 
    sparklyr::invoke("schema") %>% sparklyr::invoke("apply", 0L) %>% 
    sparklyr::invoke("metadata") %>% 
    sparklyr::invoke("getMetadata", "ml_attr") %>% 
    sparklyr::invoke("getMetadata", "attrs") %>% 
    sparklyr::invoke("json") %>%
    jsonlite::fromJSON() %>% 
    dplyr::bind_rows() %>% 
    copy_to(sc, .) %>%
    dplyr::rename(featureIndex = "idx")
  
  full_rf_spec <- rf_spec %>% 
    spark_dataframe() %>% 
    sparklyr::invoke("selectExpr", list("treeID", "nodeData.*", "nodeData.split.*"))
  
  selected = full_rf_spec %>% 
    sparklyr::invoke("drop", list("split", "impurityStata")) %>%
    sdf_register() #%>% 
  #select(-split, -impurityStats) 
  
  joinedName = selected %>% 
    left_join(meta, by = "featureIndex") %>% sdf_register("ml_rf_tree")
  
  all_gframe = tbl(sc, sql("
  select 
  id,
  treeID,
  --label,
  impurity, 
  gain,
  leftChild,
  rightChild,
  CASE 
    WHEN (size(leftCategoriesOrThreshold) == 1)
      THEN concat('<= ', round(concat_ws('', leftCategoriesOrThreshold), 3))
    ELSE concat('in {', concat_ws('', leftCategoriesOrThreshold), '}')
  END AS leftCategoriesOrThreshold,
  coalesce(name, prediction) AS name
  from 
    ml_rf_tree
"))
  
  treeIDs = if (plot_treeIDs == 'all'){(all_gframe %>% select("treeID") %>% distinct() %>% collect %>% as.data.frame %>% arrange(., treeID))[,1] 
  }else{
    plot_treeIDs
  }
  
  all_plots = treeIDs %>% map(function(treeID_target){
    gframe = all_gframe %>% filter(treeID == treeID_target) %>% collect()
    
    vertices <- if (show_stats == TRUE){
      gframe %>% mutate(label = paste(name, 
                                      "",
                                      paste0("impurity: ", round(impurity, 3)), 
                                      paste0("gain: ", round(gain, 3)), 
                                      sep = "\n"), 
                        name = id)
    }else{
      gframe %>% mutate(label = name, 
                        name = id)
    }
    
    edges <- gframe %>%
      transmute(from = id, to = leftChild, label = leftCategoriesOrThreshold) %>% 
      union_all(gframe %>% select(from = id, to = rightChild)) %>% 
      filter(to != -1)
    
    g <- igraph::graph_from_data_frame(edges, vertices = vertices)
    
    plot(
      g, 
      layout = layout_as_tree(g, root = c(1)),
      #Controlling the size of the visualization
      rescale = FALSE,
      ylim=c(3,5), xlim = c(-15, 15),
      #Specifying the Size of the rectangles in the plot 
      vertex.shape = "rectangle",  
      vertex.size = 72, # width of rectangle
      vertex.size2=if (show_stats == TRUE){50}else{20}, # The second size of the node (e.g. for a rectangle) it is height
      vertex.color="lightblue",
      #Specifying position, size, font etc for labels
      vertex.label.font=1, # Font: 1 plain, 2 bold, 3, italic, 4 bold italic, 5 symbol
      vertex.label.cex=0.7, # Font size (multiplication factor, device-dependent)
      vertex.label.dist=0, # Distance between the label and the vertex
      
      vertex.label.color="black",
      edge.width=1, # Edge width, defaults to 1
      edge.arrow.size=0.5,
      #Controlling the Arrow labels
      edge.label.cex=0.7,
      edge.label.color="darkred"
    )
    title(paste("Tree ", treeID_target),cex.main=2,col.main="black")
    
  })
  
  hconf = sc %>% spark_context() %>% sparklyr::invoke("hadoopConfiguration")
  fs = invoke_static(sc, "org.apache.hadoop.fs.FileSystem", "get",  hconf)
  
  path = sparklyr::invoke_new(sc, "org.apache.hadoop.fs.Path", path_date)
  if((fs %>% sparklyr::invoke("exists", path)==TRUE)){
    fs %>% sparklyr::invoke("delete", path, TRUE)
  }else{
    print("no random forest model detected")
  }
  
  all_plots
}