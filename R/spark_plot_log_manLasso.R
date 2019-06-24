#' A SparklyR Logistic Regression Feature Selection Method \cr
#' @description 
#' The function compares the AUC between logistic regression models for each feature, dropping each iteratively. 
#' @details
#' Important package requirements: \cr
#' You must have ggplot2 installed \cr
#' \cr
#' Example selection of a spark table and graph\cr
#' \code{spark_table = tbl(sc, sql("select * from sample_data.iris limit 100"))} \cr
#' \code{outputs = spark_plot_log_manLasso(spark_table, predictor='Species')}
#' @param sparklyr_table  is the spark table you will pass to the function. You can pass using a dplyr spark table (tbl).
#' @param predictor  is the target column to predict
#' @param num_folds (default=3) this param passes the number of cross-validation folds to use for each logistic regression model
#' @param parallelism (default=1) this param allows us to deploy default models simultaneously
#' @export 
spark_plot_log_manLasso = function(sparklyr_table, predictor, num_folds=3, parallelism=1){
  library(purrr)
  library(ggplot2)
  inputCols = sparklyr_table %>% select(-c(paste0(predictor))) %>% colnames()
  # Calculating full variable set baseline AUC
  allColumn_AUC = {
    df_va = ft_vector_assembler(sparklyr_table, 
                                input_cols = inputCols, 
                                output_col = 'features')
    estimator = ml_logistic_regression(sc, 
                                       label_col = paste0(predictor), 
                                       features_col = 'features')
    param_grid = list(logistic_regression = list(elastic_net_param = 0))
    evaluator = ml_binary_classification_evaluator(sc, 
                                                   label_col = paste0(predictor)) #predictor)
    # Using the new API ml cross validator allows us to use multithreading on the driver to launch models in parallel
    ret_cv = ml_cross_validator(
      df_va,
      estimator = estimator,
      estimator_param_maps = param_grid,
      evaluator = evaluator,
      seed = 2018,
      num_folds = num_folds,
      parallelism = parallelism
    )
    ret_cv$avg_metrics_df$areaUnderROC
  }
  #Calculating the AUC for each model after removing a variable
  columnAUC_tbl = {
    columnAUC_results = inputCols %>% map(function(dropCol){
      
      #return the column names without the column to drop
      targetCols = inputCols[!inputCols %in% dropCol]
      #runModel
      df_va = ft_vector_assembler(df_titanic, 
                                  input_cols = targetCols, 
                                  output_col = 'features')
      estimator = ml_logistic_regression(sc, 
                                         label_col = 'survived', #predictor, 
                                         features_col = 'features')
      param_grid = list(logistic_regression = list(elastic_net_param = 0))
      evaluator = ml_binary_classification_evaluator(sc, 
                                                     label_col = 'survived') #predictor)
      # Using the new API ml cross validator allows us to use multithreading on the driver to launch models in parallel
      ret_cv = ml_cross_validator(
        df_va,
        estimator = estimator,
        estimator_param_maps = param_grid,
        evaluator = evaluator,
        seed = 2018,
        num_folds = num_folds,
        parallelism = parallelism
      )
      list(excluded_feature = dropCol, areaUnderROC = ret_cv$avg_metrics_df$areaUnderROC)
    })
    #converting the list results to a df 
    do.call(rbind.data.frame, columnAUC_results)
  }
  
  
  ggplot(columnAUC_tbl,
         aes(excluded_feature, areaUnderROC, fill = avg_metric)) +
    coord_cartesian(ylim = c(0.5, 1)) +
    geom_hline(aes(yintercept = allColumn_AUC), linetype = 'dashed') +
    annotate(
      'text',
      x = 'sex',
      y = 0.87,
      label = 'Full model',
      family = 'mono',
      alpha = .75,
      size = 3.5
    ) +
    geom_point(shape = 21, size = 2.5, show.legend = FALSE) +
    scale_fill_viridis_c(option = 'B', end = 0.9) +
    labs(
      x = 'Excluded feature',
      y = 'Average Area \nunder the ROC curve',
      title = 'Feature selection by cross-validation',
      subtitle = 'Average drop in performance from removing each feature'
    ) +
    theme_minimal(base_size = 12, base_family = 'mono') +
    theme(panel.grid.major.x = element_blank())
}