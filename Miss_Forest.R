################################################################################
# Readme:
# Versión: 1.0.0 (Beta)
# Librerías necesarias ---------------------------------------------------------
library(pacman)
p_load(data.table, dplyr, foreach, doParallel, svDialogs, lubridate, ggplot2, 
       missForest, purrr, hydroGOF, tidyr, caret, randomForest)

################################################################################
# Funciones complementarias 

# Crear una lista con los directorios de los archivos a subir
data_folder = "C:/Users/Jonna/Desktop/Proyecto_U/Random_Forest/Datos_prec"
file_list = list.files(path = data_folder, pattern = "*.csv", full.names = TRUE)
station_names = gsub("\\.csv$", "", basename(file_list))
station_names = paste0(station_names, "_HH")

data_base = list()
for (i in seq_along(file_list)) {
  est = fread(file_list[i])
  names(est) = c("Date_Hour", "acumulado")
  est$Year = as.numeric(format(est$Date_Hour, "%Y"))
  est$Month = as.numeric(format(est$Date_Hour, "%m"))
  est$Day = as.numeric(format(est$Date_Hour, "%d"))
  est$Hour = as.numeric(format(est$Date_Hour, "%H"))
  est <- transform(est, Date_Hour = make_datetime(Year, Month, Day, Hour))
  data_base[[paste0("est_", i)]] = est
}

# Calcular los valores faltantes
na_results = list()
for (i in 1:length(data_base)) {
  data <- data_base[[i]]
  total_values <- nrow(data)
  missing_values <- sum(is.na(data$acumulado))
  name_estat = file_list[i]
  name_estat = gsub("\\.csv$", "", basename(name_estat))
  nas = (missing_values / total_values) * 100
  # guardo el nombre y vacíos en mi lista 
  na_results[[i]] <- list(name_estat = name_estat,
                          missing_values = nas)
}

# Reporte de Vacíos
na_results = do.call(rbind, lapply(na_results, as.data.frame))
na_results

# Crear data frame combinado
for (i in seq_along(data_base)) {
  data_base[[i]] <- data_base[[i]] %>%
    mutate(Station = station_names[i]) %>%
    dplyr::rename(Date = Date_Hour) %>%
    select(Date, Station, Value = acumulado)
}

combined_data = bind_rows(data_base)

final_combined_data = combined_data %>%
  tidyr::spread(key = Station, value = Value) %>%
  arrange(Date)

# Version prueba
final_combined_data$Year = year(final_combined_data$Date)
final_combined_data$Month = month(final_combined_data$Date)
final_combined_data$Day = day(final_combined_data$Date)
final_combined_data$Hour = hour(final_combined_data$Date)
# Excluir variables de tiempo del proceso de imputación
data_to_impute = final_combined_data %>%
  select(-Date)

# Remover filas con NA para la validaci?n
data_to_impute_kfcv <- na.omit(data_to_impute)

# Análisis de correlación
#correlation_matrix = cor(data_to_impute_kfcv, use = "pairwise.complete.obs")

# Crear variable para almacenar resultados
results_missForest_KFCV <- matrix(NA, nrow = 27, ncol = 5)

# Imputación de datos faltantes con missForest --------------------------------
num_cores = length(data_to_impute_kfcv)
cl = makeCluster(num_cores)
registerDoParallel(cl)

# Validación cruzada de k-fold
set.seed(1)
k.folds = 5
seeds_list = c(rep(1:k.folds))
# train.index = sample(seq_len(nrow(data_to_impute_kfcv)), size = 0.8 * nrow(data_to_impute_kfcv), replace = FALSE)
# data.train = data_to_impute_kfcv[train.index, ]
# data.test = data_to_impute_kfcv[-train.index, ]
# obtengo los indices de data.test

# Creo los pliegues
results.gof = vector("list", k.folds)
results_OBBerror = vector("list", k.folds)
process_fold = function(i) {
  set.seed(seeds_list[i])
  
  columns_to_modify = names(data_to_impute_kfcv)[1:(ncol(data_to_impute_kfcv) - 4)] # modificar el 4 por las columnas que no se desea poner vacios
  
  data_with_na = data_to_impute_kfcv
  for (col in columns_to_modify) {
    na_indices = sample(nrow(data_to_impute_kfcv), size = floor(0.2 * nrow(data_to_impute_kfcv)))
    data_with_na[na_indices, col] = NA
  }
  
  data_to_impute_kfcv_mis = data_with_na
  index_na_kfcv = is.na(data_to_impute_kfcv_mis)
  
  # Aplicar missForest para rellenar los datos faltantes
  missForest_result = missForest(data_to_impute_kfcv_mis,
                                  maxiter = 8, 
                                  ntree = 500, # 500
                                  mtry = 4, # 2
                                  #maxnodes = 10,
                                  variablewise = TRUE,
                                  replace = TRUE,
                                  decreasing = TRUE,
                                  parallelize = "variables")  # Cambiado a "no"
  
  data_filled = missForest_result$ximp  
  # Extraer los valores originales y los predichos
  original_values = as.matrix(data_to_impute_kfcv)[index_na_kfcv]
  predicted_values = as.matrix(data_filled)[index_na_kfcv]
  
  # Validar el proceso de relleno (OOBerror real)
  kfcv_gof = gof(predicted_values, original_values)
  
  # Validar el proceso de relleno (error estimado de imputación (OOberror))
  OOBerror = missForest_result$OOBerror
  
  # Tabla comparativa 
  residuals_kfcv = original_values - predicted_values
  residuals_df = data.frame(Index = seq_along(residuals_kfcv), Residuals = residuals_kfcv)
 
  # Generar el gráfico de los residuales usando ggplot2 ------------------------
  p = ggplot(residuals_df, aes(x = Index, y = Residuals)) +
    geom_point(color = "blue") +
    geom_hline(yintercept = mean(residuals_kfcv), color = "red") +
    labs(x = 'Index', y = 'Residuals', title = 'Residuals Plot')
  
  # Mostrar el gráfico
  ggsave(file = file.path("C:/Users/Jonna/Desktop/Proyecto_U/Random_Forest/Graficos_missForest/", paste0("RESIDUAL_GRAPH_FOLD_", i, ".png")),
         plot = p, width = 12, height = 8, dpi = 150)

  return(list(kfcv_gof = kfcv_gof, OOBerror = OOBerror))
}

for (i in 1:k.folds) {
  fold_results = process_fold(i)
  results.gof[[i]] = fold_results$kfcv_gof
  results_OBBerror[[i]] = fold_results$OOBerror
}
stopCluster(cl)
# ------------------------------------------------------------------------------
gof_Kfolds = data.frame(do.call(cbind, results.gof))
OBBerror_Kfolds = data.frame(do.call(cbind, results_OBBerror))

row.names(results_missForest_KFCV) = row.names(kfcv_gof)
colnames(results_missForest_KFCV) = c("FOLD_1", "FOLD_2", "FOLD_3", "FOLD_4", "FOLD_5")
results_missForest_KFCV

# SE REALIZA MISSFOREST A LA BASE DE DATOS GENERAL
missForest_final_result <- missForest(data_to_impute, maxiter = 10, ntree = 100)
final_combined_data_filled <- missForest_final_result$ximp
final_combined_data_filled_df <- data.frame(Date = final_combined_data$Date, final_combined_data_filled)
#write.csv(final_combined_data_filled_df, "./P3_Y_P4/R_SCRIPTS/DATOS_ACTUALES/data_base_filled_HH.csv", row.names = FALSE)
