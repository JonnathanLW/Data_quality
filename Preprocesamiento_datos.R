################################################################################
# Readme -----------------------------------------------------------------------
######### Reportes de la versión 
# Versión: 3.0.0
# Se esta implementando la función para los datos que no están en intervalos de 5 minutos (Testear funcionalidad)
# Se plantea la opción de utilizar paralelización para hacer el procedimiento mas rápido (Testear funcionalidad).
# * Nota: Proceso de paralelizar resulta poco eficiente (Testear funcionalidad)
# Librerías necesarias ---------------------------------------------------------
library(data.table)
library(dplyr)
library(foreach)
library(doParallel)
library(svDialogs)
library(lubridate)
################################################################################
# Lectura de datos
directory = "C:/Users/Jonna/Desktop/Proyecto_U/Base de Datos/DATOS_ESTACIONES_FALTANTES_24JUL"
data = fread(file.path(directory, "ChanludM_min5.csv"))

# Función para pre procesamiento de datos 
data_preprocess = function(df, variable){
  # Lectura y conversión de datos a formato correcto ---------------------------
  df = data.frame(df)
  df = df[, c("TIMESTAMP", variable)]
  df$TIMESTAMP = as.POSIXct(df$TIMESTAMP, format = "%Y-%m-%d %H:%M:%S", tz="UTC")
  df[[variable]] = as.numeric(df[[variable]])
  df = df[order(df$TIMESTAMP),]
  
  # Identificación y eliminación de duplicados ---------------------------------
  ind.duplicated = which(duplicated(df$TIMESTAMP) | duplicated(df$TIMESTAMP, fromLast = TRUE))
  if (length(ind.duplicated) > 0) {
    dlg_message("Se encontraron duplicados en la base de datos, se procederá a eliminarlos. (Verifique los datos duplicados en la variable 'datos.duplicados')")
    datos.duplicados = df[ind.duplicated,]
    datos.duplicados <<- datos.duplicados
    df = df[!duplicated(df[c("TIMESTAMP", variable)]),]
  } else {
    dlg_message("No se encontraron duplicados en la base de datos.")
  }
  
  # Verifico que los datos se encuentren en intervalos de 5 minutos ------------
  intervalo_5min = function(timestamp) {
    minute(timestamp) %% 5 == 0 & second(timestamp) == 0
  }
  
  verificacion = intervalo_5min(df$TIMESTAMP)
  
  if (any(!verificacion == TRUE)) {
    dlg_message("Se encontraron datos que no cumplen con el intervalo de 5 minutos, se procederá a tratarlos.")
    
    # Algoritmo en fase Beta --------------------------------------------------
    # Configuro la paralelizacion
    # 1. Configurar el clúster para procesamiento paralelo
    cl = makeCluster(detectCores())
    num_cores = length(cl) - 1 # Se usa el numero total de cores menos 1
    registerDoParallel(num_cores)
    
    # 1.1 Optimizo el código: Selecciono años donde tengo problemas
    df.problemas = df %>%
      filter(minute(TIMESTAMP) %% 5 != 0 | second(TIMESTAMP) != 0)
    
    # 2. Algoritmo a paralelizar
    rango.fechas = seq(floor_date(min(df.problemas$TIMESTAMP), "5 mins"), 
                       ceiling_date(max(df.problemas$TIMESTAMP), "5 mins"), 
                       by = "5 mins")
    
    fechas.ideales = data.frame(TIMESTAMP = rango.fechas)
    
    # Función para encontrar el valor más cercano dentro de 1 minuto
    valor.cercano = function(hora.objetivo, df) {
      ventana.inicio = hora.objetivo - minutes(1)
      ventana.fin = hora.objetivo
      
      dato.cercano = df %>% 
        filter(TIMESTAMP > ventana.inicio, TIMESTAMP <= ventana.fin) %>% 
        arrange(desc(TIMESTAMP)) %>%
        slice(1)
      
      if (nrow(dato.cercano) == 0) {
        return(NA)
      } else {
        return(dato.cercano[[variable]])
      }
    }
    
    # 3. Divido los datos en chunks para procesamiento en paralelo
    chunks = split(fechas.ideales, cut(seq(nrow(fechas.ideales)), num_cores, labels = FALSE))
    
    # 4. Procesamiento paralelo
    result_list = foreach(chunk = chunks, .packages = c("dplyr", "lubridate")) %dopar% {
      chunk %>%
        rowwise() %>%
        mutate(!!variable := valor.cercano(TIMESTAMP, df.problemas)) %>%
        filter(!is.na(!!sym(variable)))  
    }
    
    # 6. Combino los resultados
    df.final = bind_rows(result_list)
    
    df_sin_problemas = df %>% filter(minute(TIMESTAMP) %% 5 == 0 & second(TIMESTAMP) == 0)
    df.final = bind_rows(df.final, df_sin_problemas)
    df.final = df.final[!duplicated(df.final[c("TIMESTAMP", variable)]),]
    df.final = df.final[order(df.final$TIMESTAMP),]
    
    ind.duplicated_prob = which(duplicated(df.final$TIMESTAMP) | duplicated(df.final$TIMESTAMP, fromLast = TRUE))
    duplicados.error = df.final[ind.duplicated_prob,]
    
    # Versión beta para conservar valores en ventanas de 20 min en pasado y + 15 en el futuro.
    resolver_duplicados =function(df.final, indice, ventana_antes = 20, ventana_despues = 15) {
      timestamp_actual =df.final$TIMESTAMP[indice]
      ventana_inicio =timestamp_actual - minutes(ventana_antes)
      ventana_fin =timestamp_actual + minutes(ventana_despues)
      
      valores_cercanos =df.final[[variable]][df.final$TIMESTAMP >= ventana_inicio & 
                                               df.final$TIMESTAMP <= ventana_fin & 
                                               df.final$TIMESTAMP != timestamp_actual]
      
      filas_actuales =which(df.final$TIMESTAMP == timestamp_actual)
      valores_actuales =df.final[[variable]][filas_actuales]
      
      # Encontrar el valor más frecuente en los valores cercanos y actuales
      todos_valores =c(valores_cercanos, valores_actuales)
      valor_a_mantener =as.numeric(names(which.max(table(todos_valores))))
      
      # Identificar las filas que no coinciden con el valor a mantener
      filas_a_na =filas_actuales[df.final[[variable]][filas_actuales] != valor_a_mantener]
      
      return(list(valor = valor_a_mantener, filas_na = filas_a_na))
    }
    
    # Crear una copia del dataframe original
    df_limpio = df.final
    
    resultados = foreach(i = ind.duplicated_prob, .packages = c("lubridate")) %dopar% {
      resolver_duplicados(df_limpio, i)
    }
    
    for (i in seq_along(ind.duplicated_prob)) {
      indice = ind.duplicated_prob[i]
      resultado = resultados[[i]]
      timestamp_actual = df_limpio$TIMESTAMP[indice]
      
      if (!is.na(timestamp_actual)) {
        df_limpio[[variable]][df_limpio$TIMESTAMP == timestamp_actual] = resultado$valor
      }
      
      # Establecer NA para las filas que deben ser reemplazadas
      if (length(resultado$filas_na) > 0) {
        df_limpio$TIMESTAMP[resultado$filas_na] =NA
        df_limpio[[variable]][resultado$filas_na] =NA
      }
    }
    
    # Elimino las filas con NA de TIMESTAMP
    df_limpio =df_limpio[!is.na(df_limpio$TIMESTAMP),]
    
    # Se une directamente los datos
    min.date = min(df$TIMESTAMP)
    max.date = max(df$TIMESTAMP)
    TIMESTAMP = seq.POSIXt(min.date, max.date, by = "5 min")
    df.final = merge(data.frame(TIMESTAMP = TIMESTAMP), df_limpio, by = "TIMESTAMP", all = TRUE)
    
    if (length(df.final$TIMESTAMP) != length(TIMESTAMP)) {
      dlg_message("Verificar la secuencia de datos, no coincide")
      stop("Error en la secuencia de datos")
    } 
    # 5. Detener el clúster
    stopCluster(cl)
    
    # -------------------------------------------------------------------------
    
  } else {
    dlg_message("Todos los datos cumplen con el intervalo de 5 minutos.")
    # Se une directamente los datos
    min.date = min(df$TIMESTAMP)
    max.date = max(df$TIMESTAMP)
    TIMESTAMP = seq.POSIXt(min.date, max.date, by = "5 min")
    df.final = merge(data.frame(TIMESTAMP = TIMESTAMP), df, by = "TIMESTAMP", all = TRUE)
    
    if (length(df.final$TIMESTAMP) != length(TIMESTAMP)) {
      dlg_message("Verificar la secuencia de datos, no coincide")
      stop("Error en la secuencia de datos")
    } else {
      dlg_message("La secuencia esta correcta")
    }
    # -------------------------------------------------------------------------
  }
  return(df.final)
}

# Ejecución de la función ------------------------------------------------------
df = data_preprocess(data, "Lluvia_Tot")

