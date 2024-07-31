################################################################################
# Readme -----------------------------------------------------------------------
######### Reportes de la versión 
# Versión: 2.0.0
# Se esta implementando la función para los datos que no están en intervalos de 5 minutos (Testear funcionalidad).
# Se plantea la opción de utilizar paralelización para hacer el procedimiento mas rápido (Testear funcionalidad).
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
################################################################################
# ----------------------- Funciones implementadas ------------------------------
# Función para los limites duros de la base de datos
limites.duros = function(df, variable) {
  
  # 1. Deteccion de umbrales
  pretrat = function(df){
    df = data.frame(df)
    df = df[, c("TIMESTAMP", variable)]
    df$TIMESTAMP = as.POSIXct(df$TIMESTAMP, format = "%Y-%m-%d %H:%M:%S", tz="UTC")
    df[[variable]] = as.numeric(df[[variable]])
    df = df[order(df$TIMESTAMP),]
  }
  
  df = pretrat(df)

  # Limite duros 
  Li = 0   # Limite inferior
  Ls = 20  # Limite superior
  
  # Verificación de limites duros
  ind.Li = which(df[[variable]] < Li)
  ind.Ls = which(df[[variable]] > Ls)
  
  # Limites inferiores 
  if (length(ind.Li) > 0) {
    dlg_message("Se encontraron valores por debajo del límite inferior, se procederá a eliminarloss.")
    df[ind.Li, variable] = NA
    # Lógica para tratar los datos
  } else {
    dlg_message("No se encontraron valores por debajo del límite inferior.")
  }
  
  # Limites superiores
  if (length(ind.Ls) > 0) {
    dlg_message("Se encontraron valores por encima del límite superior, se procederá a tratarlos.")
    Limite.superior = df[ind.Ls,]
    Limite.superior <<- Limite.superior
    
    # Lógica para tratar los datos
    # Se va a verificar 3 estaciones cercanas para corroborar información 
    dlg_message("A continuación seleccione las tres estaciones mas cercanas. ")
    dlg_message("Seleccione la primera estación cercana.")
    est.1 = dlg_open()$res
    est.load.1 = fread(est.1)
    est.load.1 = pretrat(est.load.1)
    dlg_message("Seleccione la segunda estación cercana.")
    est.2 = dlg_open()$res
    est.load.2 = fread(est.2)
    est.load.2 = pretrat(est.load.2)
    dlg_message("Seleccione la tercera estación cercana.")
    est.3 = dlg_open()$res
    est.load.3 = fread(est.3)
    est.load.3 = pretrat(est.load.3)
    
    est.near1 = est.load.1[est.load.1$TIMESTAMP %in% Limite.superior$TIMESTAMP, ]
    est.near2 = est.load.2[est.load.2$TIMESTAMP %in% Limite.superior$TIMESTAMP, ]
    est.near3 = est.load.3[est.load.3$TIMESTAMP %in% Limite.superior$TIMESTAMP, ]
    
    df.comparar = merge(Limite.superior, est.near1, by = "TIMESTAMP", all = TRUE)
    names(df.comparar) = c("TIMESTAMP", "Est.objetivo", "est.near1")
    df.comparar = merge(df.comparar, est.near2, by = "TIMESTAMP", all = TRUE)
    names(df.comparar) = c("TIMESTAMP", "Est.objetivo", "est.near1", "est.near2")
    df.comparar = merge(df.comparar, est.near3, by = "TIMESTAMP", all = TRUE)
    names(df.comparar) = c("TIMESTAMP", "Est.objetivo", "est.near1", "est.near2", "est.near3")
    
    # Método de Desviación Estándar sin ponderación ----------------------------
    Resultados = df.comparar
    for (i in 1:nrow(df.comparar)) {
      precipitations = as.matrix(df.comparar[i, c("Est.objetivo", "est.near1", "est.near2", "est.near3")])
      mean_values = mean(precipitations[,-1], na.rm = TRUE)
      std_dev_values = sd(precipitations[,-1], na.rm = TRUE)
      v_outlier = precipitations[,1]
      C_final = (v_outlier - mean_values) / std_dev_values
      valid = ifelse(abs(C_final) > 2, "Si", "No")
      Resultados[i, "outlier_std"] = valid
    }
    
    View(Resultados)
    
    # Elijo si desean eliminar los valores por encima del limite superior
    msn = dlg_message("¿Desea eliminar los valores por encima del límite superior?", type = c("yesno"))$res
    
    if (msn == "yes") {
      df[ind.Ls, variable] = NA
    } 

  } else {
    dlg_message("No se encontraron valores por encima del límite superior.")
  }
  return(df)
}

# Función para pre procesamiento de datos 
data_preprocess = function(df, variable){
  # Lectura y conversión de datos a formato correcto ---------------------------
  df = data.frame(df)
  df = df[, c("TIMESTAMP", variable)]
  df$TIMESTAMP = as.POSIXct(df$TIMESTAMP, format = "%Y-%m-%d %H:%M:%S", tz="UTC")
  df[[variable]] = as.numeric(df[[variable]])
  df = df[!is.na(df$TIMESTAMP),] # verificar esta linea en especifico. 
  df = df[order(df$TIMESTAMP),]
  
  # Identificación y eliminación de fechas repetidas ---------------------------
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

# Función para ver si hay valores seguidos continuos
posibles.fallas = function(df) {
# Secuencia estricta y Normal --------------------------------------------------
  tramos.repetidos = function(df, inicio, fin, j) {
    secuencias = data.frame(inicio = integer(), fin = integer(), valor = numeric())
    i = inicio
    while (i <= fin - j + 1) {
      secuencia = df$Lluvia_Tot[i:(i+j-1)]
      if (length(unique(secuencia)) == 1 && unique(secuencia) != 0 && !is.na(unique(secuencia))) {
        secuencias = rbind(secuencias, 
                           data.frame(inicio = i, 
                                      fin = i + j - 1, 
                                      valor = secuencia[1]))
        i = i + j  # Saltar a la siguiente secuencia potencial
      } else {
        i = i + 1
      }
    }
    return(secuencias)
  }
  
  tramos.repetidos <<- tramos.repetidos
  
  estrict.secuencia = function(df) {
    j = 288
    j <<- j
    # Configuro la paralelización
    num_cores = detectCores() - 1  # Usar todos los cores menos uno
    cl = makeCluster(num_cores)
    registerDoParallel(cl)
      
    clusterExport(cl, c("tramos.repetidos", "df", "j"))
    # Cargar dplyr en cada nodo
    clusterEvalQ(cl, library(dplyr))
    # Dividir el rango de índices entre los cores
    n = nrow(df)
    chunk_size = ceiling(n / num_cores)
    rangos = lapply(seq(1, n, by = chunk_size), function(x) c(x, min(x + chunk_size - 1, n)))
    
    # Aplicar la función en paralelo
    resultados = parLapply(cl, rangos, function(rango) {
      tramos.repetidos(df, rango[1], rango[2], j)
    })
    
    # Combinar los resultados
    secuencias_detectadas = do.call(rbind, resultados)
    
    # Crear el dataframe final con las fechas
    if (nrow(secuencias_detectadas) > 0) {
      resultado = secuencias_detectadas %>%
        mutate(fecha_inicio = df$TIMESTAMP[inicio],
               fecha_fin = df$TIMESTAMP[fin]) %>%
        select(fecha_inicio, fecha_fin, valor)
    } else {
      resultado = NULL
    }
    
    if (!is.null(resultado)) {
      return(resultado)
    }
    stopCluster(cl)
  }
  
  tramos.repetidosSeq = function(df, inicio, fin, j) {
    secuencias = data.frame(inicio = integer(), fin = integer(), valor = numeric())
    i = inicio
    while (i <= fin - j + 1) {
      secuencia = df$Lluvia_Tot[i:(i+j-1)]
      if (length(unique(secuencia)) == 1 && unique(secuencia) == 0 && !is.na(unique(secuencia))) {
        secuencias = rbind(secuencias, 
                           data.frame(inicio = i, 
                                      fin = i + j - 1, 
                                      valor = secuencia[1]))
        i = i + j  # Saltar a la siguiente secuencia potencial
      } else {
        i = i + 1
      }
    }
    return(secuencias)
  }
  
  tramos.repetidosSeq <<- tramos.repetidosSeq
  sequia.secuencia = function(df) {
    j = 288
    j <<- j
    # Configuro la paralelización
    num_cores = detectCores() - 1  # Usar todos los cores menos uno
    cl = makeCluster(num_cores)
    registerDoParallel(cl)
    
    clusterExport(cl, c("tramos.repetidosSeq", "df", "j"))
    # Cargar dplyr en cada nodo
    clusterEvalQ(cl, library(dplyr))
    # Dividir el rango de índices entre los cores
    n = nrow(df)
    chunk_size = ceiling(n / num_cores)
    rangos = lapply(seq(1, n, by = chunk_size), function(x) c(x, min(x + chunk_size - 1, n)))
    
    # Aplicar la función en paralelo
    resultados = parLapply(cl, rangos, function(rango) {
      tramos.repetidosSeq(df, rango[1], rango[2], j)
    })
    
    # Combinar los resultados
    secuencias_detectadas = do.call(rbind, resultados)
    
    # Crear el dataframe final con las fechas
    if (nrow(secuencias_detectadas) > 0) {
      resultado = secuencias_detectadas %>%
        mutate(fecha_inicio = df$TIMESTAMP[inicio],
               fecha_fin = df$TIMESTAMP[fin]) %>%
        select(fecha_inicio, fecha_fin, valor)
    } else {
      resultado = NULL
    }
    
    if (!is.null(resultado)) {
      return(resultado)
    }
    stopCluster(cl)
  }
  
  fallos.sensor = estrict.secuencia(df)
  posibles.Seq = sequia.secuencia(df)
  
  return(list(fallas_sensor = fallos.sensor, posibles_sequias = posibles.Seq))
  
  }
  

################################################################################
# ------------------------ Ejecución de la función -----------------------------
data = fread(file.path(directory, "ElLabradoM_min5.csv"))

df = limites.duros(data, "Lluvia_Tot") # Límites duros
df = data_preprocess(df, "Lluvia_Tot") # Pre procesamiento
fallas_sequias = posibles.fallas(df) # Posibles fallas




summary(df)
vacios = sum(is.na(df$Lluvia_Tot)) / nrow(df) * 100
vacios

directory.save = "C:/Users/Jonna/Desktop/Proyecto_U/Base de Datos/DATOS_ESTACIONES_FALTANTES_24JUL/Base datos procesada/Precipitación"
write.csv2(df, file.path(directory.save, "ElLabradoM_min5.csv"), row.names = FALSE)
data.c = fread(file.path(directory.save, "ElLabradoM_min5.csv"))
data.t = data.c
data.c$Lluvia_Tot = as.double(data.c$Lluvia_Tot)
# Comrobacion final
pretrat = function(df, variable){
  df = data.frame(df)
  df = df[, c("TIMESTAMP", variable)]
  df$TIMESTAMP = as.POSIXct(df$TIMESTAMP, format = "%Y-%m-%d %H:%M:%S", tz="UTC")
  df[[variable]] = as.numeric(df[[variable]])
  df = df[order(df$TIMESTAMP),]
}

data.2 = pretrat(data, "Lluvia_Tot")
summary(data.2)
