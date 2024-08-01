################################################################################
# Readme:
# Versión: 2.13.4 (Estable)
# Librerías necesarias ---------------------------------------------------------
library(pacman)
p_load(data.table, dplyr, foreach, doParallel, svDialogs, lubridate, ggplot2, gridExtra)

################################################################################
# directorio raíz. 
directory = "C:/Users/Jonna/Desktop/Proyecto_U/Base de Datos/DATOS_ESTACIONES_FALTANTES_24JUL"
################################################################################
# ----------------------- Funciones implementadas ------------------------------
# Funciones complementarias ----------------------------------------------------
leer.datos = function() {
  estacion = paste0(nombre.estat, ".csv")
  df = fread(file.path(directory, estacion))
  df = data.frame(df)
  df = df[, c("TIMESTAMP", variable)]
  
  if (!is.numeric(df[1,2])){
    df = df[-1,]
  }
  df$TIMESTAMP = as.POSIXct(df$TIMESTAMP, format = "%Y-%m-%d %H:%M:%S", tz="UTC")
  df[[variable]] = as.numeric(df[[variable]])
  df = df[!is.na(df$TIMESTAMP),] # verificar esta linea en especifico. 
  df = df[order(df$TIMESTAMP),]
  return(df)
}
graficar = function(df, variable, nombre.estat, carpeta_1 = NULL, carpeta_2) {
  num_cores = detectCores() - 1  # Usa todos los núcleos menos uno
  cl = makeCluster(num_cores)
  registerDoParallel(cl)
  # Configuración de directorios
  if (is.null(carpeta_1)) {
    ruta = paste0(directory, "/", carpeta_2)
  } else {
    ruta = paste0(directory, "/", carpeta_1, "/", carpeta_2)
    if (!dir.exists(ruta)) {
      dir.create(ruta, recursive = TRUE)
    }
  }
  
  if (variable == "Lluvia_Tot") {
    titulo = "Precipitación en el año"
    eje_y = "Precipitación (mm)"
  } else {
    titulo = "Niveles en el año"
    eje_y = "Nivel (cm)"
  }
  
  df_graf = df
  años = unique(year(df_graf$TIMESTAMP))
  
  crear_grafico_anual = function(df_año, año, variable, titulo, eje_y) {
    # Calcular el inicio y fin de cada mes
    last_day_of_month = function(date) {
      ceiling_date(date, "month") - days(1)
    }
    
    month_breaks = c(
      as.Date(paste0(año, "-01-01")),  # Primer día de enero
      sapply(2:12, function(m) last_day_of_month(as.Date(paste0(año, "-", m, "-01"))))
    )
    
    # Crear gráfico
    p.1 = ggplot(df_año, aes(x = TIMESTAMP, y = !!sym(variable))) +
      geom_line(color = "blue") +
      labs(title = paste(titulo, año), x = "Fecha", y = eje_y) +
      scale_x_datetime(
        limits = c(as.POSIXct(paste0(año, "-01-01 00:00:00")), 
                   as.POSIXct(paste0(año, "-12-31 23:59:59"))),
        breaks = as.POSIXct(month_breaks),
        labels = function(x) format(x, "%b"),
        date_minor_breaks = "1 month"
      ) +
      theme(
        plot.title = element_text(hjust = 0.5, face = "bold"),
        axis.text.x = element_text(angle = 45, hjust = 1)
      )
    
    return(p.1)
  }
  
  # Paralelizar la creación de gráficos
  resultados = foreach(año = años, .packages = c('ggplot2', 'dplyr', 'lubridate', 'gridExtra')) %dopar% {
    df_año = df_graf %>% filter(year(TIMESTAMP) == año)
    crear_grafico_anual(df_año, año, variable, titulo, eje_y)
  }
  
  # Combinar y guardar gráficos
  p_f = do.call(grid.arrange, c(grobs = resultados, ncol = 3))
  ggsave(paste0(ruta, "/", nombre.estat, "_crudo.png"),
         plot = p_f, width = 12, height = 8, units = "in", dpi = 300, type = "cairo")
  
  stopCluster(cl)
}
save.data = function(df, nombre.estat, carpeta_1 = NULL, carpeta_2){
  if (is.null(carpeta_1)) {
    ruta = paste0(directory, "/", carpeta_2)
  } else {
    ruta = paste0(directory, "/", carpeta_1, "/", carpeta_2)
    if (!dir.exists(ruta)) {
      dir.create(ruta, recursive = TRUE)
    }
  }
  write.csv(df, paste0(ruta, "/", nombre.estat, ".csv"), row.names = FALSE)
}
# ------------------------------------------------------------------------------
# Funciones principales --------------------------------------------------------
# Función para pre procesamiento de datos 
data_preprocess = function(df){
  # Identificación y eliminación de fechas repetidas ---------------------------
  ind.duplicated = which(duplicated(df$TIMESTAMP) | duplicated(df$TIMESTAMP, fromLast = TRUE))
  if (length(ind.duplicated) > 0) {
    dlg_message("Se encontraron duplicados en la base de datos, se procederá a eliminarlos. (Verifique los datos duplicados en la variable 'datos.duplicados')")
    datos.duplicados = df[ind.duplicated,]
    datos.duplicados = datos.duplicados[order(datos.duplicados$TIMESTAMP),]
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
    
    # Algoritmo en paralelo
    # Configuro la paralelizacion
    # 1. Configurar el clúster para procesamiento paralelo
    cl = makeCluster(detectCores())
    num_cores = length(cl) - 1 # Se usa el numero total de cores menos 1
    registerDoParallel(num_cores)
    
    # 1.1 Optimizo el código: Selecciono años donde tengo problemas
    df.problemas = df %>%
      filter(minute(TIMESTAMP) %% 5 != 0 | second(TIMESTAMP) != 0)
    
    datos.erroneos <<-- df.problemas
    
    # 2. Algoritmo a paralelizar
    rango.fechas = seq(floor_date(min(df.problemas$TIMESTAMP), "5 mins"), 
                       ceiling_date(max(df.problemas$TIMESTAMP), "5 mins"), 
                       by = "5 mins")
    
    fechas.ideales = data.frame(TIMESTAMP = rango.fechas)
    
    # Función para encontrar el valor más cercano dentro de 1 minuto
    valor.cercano = function(hora.objetivo, df) {
      ventana.inicio = hora.objetivo - minutes(1)
      ventana.fin = hora.objetivo
      
      if (!any(df$TIMESTAMP == ventana.fin)) {
        dato.cercano = df %>% 
          filter(TIMESTAMP > ventana.inicio, TIMESTAMP <= ventana.fin) %>% 
          arrange(desc(TIMESTAMP)) %>%
          slice(1)
        
        if (nrow(dato.cercano) == 0) {
          return(NA)
        } else {
          return(dato.cercano[[variable]])
        }
      } else {
        return(df[df$TIMESTAMP == ventana.fin, variable])
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
    resolver_duplicados = function(df.final, indice, ventana_antes = 20, ventana_despues = 15) {
      timestamp_actual = df.final$TIMESTAMP[indice]
      ventana_inicio = timestamp_actual - minutes(ventana_antes)
      ventana_fin = timestamp_actual + minutes(ventana_despues)
      
      
      valores_cercanos = df.final[[variable]][df.final$TIMESTAMP >= ventana_inicio & 
                                               df.final$TIMESTAMP <= ventana_fin & 
                                               df.final$TIMESTAMP != timestamp_actual]
      
      filas_actuales = which(df.final$TIMESTAMP == timestamp_actual)
      valores_actuales =df.final[[variable]][filas_actuales]
      
      # Encontrar el valor más frecuente en los valores cercanos y actuales
      todos_valores =c(valores_cercanos, valores_actuales)
      valor_a_mantener =as.numeric(names(which.max(table(todos_valores))))
      
      # Identificar las filas que no coinciden con el valor a mantener
      filas_a_na = filas_actuales[df.final[[variable]][filas_actuales] != valor_a_mantener]
      
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
        df_limpio$TIMESTAMP[resultado$filas_na] = NA
        df_limpio[[variable]][resultado$filas_na] = NA
      }
    }
    
    # Elimino las filas con NA de TIMESTAMP
    df_limpio = df_limpio[!is.na(df_limpio$TIMESTAMP),]
    
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

# Función para los limites duros de la base de datos
limites.duros = function(df) {
  
  # Creo carpeta para guardar los gráficos -------------------------------------
  carpeta_1 = "Graph_LimitesDuros"
  carpeta_2 = "Estaciones_crudas"
  graficos.iniciales = graficar(df, variable, nombre.estat, carpeta_1, carpeta_2)
# ------------------------------------------------------------------------------
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
    
    pretrat = function(df) {
      df = data.frame(df)
      df = df[, c("TIMESTAMP", variable)]
      
      if (!is.numeric(df[1,2])){
        df = df[-1,]
      }
      
      df$TIMESTAMP = as.POSIXct(df$TIMESTAMP, format = "%Y-%m-%d %H:%M:%S", tz="UTC")
      df[[variable]] = as.numeric(df[[variable]])
      df = df[!is.na(df$TIMESTAMP),] # verificar esta linea en especifico. 
      df = df[order(df$TIMESTAMP),]
    }
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
  
  # Gráficos en caso de que se hayan eliminado limites 
  if (length(ind.Li) > 0 | length(ind.Ls) > 0) {
    carpeta_1 = "Graph_LimitesDuros"
    carpeta_2 = "Estaciones_Limites_process"
    graficos.finales = graficar(df, variable, nombre.estat, carpeta_1, carpeta_2)
  } else {
    dlg_message("No se encontraron limites dinferiores y superiores (Los gráficos no cambian).")
  }
  return(df)
}

# Función para ver si hay valores seguidos continuos
posibles.fallas = function(df) {
  # Secuencia estricta y Normal ------------------------------------------------
  tramos.repetidos = function(df, inicio, fin, j, intervalo) {
    secuencias = data.frame(inicio = integer(), fin = integer(), valor = numeric())
    i = inicio
    while (i <= fin - j + 1) {
      secuencia = df$Lluvia_Tot[i:(i + j - 1)]
      
      if (length(unique(secuencia)) == 1 && unique(secuencia) != 0 && !is.na(unique(secuencia))) {
        valor_repetido = unique(secuencia)
        k = j
        
        while (i + k <= fin && df$Lluvia_Tot[i + k] == valor_repetido) {
          k = k + 1
        }
        
        secuencias = rbind(secuencias, 
                            data.frame(inicio = i, 
                                       fin = i + k - 1, 
                                       valor = valor_repetido))
        
        # Buscar apariciones adicionales en el intervalo de tiempo posterior
        tiempo_final = as.POSIXct(df$TIMESTAMP[i + k - 1]) + intervalo
        
        while (i + k <= fin && as.POSIXct(df$TIMESTAMP[i + k]) <= tiempo_final) {
          if (df$Lluvia_Tot[i + k] == valor_repetido) {
            k = k + 1
            secuencias$fin[nrow(secuencias)] = i + k - 1
            tiempo_final = as.POSIXct(df$TIMESTAMP[i + k - 1]) + intervalo
          } else {
            k = k + 1
          }
        }
        
        i = i + k  # Saltar a la siguiente secuencia potencial
      } else {
        i = i + 1
      }
    }
    
    return(secuencias)
  }
  
  tramos.repetidos <<- tramos.repetidos
  
  estrict.secuencia = function(df) {
    j = 288  # 24 horas
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
      tramos.repetidos(df, rango[1], rango[2], j, 20 * 60) #establezo ventana de 20 minutos adicionales. 
    })
    
    stopCluster(cl)
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
  }
  
  fallos.sensor = estrict.secuencia(df)
  
  # Lógica para visualizar valores antes de eliminarlos 
  if (!is.null(fallos.sensor)){
    View(fallos.sensor)
    msj_temp = dlg_message("Se encontraron tramos temporales repetidos (Posibles fallas del sensor) Desea remplazar por NA dichos valores?", type = c("yesno"))$res
    if (msj_temp == "yes") {
      for (i in 1:nrow(fallos.sensor)) {
        fecha_inicio <- fallos.sensor$fecha_inicio[i]
        fecha_fin <- fallos.sensor$fecha_fin[i]
        df$Lluvia_Tot[df$TIMESTAMP >= fecha_inicio & df$TIMESTAMP<= fecha_fin] <- NA
      }
      carpeta_1 = "Fallas_sensor"
      carpeta_2 = "Estaciones_sin_fallas"
      graficos = graficar(df, variable, nombre.estat, carpeta_1, carpeta_2)
    } 
  } else {
    dlg_message("No se encontraron posibles fallas en el sensor")
  }
  return(list(fallas_sensor = fallos.sensor, df = df))
}

# agrupamiento horario
agrupamiento.horario = function(df){
  
  num_cores = detectCores() - 1  # Usa todos los núcleos menos uno
  cl = makeCluster(num_cores)
  registerDoParallel(cl)
  
  umbral.min = 10 # % de vacíos continuos  
  umbral.max = 15 # % de vacíos en no continuos
  
  fecha.minima = min(df$TIMESTAMP)
  fecha.maxima = max(df$TIMESTAMP)
  indices = which(is.na(df$Lluvia_Tot))
  fechas = df$TIMESTAMP[indices]
  fechas = data.frame(fechas)
  
  fechas.1 = as.Date(fechas$fechas, format = "%Y-%m-%d")
  fechas.1 = unique(fechas.1)
  fechas.1 = data.frame(fechas.1)
  
  # Paralelizar la tarea usando foreach
  resultados = foreach(i = 1:nrow(fechas.1), .combine = 'c', .packages = c('dplyr', 'lubridate')) %dopar% {
    Li = as.POSIXct(paste0(fechas.1[i, ], " 00:00:00"), tz = "UTC")
    Ls = as.POSIXct(paste0(fechas.1[i, ], " 23:55:00"), tz = "UTC")
    horas = seq(Li, Ls, by = "hour")
    num_fechas.horaria = sapply(horas, function(hora) {
      fechas.filtradas = fechas %>%
        filter(fechas >= hora & fechas < hora + hours(1))
      return(nrow(fechas.filtradas))
    })
    return(num_fechas.horaria)
  }
  
  horas.seq = foreach(i = 1:nrow(fechas.1), .combine = rbind, .packages = c('lubridate')) %dopar% {
    Li = as.POSIXct(paste0(fechas.1[i, ], " 00:00:00"), tz = "UTC")
    Ls = as.POSIXct(paste0(fechas.1[i, ], " 23:55:00"), tz = "UTC")
    horas = seq(Li, Ls, by = "hour")
    return(data.frame(fecha = horas))
  }
  
 # horas.seq = do.call(rbind, lapply(horas.seq , function(x) data.frame(fecha = x)))
  Nas = data.frame(fecha = horas.seq$fecha, num.fechas = unlist(resultados))
  names(Nas) = c("fecha", "Na")
  
  # 
  fechas.comparativa = seq(as.POSIXct(fecha.minima), as.POSIXct(fecha.maxima), by = "hour")
  fechas.comparativa = trunc(fechas.comparativa, "hour")
  fechas.comparativa = data.frame(fechas.comparativa)
  names(fechas.comparativa) = c("fecha")
  
  # 
  conteo = merge(fechas.comparativa, Nas, by = "fecha", all = TRUE)
  conteo = data.frame(conteo)
  conteo$Na[is.na(conteo$Na)] = 0
  conteo$porcentaje = round((conteo$Na / 12) * 100,2)
  names(conteo) = c("fecha", "Na", "% Datos_faltantes")
  
  mayor.umbral = conteo %>% filter(conteo$`% Datos_faltantes` > umbral.min)
  
  faltantes = mayor.umbral
  names(faltantes)[1] = "TIMESTAMP"
  datos.descartados <<- faltantes
  
  df.1 = df %>% mutate(TIMESTAMP = as.POSIXct(TIMESTAMP)) %>%
    mutate(TIMESTAMP = floor_date(TIMESTAMP, "hour")) %>%
    anti_join(faltantes, by = "TIMESTAMP") %>%
    group_by(TIMESTAMP) %>%
    summarise(!!variable := sum(!!sym(variable), na.rm = TRUE))
  
  fechas.completas = fechas.comparativa
  names(fechas.completas) = c("TIMESTAMP")
  df = merge(fechas.completas, df.1, by = "TIMESTAMP", all = TRUE)
  df = df[order(df$TIMESTAMP),]
  
  stopCluster(cl)
  
  # En Desarrollo --------------------------------------------------------------
  # Descripción: Incorporar gráfico para ver la distribución de los datos que no fueron agrupados
  # de manera horaria, verificar como están distribuios y considerar agruparlos si el umbral 
  # es mayor al 15 %
  indices.vac = which(is.na(df$Luvia_Tot))
  fechas.vac = df$TIMESTAMP[indices.vac]
  fechas.vac = data.frame(fechas.vac)
  names(fechas.vac) = c("TIMESTAMP")
  
  data.rev = merge(fechas.vac, faltantes, by = "TIMESTAMP")
  
  indices.mayor15 = which(data.rev$`% Datos_faltantes` <= umbral.max)
  if (any(indices.mayor15)) {
    dlg_message("Se encontraron fechas con un porcentaje de datos faltantes menor al umbral máximo (15), revise los datos faltantes")
    stop("Se encontraron fechas con un porcentaje de datos faltantes menor al umbral máximo (15), revise los datos faltantes")
  } else {
    dlg_message("No se encontraron fechas con un porcentaje de datos faltantes menor al umbral máximo (15)")
  }
  
  # Reporte -----------------------------------------------------------------
  faltantes = sum(is.na(df$Lluvia_Tot))
  total = nrow(df)
  
  reporte.horario = data.frame(
    Estacion = nombre.estat,
    periodo_estudio = paste(fecha.minima, "al", fecha.maxima),
    Numero_años = as.numeric(year(fecha.maxima) - year(fecha.minima)),
    Datos_registrados = total,
    Datos_ausentes = faltantes,
    porcentaje_completos = round(100 - ((faltantes / total) * 100),2),
    porcentaje_ausentes = round((faltantes / total) * 100,2)
  )  
  
  reporte.horario <<- reporte.horario
  dlg_message("Se ha generado un reporte de datos faltantes. Verificar el objeto 'reporte.horario' ")
  
  # Guardo los datos diarios
  guardar.archivo = save.data(df, nombre.estat, "Datos_horarios", "Datos_process")
  
  # Gráfico la serie temporal
  
  carpeta_1 = "Graph_Agrupamiento_horario"
  carpeta_2 = "Estaciones_hora_process"
  graficos.iniciales = graficar(df, variable, nombre.estat, carpeta_1, carpeta_2)
  return(df)
}

# agrupamiento diario
agrupamiento.diario = function(df) {
  fecha.minima = min(df$TIMESTAMP)
  fecha.maxima = max(df$TIMESTAMP)
  
  umbral.min = 10 
  umbral.max = 15
  
  indices = which(is.na(df$Lluvia_Tot))
  fechas = df$TIMESTAMP[indices]
  fechas = data.frame(fechas)
  
  fechas.1 = as.Date(fechas$fechas, format = "%Y-%m-%d")
  fechas.1 = unique(fechas.1)
  fechas.1 = data.frame(fechas.1)
  
  resultados = lapply(1:nrow(fechas.1), function(i) {
    Li = as.POSIXct(paste0(fechas.1[i, ], " 00:00:00"), tz = "UTC")
    Ls = as.POSIXct(paste0(fechas.1[i, ], " 23:55:00"), tz = "UTC")
    
    fechas.filtradas = fechas %>%
      filter(fechas >= Li & fechas <= Ls)
    # Contar el número de fechas filtradas
    num.fechas = nrow(fechas.filtradas)
    
    return(num.fechas)
  })
  
  
  Nas = data.frame(fecha = fechas.1$fechas.1, num.fechas = unlist(resultados))
  names(Nas) = c("fecha", "Na")
  
  fechas.comparativa = seq(as.Date(fecha.minima), as.Date(fecha.maxima), by = "1 day")
  fechas.comparativa = data.frame(fechas.comparativa)
  names(fechas.comparativa) = c("fecha")
  
  conteo = merge(fechas.comparativa, Nas, by = "fecha", all = TRUE)
  conteo = data.frame(conteo)
  conteo$Na[is.na(conteo$Na)] = 0
  conteo$porcentaje = round((conteo$Na / 24) * 100,1)
  names(conteo) = c("fecha", "Na", "% Datos_faltantes")
  
  
  mayor.umbral = conteo %>% filter(conteo$`% Datos_faltantes` > umbral.min)
  faltantes = nrow(mayor.umbral)
  total = nrow(conteo)
  
  datos.NoAgrupadosDiarios <<- mayor.umbral
  # construcción de tabla de reporte
  fecha.i = as.Date(fecha.minima)
  fecha.f = as.Date(fecha.maxima)
  
  reporte = data.frame(
    Estacion = nombre.estat,
    periodo_estudio = paste(fecha.i, "al", fecha.f),
    Numero_años = as.numeric(year(fecha.f) - year(fecha.i)),
    Datos_registrados = total,
    Datos_ausentes = faltantes,
    porcentaje_completos = round(100 - ((faltantes / total) * 100),2),
    porcentaje_ausentes = round((faltantes / total) * 100,2)
  )
  
  reporte.diario <<- reporte
  dlg_message("Se ha generado un reporte de datos faltantes. Verificar el objeto 'reporte.diario' ")
  
  # Agrupación de datos de forma diaria --------------------------------------
  
  faltantes = mayor.umbral
  names(faltantes)[1] = "TIMESTAMP"
  
  df.1 = df %>% mutate(TIMESTAMP = as.POSIXct(TIMESTAMP)) %>%
    mutate(TIMESTAMP = floor_date(TIMESTAMP, "day")) %>%
    anti_join(faltantes, by = "TIMESTAMP") %>%
    group_by(TIMESTAMP) %>%
    summarise(!!variable := sum(!!sym(variable), na.rm = TRUE))
  
  fechas.completas = fechas.comparativa
  names(fechas.completas) = c("TIMESTAMP")
  
  df = left_join(fechas.completas, df.1, by = "TIMESTAMP") %>%
    distinct() %>%
    arrange(TIMESTAMP)
  
  # Guardo los datos diarios
  guardar.archivo = save.data(df, nombre.estat, "Datos_diarios", "Datos_process")
  return(df)
}

# ------------------------------------------------------------------------------

################################################################################
# ------------------------ Ejecución de la función -----------------------------
# Campos necesarios para la ejecución de la función
variable = "Lluvia_Tot" # Variable a analizar
nombre.estat = "MamamagM_min5" # Nombre de la estación
#--------------------- Ejecución del pre procesamiento --------------------------
df = leer.datos() # Leer datos
df = data_preprocess(df) # Pre procesamiento
df = limites.duros(df) # Límites duros

summary(df)
vacios = sum(is.na(df$Lluvia_Tot)) / nrow(df) * 100
vacios

fallas_sequias = posibles.fallas(df) # Posibles fallas
datos.horarios = agrupamiento.horario(fallas_sequias$df) # Agrupamiento horario
datos.diarios = agrupamiento.diario(datos.horarios) # Agrupamiento diario
################################################################################
