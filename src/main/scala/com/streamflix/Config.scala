package com.streamflix

object Config {

  // RUTAS DE ENTRADA
  val logsPath = "src/main/resources/data/server_logs.txt"
  val moviesPath = "src/main/resources/data/movies_metadata.csv"

  // RUTA DE SALIDA
  val outputPath = "src/main/resources/output/analytics_warehouse"

  // CONFIGURACIÓN SPARK
  val appName = "StreamFlix Analytics"
  val master = "local[*]"

}
