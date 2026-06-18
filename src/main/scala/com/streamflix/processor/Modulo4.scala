package com.streamflix.processor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Modulo4 {

  def iniciarModulo4()(implicit spark: SparkSession): DataFrame = {

  // Calcular la diferencia en minutos entre 'timestamp' y 'prev_timestamp'

    println("=== MODULO 4 EJERCICIO 1 ===")

    // Leer logs
    val rawLogsDF = spark.read
      .text("src/main/resources/data/server_logs.txt")

    // Filtrar solo INFO
    val infoLogsDF = rawLogsDF.filter(
      col("value").startsWith("[INFO]")
    )

    // Extraer timestamp
    val logsWithTimestampDF = infoLogsDF
      .withColumn("timestamp", substring(col("value"), 8, 19))
      .withColumn("user_id",
        split(split(col("value"), "\\|")(1), ":")(1)
      )

    // Crear ventana (orden por tiempo)
    val userTimeWindow = Window.orderBy("timestamp")

    // Obtener timestamp anterior
    val lagDF = logsWithTimestampDF.withColumn("prev_timestamp",
      lag("timestamp", 1).over(userTimeWindow)
    )

    // Calcular diferencia en segundos
    val diffDF = lagDF.withColumn(
      "time_diff_seconds",
      unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_timestamp"))
    )

    // Mostrar resultado
    println("=== RESULTADO DIFERENCIA TIEMPOS ===")
    diffDF.show(false)



  // Crear una columna "is_binge" si la diferencia < 20 mins

    println("=== MODULO 4 EJERCICIO 2 ===")

    // Comprobar usuarios viendo contenido seguido

    val bingeDF = diffDF.withColumn(
      "is_binge",
      col("time_diff_seconds") < 1200
    )

    println("=== RESULTADO BINGE WATCHING ===")

    bingeDF.show(false)

    bingeDF

  // Generar un reporte de los "Top 10 Binge Watchers"

    println("=== MODULO 4 EJERCICIO 3 ===")

    // Filtrar solo binge
    val onlyBingeDF = bingeDF.filter(col("is_binge") === true)

    // Agrupar por usuario
    val groupedDF = onlyBingeDF.groupBy("user_id")

    // Contar
    val countDF = groupedDF.count()

    // Ordenar
    val topBingeDF = countDF.orderBy(desc("count"))

    // Top 10
    val top10DF = topBingeDF.limit(10)

    println("=== TOP 10 USUARIOS MÁS ENGANCHADOS ===")

    top10DF.show(false)

    // devolver
    top10DF


  // Validación Manual: El dataset contiene un usuario User:999 que tiene tiempos negativos (errores
  //de reloj). Hay que identificar y explicar cómo su lógica manejó ese caso.

    val cleanDF = diffDF.filter(
      col("time_diff_seconds") >= 0
    )

    println("=== LIMPIAR DATOS (ELIMINAR TIEMPOS NEGATIVOS) ===")

    cleanDF.show(false)

    cleanDF

  }
}
