package com.streamflix.processor

import org.apache.spark.SparkContext

object Modulo1 {

  def iniciarModulo1()(implicit sc: SparkContext) = {
    val rawLogsRDD = sc.textFile("src/main/resources/data/server_logs.txt")

    val filteredLogs = rawLogsRDD.filter(line =>
      line.startsWith("[ERROR]") || line.startsWith("[INFO]")
    )

    println("=== MODULO 1 EJERCICIO 1 ===")

    filteredLogs.take(10).foreach(println)


    val mappedLogs = filteredLogs.map(line => {
      val nivel = line.split(" ")(0)
      val mensaje = line

      (nivel, mensaje)
    })

    println("=== MODULO 1 EJERCICIO 2 ===")

    mappedLogs.take(10).foreach(println)



    val errores503 = filteredLogs.filter(line =>
      line.contains("Code:503")
    )

    val totalErrores503 = errores503.count()

    println("=== MODULO 1 EJERCICIO 3 ===")
    println("Errores 503: " + totalErrores503)


    println("=== MODULO 1 EJERCICIO 4 ===")

    val totalLogs = filteredLogs.count()

    val porcentajeErrores = (totalErrores503.toDouble / totalLogs) * 100

    println("Total logs válidos: " + totalLogs)
    println("Errores 503: " + totalErrores503)
    println("Porcentaje de errores: " + porcentajeErrores + "%")


    println("=== VALIDACION 1 ===")

    val totalOriginal = rawLogsRDD.count()
    val totalValidos = filteredLogs.count()

    val descartados = totalOriginal - totalValidos

    println("Lineas descartadas: " + descartados)


    println("=== VALIDACION 2 ===")

    val codigos = filteredLogs
      .filter(line => line.contains("Code"))
      .map(line => {
        val partes = line.split("\\|")
        val campoCodigo = partes.find(_.contains("Code")).get
        val codigo = campoCodigo.split(":")(1)
        (codigo, 1)
      })

    val conteoCodigos = codigos.reduceByKey((a, b) => a + b)

    conteoCodigos
      .map { case (codigo, cantidad) => codigo + ", " + cantidad }
      .take(10)
      .foreach(println)

    filteredLogs
  }
}
