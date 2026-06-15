// 1: Filtrar solo las líneas que empiezan con [ERROR] o [INFO]

package com.streamflix

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StreamFlix Analytics")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val rawLogsRDD = sc.textFile("src/main/resources/data/server_logs.txt")

    val filteredLogs = rawLogsRDD.filter(line =>
      line.startsWith("[ERROR]") || line.startsWith("[INFO]")
    )

    filteredLogs.collect().foreach(println)

    spark.stop()



// 2: Mapear para extraer (Nivel, Mensaje)

val mappedLogs = filteredLogs.map(line => {
  val nivel = line.split(" ")(0)
  val mensaje = line

  (nivel, mensaje)
})

mappedLogs.collect().foreach(println)


// Tarea 3: Contar cuántos errores de tipo 503 ocurrieron usando RDD actions (count, filter)

val errores503 = filteredLogs.filter(line =>
  line.contains("Code:503")
)

val totalErrores503 = errores503.count()

println("Errores 503: " + totalErrores503)
  }
}