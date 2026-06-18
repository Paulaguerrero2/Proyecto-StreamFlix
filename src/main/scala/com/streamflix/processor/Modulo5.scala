package com.streamflix.processor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object Modulo5 {

  def iniciarModulo5(finalDF: DataFrame, moviesDF: DataFrame)(implicit spark: SparkSession): Unit = {

  // Asegurarse de que las columnas de partición existan y estén limpias

    // Unir logs con películas para obtener country 
    val joinedDF = finalDF.join(
      moviesDF,
      finalDF("value").contains(moviesDF("title"))
    )

    // Crear columna año
    val finalReportDF = joinedDF
      .withColumn("year", year(col("timestamp")))

    println("=== ESCRIBIENDO PARQUET ===")

    // Guardar datos en parquet con particiones
    finalReportDF.write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "country")
      .parquet("src/main/resources/output/analytics_warehouse")

    println("=== Datos guardados en parquet ===")


  }

}
