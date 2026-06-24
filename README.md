# Proyecto ETL StreamFlix

## Descripción
Proyecto ETL desarrollado con Apache Spark en Scala para procesar logs de películas y calcular KPIs.

## Cómo ejecutar el proyecto

Primero es necesario generar el archivo JAR del proyecto.

Una vez generado, se ejecuta con el siguiente comando:

spark-submit \
--class com.streamflix.Main \
target/scala-2.12/streamflix.jar \
src/main/resources/data/server_logs.txt \
src/main/resources/data/movies_metadata.csv \
output/

## Parámetros

- Ruta de logs: src/main/resources/data/server_logs.txt
- Ruta de metadata: src/main/resources/data/movies_metadata.csv
- Carpeta de salida: output/

## Resultado

El programa genera archivos en formato Parquet con los KPIs calculados.