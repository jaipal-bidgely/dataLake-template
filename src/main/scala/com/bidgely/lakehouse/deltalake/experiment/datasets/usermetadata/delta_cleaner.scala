package com.bidgely.lakehouse.deltalake.experiment.datasets.usermetadata

import org.apache.spark.sql.SparkSession
import io.delta.tables._

object delta_cleaner {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .appName("DeltaLakeExample")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") // Disable retention duration check
      .getOrCreate()

    val deltaTablePath_dedup = "s3://bidgely-lakehouse-pocs/experiments/experiment4/deltalake/dedup/"
    val deltaTablePath_history = "s3://bidgely-lakehouse-pocs/experiments/experiment2/deltalake/_history/"
    val deltaTable_dedup = DeltaTable.forPath(spark, deltaTablePath_dedup)
    val deltaTable_history = DeltaTable.forPath(spark, deltaTablePath_history)

    // Configure the target file size for optimization
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 256 * 1024 * 1024) // 256 MB


//    // for dedup
    var startTime = System.nanoTime()
//    deltaTable_dedup.optimize().executeZOrderBy("uuid")
    var endTime = System.nanoTime()
//    println(s"Delta: Time taken for z-order clustering: ${(endTime - startTime) / 1e9} seconds for dedup")
//
//    startTime = System.nanoTime()
//    deltaTable_dedup.vacuum(3.0)      // 1 hour
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for cleaning: ${(endTime - startTime) / 1e9} seconds for dedup")


    // for history
    startTime = System.nanoTime()
//    deltaTable_history.optimize().executeZOrderBy("uuid")
    endTime = System.nanoTime()
    println(s"Delta: Time taken for z-order clustering: ${(endTime - startTime) / 1e9} seconds for history")

    startTime = System.nanoTime()
    deltaTable_history.vacuum(1.0)      // 1 hour
    endTime = System.nanoTime()
    println(s"Delta: Time taken for cleaning: ${(endTime - startTime) / 1e9} seconds for history")



    spark.stop()


  }

}
