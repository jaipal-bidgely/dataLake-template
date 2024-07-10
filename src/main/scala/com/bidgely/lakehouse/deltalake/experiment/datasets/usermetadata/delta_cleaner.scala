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

    val deltaTablePath = "s3://bidgely-lakehouse-pocs/experiments/experiment2/deltalake/dedup"
    val deltaTable = DeltaTable.forPath(spark, deltaTablePath)

    deltaTable.vacuum(1.0)      // 1 hour

    spark.stop()

  }

}
