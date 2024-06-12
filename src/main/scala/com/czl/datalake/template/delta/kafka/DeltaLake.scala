package main.scala.com.czl.datalake.template.delta.kafka

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.{SparkSession, SaveMode}
import io.delta.tables._

class DeltaLake {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DeltaLakeExample")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Create a sample DataFrame
    import spark.implicits._
    val sampleData = Seq(
      ("1", "2023-06-01 00:00:00", "John", "Doe"),
      ("2", "2023-06-01 01:00:00", "Jane", "Smith"),
      ("3", "2023-06-01 02:00:00", "Sam", "Johnson")
    ).toDF("id", "timestamp", "first_name", "last_name")

    // Write the sample data to Delta Lake
    sampleData.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save("/tmp/delta/sample_table")

    // Read the Delta table
    val deltaTable = spark.read.format("delta").load("/tmp/delta/sample_table")
    deltaTable.show()

    // New data for upsert
    val newData = Seq(
      ("1", "2023-06-01 00:00:01", "John", "DoeUpdated"),
      ("2", "2023-06-01 01:00:00", "Jane", "Smith"),
      ("4", "2023-06-01 03:00:00", "Alice", "Williams")
    ).toDF("id", "timestamp", "first_name", "last_name")

    // Perform upsert (merge) operation
    // https://docs.delta.io/latest/delta-update.html#slowly-changing-data-scd-type-2-operation-into-delta-tables
    val deltaTableInstance = DeltaTable.forPath("/tmp/delta/sample_table")

    deltaTableInstance
      .as("t")
      .merge(
        newData.as("s"),
        "t.id = s.id"
      )
      .whenMatched("t.timestamp < s.timestamp")
      .updateExpr(
        Map(
          "timestamp" -> "s.timestamp",
          "first_name" -> "s.first_name",
          "last_name" -> "s.last_name"
        )
      )
      .whenNotMatched()
      .insertAll()
      .execute()

    val mergeTable = spark.read.format("delta").load("/tmp/delta/sample_table")
    mergeTable.show()

    // Compaction
    val deltaTableToCompact = DeltaTable.forPath(spark, "/tmp/delta/sample_table")
    deltaTableToCompact.optimize().executeCompaction()

    // Verify the final table after compaction
    val finalTable = spark.read.format("delta").load("/tmp/delta/sample_table")
    finalTable.show()

    // Z-ORDER clustering
    val deltaTableForClustering = DeltaTable.forPath(spark, "/tmp/delta/sample_table")
    deltaTableForClustering.optimize().executeZOrderBy("timestamp", "id") // example

    // Time travel
    val dfVersion = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta/sample_table")
    dfVersion.show()

    //    // Query by timestamp (this thing requires exact timestamp)
    //    val dfTimestamp = spark.read.format("delta").option("timestampAsOf", "2024-06-12 00:00:00").load("/tmp/delta/sample_table")
    //    dfTimestamp.show()

    // Cleaning / VACUUM
    deltaTableToCompact.vacuum()

    // VACUUM with a custom retention period (e.g., 1 hour)
    deltaTableToCompact.vacuum(3600) // 3600 seconds = 1 hour

    spark.stop()
  }
}
