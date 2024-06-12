package com.czl.datalake.template.delta.kafka

import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.hudi.config.HoodieClusteringConfig

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/22
 * Description:
 */
object Hudi {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("HudiExample")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .getOrCreate()

    // Create a sample DataFrame
    import spark.implicits._
    val data = Seq(
      ("1", "2023-06-01 00:00:00", "John", "Doe"),
      ("2", "2023-06-01 01:00:00", "Jane", "Smith"),
      ("3", "2023-06-01 02:00:00", "Sam", "Johnson")
    )

    val df = spark.createDataFrame(data).toDF("id", "timestamp", "first_name", "last_name")

    val myUDF = udf((row: Row) => {
      val id = row.getString(0)
      val timestamp = row.getString(1)
      val firstName = row.getString(2)
      val lastName = row.getString(3)
      s"$id, $timestamp, $firstName, $lastName"
    })

    spark.udf.register("myUDF", myUDF)
    val sampleData = df.withColumn("combined", myUDF(struct(df.columns.map(col): _*)))

    // Set Hudi table options
    val hudiOptions = Map[String, String](
      "hoodie.table.name" -> "Test",
      "hoodie.datasource.write.recordkey.field" -> "id",
      "hoodie.datasource.write.precombine.field" -> "timestamp",
      "hoodie.datasource.write.partitionpath.field" -> "last_name",
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> "bulk_insert",     // insert, upsert, bulk_insert, delete
      "hoodie.index.type" -> "GLOBAL_SIMPLE",      // SIMPLE, BLOOM, GLOBAL_BLOOM, RECORD_INDEX
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.insert.shuffle.parallelism" -> "4",
    )

    // Insert data into Hudi table using bulk insert
    df.write
      .format("hudi")
      .options(hudiOptions)
      .mode(SaveMode.Overwrite)
      .save("/tmp/hudi/sample_table")

    // Read the Hudi table
    val hudiTable = spark.read.format("hudi").load("/tmp/hudi/sample_table")
    hudiTable.show()

    // Deduplication and Update: Assume new data to update and deduplicate
    val newData = Seq(
      ("1", "2023-06-01 00:00:01", "John", "DoeUpdated"),
      ("2", "2023-06-01 01:00:00", "Jane", "Smith"),
      ("4", "2023-06-01 03:00:00", "Alice", "Williams")
    ).toDF("id", "timestamp", "first_name", "last_name")

    // Set Hudi table options for upsert
    val hudiUpsertOptions = Map[String, String](
      "hoodie.table.name" -> "Test",
      "hoodie.datasource.write.recordkey.field" -> "id",
      "hoodie.datasource.write.precombine.field" -> "timestamp",
      "hoodie.datasource.write.partitionpath.field" -> "last_name",
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> "upsert",     // insert, upsert, bulk_insert, delete
      "hoodie.index.type" -> "GLOBAL_SIMPLE",      // SIMPLE
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.insert.shuffle.parallelism" -> "4",
    )

    // Upsert data into Hudi table
    newData.write
      .format("hudi")
      .options(hudiUpsertOptions)
      .mode(SaveMode.Append)
      .save("/tmp/hudi/sample_table")

    val newTable = spark.read.format("hudi").load("/tmp/hudi/sample_table")
    newTable.show()

    // COMPACTION IS NOT AVAILABLE FOR COW TABLES IN HUDI


    val finalTable = spark.read.format("hudi").load("/tmp/hudi/sample_table")
    finalTable.show()


    // TIME TRAVEL

    val timeTravelDF = spark.read.format("hudi")
      .option("as.of.instant",  "2024-06-08") // since all data was inserted in 2024-06-07
      .load("/tmp/hudi/sample_table")

    timeTravelDF.show()


    // CLEANING
    // Set Hudi cleaning options for async cleaning along with writing
    val cleaningOptions = Map[String, String](
      "hoodie.clean.automatic" -> "true",
      "hoodie.clean.async" -> "true",
      "hoodie.clean.policy" -> "KEEP_LATEST_COMMITS",     // KEEP_LATEST_FILE_VERSIONS, KEEP_LATEST_BY_HOURS
      "hoodie.clean.max_commits" -> "10"
    )
    // can use this options while writing

    // or just run a separate process :
    //  spark-submit --master local --class org.apache.hudi.utilities.HoodieCleaner `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar`\
    //  --target-base-path /path/to/hoodie_table \
    //  --hoodie-conf hoodie.cleaner.policy=KEEP_LATEST_COMMITS \
    //  --hoodie-conf hoodie.cleaner.commits.retained=10 \
    //  --hoodie-conf hoodie.cleaner.parallelism=200


    // Z-ORDER CLUSTERING
    val layoutOptStrategy = "z-order"   // hilbert
    val clusteringOptions = Map[String, String](
      "hoodie.clustering.inline" -> "true",
      "hoodie.clustering.inline.max.commits" -> "1",
      // NOTE: Small file limit is intentionally kept _ABOVE_ target file-size max threshold for Clustering,
      // to force re-clustering
      "hoodie.clustering.plan.strategy.small.file.limit" -> String.valueOf(1024 * 1024 * 1024), // 1Gb
      "hoodie.clustering.plan.strategy.target.file.max.bytes" -> String.valueOf(128 * 1024 * 1024), // 128Mb
      "hoodie.clustering.plan.strategy.max.num.groups" -> String.valueOf(4096),
      HoodieClusteringConfig.LAYOUT_OPTIMIZE_ENABLE.key -> "true",
      HoodieClusteringConfig.LAYOUT_OPTIMIZE_STRATEGY.key -> layoutOptStrategy,
      HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key -> "product_id,customer_id"
    )
    // can use this options during write.

    ////    Trigger compaction on MoR tables ->
    //    val compactionOptions = Map(
    //      "hoodie.compact.inline" -> "true",
    //      "hoodie.compact.inline.max.delta.commits" -> "1"
    //    )
    //
    //    spark.read.format("hudi")
    //      .options(compactionOptions)
    //      .load("/tmp/hudi/sample_table")
    //      .write
    //      .format("hudi")
    //      .options(hudiOptions)
    //      .option("hoodie.datasource.write.operation", "compact")
    //      .mode("Append")
    //      .save("/tmp/hudi/sample_table")
    //


    //    // Perform compaction
    //    val compactionOptions = Map(
    //      TABLE_NAME_1.key -> "hudi_sample_table",
    //      ASYNC_COMPACT_ENABLE.key -> "false"
    //    )
    //
    //    spark.read.format("hudi")
    //      .options(compactionOptions)
    //      .load("/tmp/hudi/sample_table")
    //      .write.format("hudi")
    //      .options(compactionOptions)
    //      .mode(SaveMode.Append)
    //      .save("/tmp/hudi/sample_table")
    //
    //    // Verify the final table after compaction
    //    val finalTable = spark.read.format("hudi").load("/tmp/hudi/sample_table")
    //    finalTable.show()
    //
    spark.stop()

  }
}
