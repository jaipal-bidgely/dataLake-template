package com.bidgely.lakehouse.hudi.experiment.datasets.usermetadata

import org.apache.hudi.config.HoodieClusteringConfig
import org.apache.spark.sql.functions.{col, date_format, struct, udf}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object Hudi {

  def main(args: Array[String]): Unit = {


    // ------------------------------------------------------------------------------------------------------------
    val spark = SparkSession.builder()
      .appName("Hudi")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .getOrCreate()

    // Define the paths
    val readPath = "s3://bidgely-adhoc-dev/dhruv/read"
    val writePath = "s3://bidgely-adhoc-dev/dhruv/write"

    // Load data for 2020 and 2021
    val df2020_2021 = spark.read.parquet(
      s"$readPath/event_month=2020-12-01/",
      s"$readPath/event_month=2021-01-01/",
      s"$readPath/event_month=2021-02-01/",
      s"$readPath/event_month=2021-03-01/",
      s"$readPath/event_month=2021-04-01/",
      s"$readPath/event_month=2021-05-01/",
      s"$readPath/event_month=2021-06-01/",
      s"$readPath/event_month=2021-07-01/",
      s"$readPath/event_month=2021-08-01/",
      s"$readPath/event_month=2021-09-01/",
      s"$readPath/event_month=2021-10-01/",
      s"$readPath/event_month=2021-11-01/",
      s"$readPath/event_month=2021-12-01/"
    ).withColumn("partitionpath", date_format(col("event_date"), "yyyy-MM"))

    // Insert data into Hudi table using bulk insert
    val hudiOptions = Map[String, String](
      "hoodie.table.name" -> "Test",
      "hoodie.datasource.write.recordkey.field" -> "uuid",
      "hoodie.datasource.write.precombine.field" -> "last_updated_timestamp",
      "hoodie.datasource.write.partitionpath.field" -> "partitionpath",
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> "bulk_insert", // insert, upsert, bulk_insert, delete
      "hoodie.index.type" -> "GLOBAL_SIMPLE", // SIMPLE, BLOOM, GLOBAL_BLOOM, RECORD_INDEX
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.insert.shuffle.parallelism" -> "4"
    )

    df2020_2021.write
      .format("hudi")
      .options(hudiOptions)
      .mode(SaveMode.Overwrite)
      .save(s"$writePath/sample_table")

    // Load data for 2022
    val df2022 = spark.read.parquet(
      s"$readPath/event_month=2022-01-01/",
      s"$readPath/event_month=2022-02-01/",
      s"$readPath/event_month=2022-03-01/",
      s"$readPath/event_month=2022-04-01/",
      s"$readPath/event_month=2022-05-01/",
      s"$readPath/event_month=2022-06-01/",
      s"$readPath/event_month=2022-07-01/",
      s"$readPath/event_month=2022-08-01/",
      s"$readPath/event_month=2022-09-01/",
      s"$readPath/event_month=2022-10-01/"
    ).withColumn("partitionpath", date_format(col("event_date"), "yyyy-MM"))

    // Upsert data into Hudi table
    val hudiUpsertOptions = Map[String, String](
      "hoodie.table.name" -> "Test",
      "hoodie.datasource.write.recordkey.field" -> "uuid",
      "hoodie.datasource.write.precombine.field" -> "last_updated_timestamp",
      "hoodie.datasource.write.partitionpath.field" -> "partitionpath",
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> "upsert", // insert, upsert, bulk_insert, delete
      "hoodie.index.type" -> "GLOBAL_SIMPLE", // SIMPLE
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.insert.shuffle.parallelism" -> "4"
    )

    df2022.write
      .format("hudi")
      .options(hudiUpsertOptions)
      .mode(SaveMode.Append)
      .save(s"$writePath/sample_table")

    // Read and show the Hudi table
    val finalTable = spark.read.format("hudi").load(s"$writePath/sample_table")
    finalTable.show()


    val timeTravelDF = spark.read.format("hudi")
      .option("as.of.instant",  "2024-06-21") // since all data was inserted in 2024-06-07
      .load(s"$writePath/sample_table")

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
//      HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key -> "state"      //optional, if sorting is needed as part of rewriting data
    )
//     can use this options during write.

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
