package com.bidgely.lakehouse.hudi.experiment.datasets.usermetadata

import com.bidgely.lakehouse.Hudi.{createSparkSession, getCleaningOptions, getClusteringOptions, getHudiOptions, loadData, writeHudiTable}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.functions.{col, input_file_name, udf}


/*
  gbdisagg-timeband-data with 3 pilot ids

  pilot_id=10010/         169.7 GB      18312 objects
  pilot_id=10011/         90.1 GB       11254 objects
  pilot_id=10015/         96.4 GB       12890 objects

  Total                   356.2 GB
*/


object Experiment3 {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    val readPath = "s3://bidgely-data-warehouse-prod-na/disagg/gbdisagg-timeband-data/v3/"
    val writePath_history = "s3://bidgely-lakehouse-pocs/experiments/experiment3/hudi/history"
    val writePath_dedup = "s3://bidgely-lakehouse-pocs/experiments/experiment3/hudi/dedup"

    val pilotIds = Seq("10010", "10011", "10015")

    var startTime = System.nanoTime()
    val df_history_temp = spark
      .read
      .option("basePath", readPath)
      .parquet(s"$readPath/pilot_id={10010,10011,10015}/")
    val df_filtered = df_history_temp.filter(col("app_id") =!= 99)
    var endTime = System.nanoTime()
    println(s"Hudi: Time taken for loading data (historic load) operation: ${(endTime - startTime) / 1e9} seconds")

    val pilotIdRegex = "(?<=pilot_id=)\\d+".r
    val extractPilotIdUDF = udf((path: String) => pilotIdRegex.findFirstIn(path).getOrElse(""))

    val df_history = df_filtered.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))

    val hudiOptions = getHudiOptions(
      tableName = "hudi_exp1_history",
      databaseName = "default",
      recordKeyField = "uuid",
      precombineField = "event_timestamp",
      partitionPathField = "pilot_id,app_id",
      writeOperation = "bulk_insert",
      indexType = None,                   // Some("GLOBAL_SIMPLE") or None or any other
      drop_duplicates = "false",
      writePath = writePath_history
    )

    val clusteringOptions = getClusteringOptions(
      layoutOptStrategy = "z-order",
      smallFileLimit = 1024 * 1024 * 1024, // 1Gb
      targetFileMaxBytes = 128 * 1024 * 1024, // 128Mb
      maxNumGroups = 4096,
      sortColumns = "uuid"
    )

    val cleaningOptions = getCleaningOptions(
      automatic = true,
      async = true,
      policy = "KEEP_LATEST_FILE_VERSIONS",
      retained = 1
    )
    startTime = System.nanoTime()
    //    writeHudiTable(spark, df_history, writePath_history, SaveMode.Overwrite, hudiOptions, Some(clusteringOptions), Some(cleaningOptions))    // Clustering and Cleaning
    // writeHudiTable(spark, df_history, writePath_history, SaveMode.Overwrite, hudiOptions, None, Some(cleaningOptions))                    // only Cleaning
    writeHudiTable(spark, df_history, writePath_history, SaveMode.Overwrite, hudiOptions, Some(clusteringOptions), None)                  // only Clustering
    // writeHudiTable(spark, df_history, writePath_history, SaveMode.Overwrite, hudiOptions, None, None)                                     // none
    endTime = System.nanoTime()
    println(s"Hudi: Time taken for write (historic load) operation:  ${(endTime - startTime) / 1e9} seconds")





    // INSERTING DATA OF AN app_id (HISTORY)
    val paths = Seq(
      s"$readPath/pilot_id=10010/app_id=99/",
      s"$readPath/pilot_id=10011/app_id=99/",
      s"$readPath/pilot_id=10015/app_id=99/"
    )

    paths.foreach { path =>
      // Load data
      val df_temp = spark
        .read
        .option("basePath", readPath)
        .parquet(path)
      val df = df_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))

      // Write to Hudi Table
      startTime = System.nanoTime()
      writeHudiTable(spark, df, writePath_history, SaveMode.Append, hudiOptions, Some(clusteringOptions), None)
      endTime = System.nanoTime()
      println(s"Hudi: Time taken for write (app_id=99) history operation: ${(endTime - startTime) / 1e9} seconds")
    }





    // CODE FOR DEDUP DATA


    val df_dedup_temp = spark
      .read
      .option("mergeSchema", "true")
      .option("basePath", readPath)
      .parquet(s"$readPath/pilot_id={10010,10011,10015}/")
    val df_dedup = df_dedup_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))

    val hudiOptions_dedup = getHudiOptions(
      tableName = "hudi_exp1_dedup",
      databaseName = "default",
      recordKeyField = "uuid",
      precombineField = "event_timestamp",
      partitionPathField = "pilot_id,app_id",
      writeOperation = "insert",
      indexType = None,                    // Some("GLOBAL_SIMPLE")
      drop_duplicates = "true",
      writePath = writePath_dedup
    )

    startTime = System.nanoTime()
    writeHudiTable(spark, df_dedup, writePath_dedup, SaveMode.Overwrite, hudiOptions_dedup, Some(clusteringOptions), None)
    endTime = System.nanoTime()
    println(s"Hudi: Time taken for write (historic load dedup) operation:  ${(endTime - startTime) / 1e9} seconds")




    // INSERTING DATA OF AN app_id (DEDUP)
    val paths_dedup = Seq(
      s"$readPath/pilot_id=10010/app_id=99/",
      s"$readPath/pilot_id=10011/app_id=99/",
      s"$readPath/pilot_id=10015/app_id=99/"
    )


    val hudiOptions_dedup_upsert = getHudiOptions(
      tableName = "hudi_exp1_dedup",
      databaseName = "default",
      recordKeyField = "uuid",
      precombineField = "event_timestamp",
      partitionPathField = "pilot_id,app_id",
      writeOperation = "upsert",
      indexType = None,                    // Some("GLOBAL_SIMPLE")
      drop_duplicates = "true",
      writePath = writePath_dedup
    )

    paths_dedup.foreach { path_dedup =>
      // Load data
      val df_temp = spark
        .read
        .option("basePath", readPath)
        .parquet(path_dedup)
      val df = df_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))

      // Write to Hudi Table
      startTime = System.nanoTime()
      writeHudiTable(spark, df, writePath_dedup, SaveMode.Append, hudiOptions_dedup_upsert, Some(clusteringOptions), None)
      endTime = System.nanoTime()
      println(s"Hudi: Time taken for write (app_id=99) dedup operation: ${(endTime - startTime) / 1e9} seconds")
    }


    //    timeTravelQuery(spark, writePath_history, "2024-06-26")

    spark.stop()
  }
}
