package com.bidgely.lakehouse.hudi.experiment.datasets.usermetadata

import com.bidgely.lakehouse.Hudi.{createSparkSession, getCleaningOptions, getClusteringOptions, getHudiOptions, writeHudiTable}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, input_file_name, udf}


/*
  hybrid-disagg-electric with 6 pilot ids

  pilot_id=10010/         14.1 GB       202399 objects
  pilot_id=10011/          4.4 GB       144615 objects
  pilot_id=10012/         41.3 GB       232217 objects
  pilot_id=10013/         14.6 GB       203678 objects
  pilot_id=10014/         20.8 GB       160494 objects
  pilot_id=10015/          4.0 GB       129336 objects

  Total                   99.1 GB
*/


object Experiment4 {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    val readPath = "s3://bidgely-data-warehouse-prod-na/disagg/hybrid-disagg-electric/v4/"
    val writePath_history = "s3://bidgely-lakehouse-pocs/experiments/experiment4/hudi/history"
    val writePath_dedup = "s3://bidgely-lakehouse-pocs/experiments/experiment4/hudi/dedup"

    val pilotIds = Seq("10010", "10011", "10012", "10013", "10014", "10015")

    var startTime = System.nanoTime()
    val df_history_temp = spark
      .read
      .option("basePath", readPath)
      .parquet(s"$readPath/pilot_id={10010,10011,10012,10013,10014,10015}/")
    val df_filtered = df_history_temp.filter(col("bill_start_month") =!= "2023-07-01")
    var endTime = System.nanoTime()
    println(s"Hudi: Time taken for loading data (historic load) operation: ${(endTime - startTime) / 1e9} seconds")

    val pilotIdRegex = "(?<=pilot_id=)\\d+".r
    val extractPilotIdUDF = udf((path: String) => pilotIdRegex.findFirstIn(path).getOrElse(""))

    val df_history = df_filtered.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))

    val hudiOptions = getHudiOptions(
      tableName = "hudi_exp4_history",
      databaseName = "default",
      recordKeyField = "uuid",
      precombineField = "lastupdate",
      partitionPathField = "pilot_id",
      writeOperation = "bulk_insert",
      indexType = None,                   // Some("GLOBAL_SIMPLE") or None or any other
      drop_duplicates = "false",
      writePath = writePath_history
    )

    val clusteringOptions = getClusteringOptions(
      layoutOptStrategy = "z-order",
      smallFileLimit = 256 * 1024 * 1024, // 256Mb
      targetFileMaxBytes = 256 * 1024 * 1024, // 256Mb
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
//    writeHudiTable(spark, df_history, writePath_history, SaveMode.Overwrite, hudiOptions, Some(clusteringOptions), None)                  // only Clustering
    writeHudiTable(spark, df_history, writePath_history, SaveMode.Overwrite, hudiOptions, None, None)                                     // none
    endTime = System.nanoTime()
    println(s"Hudi: Time taken for write (historic load) operation:  ${(endTime - startTime) / 1e9} seconds")





    // INSERTING DATA OF A MONTH (HISTORY)
    val paths = Seq(
      s"$readPath/pilot_id=10010/bill_start_month=2023-07-01/",
      s"$readPath/pilot_id=10011/bill_start_month=2023-07-01/",
      s"$readPath/pilot_id=10012/bill_start_month=2023-07-01/",
      s"$readPath/pilot_id=10013/bill_start_month=2023-07-01/",
      s"$readPath/pilot_id=10014/bill_start_month=2023-07-01/",
      s"$readPath/pilot_id=10015/bill_start_month=2023-07-01/"
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
      writeHudiTable(spark, df, writePath_history, SaveMode.Append, hudiOptions, None, None)
      endTime = System.nanoTime()
      println(s"Hudi: Time taken for write (bill_start_month=2023-07-01) history operation: ${(endTime - startTime) / 1e9} seconds")
    }





    // CODE FOR DEDUP DATA


    val df_dedup_temp = spark
      .read
      .option("basePath", readPath)
      .parquet(s"$readPath/pilot_id={10010,10011,10012,10013,10014,10015}/")
    val df_filtered_dedup = df_dedup_temp.filter(col("bill_start_month") =!= "2023-07-01")
    val df_dedup = df_filtered_dedup.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))

    val hudiOptions_dedup = getHudiOptions(
      tableName = "hudi_exp4_dedup",
      databaseName = "default",
      recordKeyField = "uuid",
      precombineField = "lastupdate",
      partitionPathField = "pilot_id",
      writeOperation = "insert",
      indexType = None,                    // Some("GLOBAL_SIMPLE")
      drop_duplicates = "true",
      writePath = writePath_dedup
    )

    startTime = System.nanoTime()
    writeHudiTable(spark, df_dedup, writePath_dedup, SaveMode.Overwrite, hudiOptions_dedup, None, None)
    endTime = System.nanoTime()
    println(s"Hudi: Time taken for write (historic load dedup) operation:  ${(endTime - startTime) / 1e9} seconds")




    // INSERTING DATA OF A MONTH (DEDUP)
    val paths_dedup = Seq(
      s"$readPath/pilot_id=10010/bill_start_month=2023-07-01/",
      s"$readPath/pilot_id=10011/bill_start_month=2023-07-01/",
      s"$readPath/pilot_id=10012/bill_start_month=2023-07-01/",
      s"$readPath/pilot_id=10013/bill_start_month=2023-07-01/",
      s"$readPath/pilot_id=10014/bill_start_month=2023-07-01/",
      s"$readPath/pilot_id=10015/bill_start_month=2023-07-01/"
    )


    val hudiOptions_dedup_upsert = getHudiOptions(
      tableName = "hudi_exp4_dedup",
      databaseName = "default",
      recordKeyField = "uuid",
      precombineField = "lastupdate",
      partitionPathField = "pilot_id",
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
      writeHudiTable(spark, df, writePath_dedup, SaveMode.Append, hudiOptions_dedup_upsert, None, None)
      endTime = System.nanoTime()
      println(s"Hudi: Time taken for write (bill_start_month=2023-07-01) dedup operation: ${(endTime - startTime) / 1e9} seconds")
    }


    //    timeTravelQuery(spark, writePath_history, "2024-06-26")

    spark.stop()
  }
}
