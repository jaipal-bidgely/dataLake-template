package com.bidgely.lakehouse.hudi.experiment.datasets.usermetadata

import com.bidgely.lakehouse.Hudi.{createSparkSession, getCleaningOptions, getClusteringOptions, getHudiOptions, loadData, writeHudiTable}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, input_file_name, udf}

/*
  user_meta_data with 4 pilot ids

  pilot_id=10010/         14.2 GB       9495 objects
  pilot_id=10011/         10.5 GB       9214 objects
  pilot_id=10013/         35.5 GB       9562 objects
  pilot_id=10015/         9.7 GB        9260 objects

  Total                   69.9 GB
*/

object Experiment2 {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    val readPath = "s3://bidgely-data-warehouse-prod-na/user-home-data/home-meta-data/v3/"
    val writePath_history = "s3://bidgely-lakehouse-pocs/experiments/experiment2/hudi/history"
    val writePath_dedup = "s3://bidgely-lakehouse-pocs/experiments/experiment2/hudi/dedup"

    val pilotIds = Seq("10010", "10011", "10013", "10015")

    val insertPaths_history = Seq(
      s"$readPath/pilot_id=10010/event_month=2020-12-01/",
      s"$readPath/pilot_id=10010/event_month=2021-01-01/",
      s"$readPath/pilot_id=10010/event_month=2021-02-01/",
      s"$readPath/pilot_id=10010/event_month=2021-03-01/",
      s"$readPath/pilot_id=10010/event_month=2021-04-01/",
      s"$readPath/pilot_id=10010/event_month=2021-05-01/",
      s"$readPath/pilot_id=10010/event_month=2021-06-01/",
      s"$readPath/pilot_id=10010/event_month=2021-07-01/",
      s"$readPath/pilot_id=10010/event_month=2021-08-01/",
      s"$readPath/pilot_id=10010/event_month=2021-09-01/",
      s"$readPath/pilot_id=10010/event_month=2021-10-01/",
      s"$readPath/pilot_id=10010/event_month=2021-11-01/",
      s"$readPath/pilot_id=10010/event_month=2021-12-01/",
      s"$readPath/pilot_id=10010/event_month=2022-01-01/",
      s"$readPath/pilot_id=10010/event_month=2022-02-01/",
      s"$readPath/pilot_id=10010/event_month=2022-03-01/",
      s"$readPath/pilot_id=10010/event_month=2022-04-01/",
      s"$readPath/pilot_id=10010/event_month=2022-05-01/",
      s"$readPath/pilot_id=10010/event_month=2022-06-01/",
      s"$readPath/pilot_id=10010/event_month=2022-07-01/",
      s"$readPath/pilot_id=10010/event_month=2022-08-01/",
      s"$readPath/pilot_id=10010/event_month=2022-09-01/",
      s"$readPath/pilot_id=10010/event_month=2022-10-01/",
      s"$readPath/pilot_id=10010/event_month=2022-11-01/",
      s"$readPath/pilot_id=10010/event_month=2022-12-01/",
      s"$readPath/pilot_id=10010/event_month=2023-01-01/",
      s"$readPath/pilot_id=10010/event_month=2023-02-01/",
      s"$readPath/pilot_id=10010/event_month=2023-03-01/",
      s"$readPath/pilot_id=10010/event_month=2023-04-01/",
      s"$readPath/pilot_id=10010/event_month=2023-05-01/",
      s"$readPath/pilot_id=10010/event_month=2023-06-01/",
      s"$readPath/pilot_id=10010/event_month=2023-07-01/",
      s"$readPath/pilot_id=10010/event_month=2023-08-01/",
      s"$readPath/pilot_id=10010/event_month=2023-09-01/",
      s"$readPath/pilot_id=10010/event_month=2023-10-01/",
      s"$readPath/pilot_id=10010/event_month=2023-12-01/",
      s"$readPath/pilot_id=10010/event_month=2024-01-01/",
      s"$readPath/pilot_id=10010/event_month=2024-03-01/",
//      s"$readPath/pilot_id=10010/event_month=2024-05-01/",
      s"$readPath/pilot_id=10011/event_month=2020-12-01/",
      s"$readPath/pilot_id=10011/event_month=2021-01-01/",
      s"$readPath/pilot_id=10011/event_month=2021-02-01/",
      s"$readPath/pilot_id=10011/event_month=2021-03-01/",
      s"$readPath/pilot_id=10011/event_month=2021-04-01/",
      s"$readPath/pilot_id=10011/event_month=2021-05-01/",
      s"$readPath/pilot_id=10011/event_month=2021-06-01/",
      s"$readPath/pilot_id=10011/event_month=2021-07-01/",
      s"$readPath/pilot_id=10011/event_month=2021-08-01/",
      s"$readPath/pilot_id=10011/event_month=2021-09-01/",
      s"$readPath/pilot_id=10011/event_month=2021-10-01/",
      s"$readPath/pilot_id=10011/event_month=2021-11-01/",
      s"$readPath/pilot_id=10011/event_month=2021-12-01/",
      s"$readPath/pilot_id=10011/event_month=2022-01-01/",
      s"$readPath/pilot_id=10011/event_month=2022-02-01/",
      s"$readPath/pilot_id=10011/event_month=2022-03-01/",
      s"$readPath/pilot_id=10011/event_month=2022-04-01/",
      s"$readPath/pilot_id=10011/event_month=2022-05-01/",
      s"$readPath/pilot_id=10011/event_month=2022-06-01/",
      s"$readPath/pilot_id=10011/event_month=2022-07-01/",
      s"$readPath/pilot_id=10011/event_month=2022-08-01/",
      s"$readPath/pilot_id=10011/event_month=2022-09-01/",
      s"$readPath/pilot_id=10011/event_month=2022-10-01/",
      s"$readPath/pilot_id=10011/event_month=2022-11-01/",
      s"$readPath/pilot_id=10011/event_month=2022-12-01/",
      s"$readPath/pilot_id=10011/event_month=2023-01-01/",
      s"$readPath/pilot_id=10011/event_month=2023-02-01/",
      s"$readPath/pilot_id=10011/event_month=2023-03-01/",
      s"$readPath/pilot_id=10011/event_month=2023-04-01/",
      s"$readPath/pilot_id=10011/event_month=2023-05-01/",
      s"$readPath/pilot_id=10011/event_month=2023-06-01/",
      s"$readPath/pilot_id=10011/event_month=2023-07-01/",
      s"$readPath/pilot_id=10011/event_month=2023-08-01/",
      s"$readPath/pilot_id=10011/event_month=2023-09-01/",
      s"$readPath/pilot_id=10011/event_month=2023-10-01/",
      s"$readPath/pilot_id=10011/event_month=2024-01-01/",
      s"$readPath/pilot_id=10011/event_month=2024-02-01/",
      s"$readPath/pilot_id=10011/event_month=2024-03-01/",
      s"$readPath/pilot_id=10013/event_month=2020-12-01/",
      s"$readPath/pilot_id=10013/event_month=2021-01-01/",
      s"$readPath/pilot_id=10013/event_month=2021-02-01/",
      s"$readPath/pilot_id=10013/event_month=2021-03-01/",
      s"$readPath/pilot_id=10013/event_month=2021-04-01/",
      s"$readPath/pilot_id=10013/event_month=2021-05-01/",
      s"$readPath/pilot_id=10013/event_month=2021-06-01/",
      s"$readPath/pilot_id=10013/event_month=2021-07-01/",
      s"$readPath/pilot_id=10013/event_month=2021-08-01/",
      s"$readPath/pilot_id=10013/event_month=2021-09-01/",
      s"$readPath/pilot_id=10013/event_month=2021-10-01/",
      s"$readPath/pilot_id=10013/event_month=2021-11-01/",
      s"$readPath/pilot_id=10013/event_month=2021-12-01/",
      s"$readPath/pilot_id=10013/event_month=2022-01-01/",
      s"$readPath/pilot_id=10013/event_month=2022-02-01/",
      s"$readPath/pilot_id=10013/event_month=2022-03-01/",
      s"$readPath/pilot_id=10013/event_month=2022-04-01/",
      s"$readPath/pilot_id=10013/event_month=2022-05-01/",
      s"$readPath/pilot_id=10013/event_month=2022-06-01/",
      s"$readPath/pilot_id=10013/event_month=2022-07-01/",
      s"$readPath/pilot_id=10013/event_month=2022-08-01/",
      s"$readPath/pilot_id=10013/event_month=2022-09-01/",
      s"$readPath/pilot_id=10013/event_month=2022-10-01/",
      s"$readPath/pilot_id=10013/event_month=2022-11-01/",
      s"$readPath/pilot_id=10013/event_month=2022-12-01/",
      s"$readPath/pilot_id=10013/event_month=2023-01-01/",
      s"$readPath/pilot_id=10013/event_month=2023-02-01/",
      s"$readPath/pilot_id=10013/event_month=2023-03-01/",
      s"$readPath/pilot_id=10013/event_month=2023-04-01/",
      s"$readPath/pilot_id=10013/event_month=2023-05-01/",
      s"$readPath/pilot_id=10013/event_month=2023-06-01/",
      s"$readPath/pilot_id=10013/event_month=2023-07-01/",
      s"$readPath/pilot_id=10013/event_month=2023-08-01/",
      s"$readPath/pilot_id=10013/event_month=2023-09-01/",
      s"$readPath/pilot_id=10013/event_month=2023-10-01/",
      s"$readPath/pilot_id=10013/event_month=2023-11-01/",
      s"$readPath/pilot_id=10013/event_month=2023-12-01/",
//      s"$readPath/pilot_id=10013/event_month=2024-01-01/",
      s"$readPath/pilot_id=10015/event_month=2020-12-01/",
      s"$readPath/pilot_id=10015/event_month=2021-01-01/",
      s"$readPath/pilot_id=10015/event_month=2021-02-01/",
      s"$readPath/pilot_id=10015/event_month=2021-03-01/",
      s"$readPath/pilot_id=10015/event_month=2021-04-01/",
      s"$readPath/pilot_id=10015/event_month=2021-05-01/",
      s"$readPath/pilot_id=10015/event_month=2021-06-01/",
      s"$readPath/pilot_id=10015/event_month=2021-07-01/",
      s"$readPath/pilot_id=10015/event_month=2021-08-01/",
      s"$readPath/pilot_id=10015/event_month=2021-09-01/",
      s"$readPath/pilot_id=10015/event_month=2021-10-01/",
      s"$readPath/pilot_id=10015/event_month=2021-11-01/",
      s"$readPath/pilot_id=10015/event_month=2021-12-01/",
      s"$readPath/pilot_id=10015/event_month=2022-01-01/",
      s"$readPath/pilot_id=10015/event_month=2022-02-01/",
      s"$readPath/pilot_id=10015/event_month=2022-03-01/",
      s"$readPath/pilot_id=10015/event_month=2022-04-01/",
      s"$readPath/pilot_id=10015/event_month=2022-05-01/",
      s"$readPath/pilot_id=10015/event_month=2022-06-01/",
      s"$readPath/pilot_id=10015/event_month=2022-07-01/",
      s"$readPath/pilot_id=10015/event_month=2022-08-01/",
      s"$readPath/pilot_id=10015/event_month=2022-09-01/",
      s"$readPath/pilot_id=10015/event_month=2022-10-01/",
      s"$readPath/pilot_id=10015/event_month=2022-11-01/",
      s"$readPath/pilot_id=10015/event_month=2022-12-01/",
      s"$readPath/pilot_id=10015/event_month=2023-01-01/",
      s"$readPath/pilot_id=10015/event_month=2023-02-01/",
      s"$readPath/pilot_id=10015/event_month=2023-03-01/",
      s"$readPath/pilot_id=10015/event_month=2023-04-01/",
      s"$readPath/pilot_id=10015/event_month=2023-05-01/",
      s"$readPath/pilot_id=10015/event_month=2023-06-01/",
      s"$readPath/pilot_id=10015/event_month=2023-07-01/",
      s"$readPath/pilot_id=10015/event_month=2023-08-01/",
      s"$readPath/pilot_id=10015/event_month=2023-09-01/",
      s"$readPath/pilot_id=10015/event_month=2023-10-01/",
      s"$readPath/pilot_id=10015/event_month=2023-11-01/",
      s"$readPath/pilot_id=10015/event_month=2023-12-01/",
      s"$readPath/pilot_id=10015/event_month=2024-01-01/",
      s"$readPath/pilot_id=10015/event_month=2024-02-01/",
//      s"$readPath/pilot_id=10015/event_month=2024-03-01/"
    )

    var startTime = System.nanoTime()
    val df_history_temp = loadData(spark, insertPaths_history, "event_date", "yyyy-MM")
    var endTime = System.nanoTime()
    println(s"Hudi: Time taken for loading data (historic load) operation: ${(endTime - startTime) / 1e9} seconds")

    val pilotIdRegex = "(?<=pilot_id=)\\d+".r
    val extractPilotIdUDF = udf((path: String) => pilotIdRegex.findFirstIn(path).getOrElse(""))

    val df_history = df_history_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))

    val hudiOptions = getHudiOptions(
      tableName = "hudi_exp1_history",
      databaseName = "default",
      recordKeyField = "uuid",
      precombineField = "last_updated_timestamp",
      partitionPathField = "pilot_id",
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





    // INSERTING DATA OF A MONTH (HISTORY)
    val paths = Seq(
      s"$readPath/pilot_id=10010/event_month=2024-05-01/",
      s"$readPath/pilot_id=10013/event_month=2024-01-01/",
      s"$readPath/pilot_id=10015/event_month=2024-03-01/"
    )

    paths.foreach { path =>
      // Load data
      val df_temp = loadData(spark, Seq(path), "event_date", "yyyy-MM")
      val df = df_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))

      // Write to Hudi Table
      startTime = System.nanoTime()
      writeHudiTable(spark, df, writePath_history, SaveMode.Append, hudiOptions, Some(clusteringOptions), None)
      endTime = System.nanoTime()
      println(s"Hudi: Time taken for write (a month) operation: ${(endTime - startTime) / 1e9} seconds")
    }



    // CODE FOR DEDUP DATA

//    val insertPaths_dedup = Seq(
//      s"$readPath/pilot_id=10010/event_month=2020-12-01/",
//      s"$readPath/pilot_id=10010/event_month=2021-01-01/",
//      s"$readPath/pilot_id=10010/event_month=2021-02-01/",
//      s"$readPath/pilot_id=10010/event_month=2021-03-01/",
//      s"$readPath/pilot_id=10010/event_month=2021-04-01/",
//      s"$readPath/pilot_id=10010/event_month=2021-05-01/",
//      s"$readPath/pilot_id=10010/event_month=2021-06-01/",
//      s"$readPath/pilot_id=10010/event_month=2021-07-01/",
//      s"$readPath/pilot_id=10010/event_month=2021-08-01/",
//      s"$readPath/pilot_id=10010/event_month=2021-09-01/",
//      s"$readPath/pilot_id=10010/event_month=2021-10-01/",
//      s"$readPath/pilot_id=10010/event_month=2021-11-01/",
//      s"$readPath/pilot_id=10010/event_month=2021-12-01/",
//      s"$readPath/pilot_id=10010/event_month=2022-01-01/",
//      s"$readPath/pilot_id=10010/event_month=2022-02-01/",
//      s"$readPath/pilot_id=10010/event_month=2022-03-01/",
//      s"$readPath/pilot_id=10010/event_month=2022-04-01/",
//      s"$readPath/pilot_id=10010/event_month=2022-05-01/",
//      s"$readPath/pilot_id=10010/event_month=2022-06-01/",
//      s"$readPath/pilot_id=10010/event_month=2022-07-01/",
//      s"$readPath/pilot_id=10010/event_month=2022-08-01/",
//      s"$readPath/pilot_id=10010/event_month=2022-09-01/",
//      s"$readPath/pilot_id=10010/event_month=2022-10-01/",
//      s"$readPath/pilot_id=10010/event_month=2022-11-01/",
//      s"$readPath/pilot_id=10010/event_month=2022-12-01/",
//      s"$readPath/pilot_id=10010/event_month=2023-01-01/",
//      s"$readPath/pilot_id=10010/event_month=2023-02-01/",
//      s"$readPath/pilot_id=10010/event_month=2023-03-01/",
//      s"$readPath/pilot_id=10010/event_month=2023-04-01/",
//      s"$readPath/pilot_id=10010/event_month=2023-05-01/",
//      s"$readPath/pilot_id=10010/event_month=2023-06-01/",
//      s"$readPath/pilot_id=10010/event_month=2023-07-01/",
//      s"$readPath/pilot_id=10010/event_month=2023-08-01/",
//      s"$readPath/pilot_id=10010/event_month=2023-09-01/",
//      s"$readPath/pilot_id=10010/event_month=2023-10-01/",
//      s"$readPath/pilot_id=10010/event_month=2023-12-01/",
//      s"$readPath/pilot_id=10010/event_month=2024-01-01/",
//      s"$readPath/pilot_id=10010/event_month=2024-03-01/",
//      s"$readPath/pilot_id=10010/event_month=2024-05-01/",
//      s"$readPath/pilot_id=10011/event_month=2020-12-01/",
//      s"$readPath/pilot_id=10011/event_month=2021-01-01/",
//      s"$readPath/pilot_id=10011/event_month=2021-02-01/",
//      s"$readPath/pilot_id=10011/event_month=2021-03-01/",
//      s"$readPath/pilot_id=10011/event_month=2021-04-01/",
//      s"$readPath/pilot_id=10011/event_month=2021-05-01/",
//      s"$readPath/pilot_id=10011/event_month=2021-06-01/",
//      s"$readPath/pilot_id=10011/event_month=2021-07-01/",
//      s"$readPath/pilot_id=10011/event_month=2021-08-01/",
//      s"$readPath/pilot_id=10011/event_month=2021-09-01/",
//      s"$readPath/pilot_id=10011/event_month=2021-10-01/",
//      s"$readPath/pilot_id=10011/event_month=2021-11-01/",
//      s"$readPath/pilot_id=10011/event_month=2021-12-01/",
//      s"$readPath/pilot_id=10011/event_month=2022-01-01/",
//      s"$readPath/pilot_id=10011/event_month=2022-02-01/",
//      s"$readPath/pilot_id=10011/event_month=2022-03-01/",
//      s"$readPath/pilot_id=10011/event_month=2022-04-01/",
//      s"$readPath/pilot_id=10011/event_month=2022-05-01/",
//      s"$readPath/pilot_id=10011/event_month=2022-06-01/",
//      s"$readPath/pilot_id=10011/event_month=2022-07-01/",
//      s"$readPath/pilot_id=10011/event_month=2022-08-01/",
//      s"$readPath/pilot_id=10011/event_month=2022-09-01/",
//      s"$readPath/pilot_id=10011/event_month=2022-10-01/",
//      s"$readPath/pilot_id=10011/event_month=2022-11-01/",
//      s"$readPath/pilot_id=10011/event_month=2022-12-01/",
//      s"$readPath/pilot_id=10011/event_month=2023-01-01/",
//      s"$readPath/pilot_id=10011/event_month=2023-02-01/",
//      s"$readPath/pilot_id=10011/event_month=2023-03-01/",
//      s"$readPath/pilot_id=10011/event_month=2023-04-01/",
//      s"$readPath/pilot_id=10011/event_month=2023-05-01/",
//      s"$readPath/pilot_id=10011/event_month=2023-06-01/",
//      s"$readPath/pilot_id=10011/event_month=2023-07-01/",
//      s"$readPath/pilot_id=10011/event_month=2023-08-01/",
//      s"$readPath/pilot_id=10011/event_month=2023-09-01/",
//      s"$readPath/pilot_id=10011/event_month=2023-10-01/",
//      s"$readPath/pilot_id=10011/event_month=2024-01-01/",
//      s"$readPath/pilot_id=10011/event_month=2024-02-01/",
//      s"$readPath/pilot_id=10011/event_month=2024-03-01/",
//      s"$readPath/pilot_id=10013/event_month=2020-12-01/",
//      s"$readPath/pilot_id=10013/event_month=2021-01-01/",
//      s"$readPath/pilot_id=10013/event_month=2021-02-01/",
//      s"$readPath/pilot_id=10013/event_month=2021-03-01/",
//      s"$readPath/pilot_id=10013/event_month=2021-04-01/",
//      s"$readPath/pilot_id=10013/event_month=2021-05-01/",
//      s"$readPath/pilot_id=10013/event_month=2021-06-01/",
//      s"$readPath/pilot_id=10013/event_month=2021-07-01/",
//      s"$readPath/pilot_id=10013/event_month=2021-08-01/",
//      s"$readPath/pilot_id=10013/event_month=2021-09-01/",
//      s"$readPath/pilot_id=10013/event_month=2021-10-01/",
//      s"$readPath/pilot_id=10013/event_month=2021-11-01/",
//      s"$readPath/pilot_id=10013/event_month=2021-12-01/",
//      s"$readPath/pilot_id=10013/event_month=2022-01-01/",
//      s"$readPath/pilot_id=10013/event_month=2022-02-01/",
//      s"$readPath/pilot_id=10013/event_month=2022-03-01/",
//      s"$readPath/pilot_id=10013/event_month=2022-04-01/",
//      s"$readPath/pilot_id=10013/event_month=2022-05-01/",
//      s"$readPath/pilot_id=10013/event_month=2022-06-01/",
//      s"$readPath/pilot_id=10013/event_month=2022-07-01/",
//      s"$readPath/pilot_id=10013/event_month=2022-08-01/",
//      s"$readPath/pilot_id=10013/event_month=2022-09-01/",
//      s"$readPath/pilot_id=10013/event_month=2022-10-01/",
//      s"$readPath/pilot_id=10013/event_month=2022-11-01/",
//      s"$readPath/pilot_id=10013/event_month=2022-12-01/",
//      s"$readPath/pilot_id=10013/event_month=2023-01-01/",
//      s"$readPath/pilot_id=10013/event_month=2023-02-01/",
//      s"$readPath/pilot_id=10013/event_month=2023-03-01/",
//      s"$readPath/pilot_id=10013/event_month=2023-04-01/",
//      s"$readPath/pilot_id=10013/event_month=2023-05-01/",
//      s"$readPath/pilot_id=10013/event_month=2023-06-01/",
//      s"$readPath/pilot_id=10013/event_month=2023-07-01/",
//      s"$readPath/pilot_id=10013/event_month=2023-08-01/",
//      s"$readPath/pilot_id=10013/event_month=2023-09-01/",
//      s"$readPath/pilot_id=10013/event_month=2023-10-01/",
//      s"$readPath/pilot_id=10013/event_month=2023-11-01/",
//      s"$readPath/pilot_id=10013/event_month=2023-12-01/",
//      s"$readPath/pilot_id=10013/event_month=2024-01-01/",
//      s"$readPath/pilot_id=10015/event_month=2020-12-01/",
//      s"$readPath/pilot_id=10015/event_month=2021-01-01/",
//      s"$readPath/pilot_id=10015/event_month=2021-02-01/",
//      s"$readPath/pilot_id=10015/event_month=2021-03-01/",
//      s"$readPath/pilot_id=10015/event_month=2021-04-01/",
//      s"$readPath/pilot_id=10015/event_month=2021-05-01/",
//      s"$readPath/pilot_id=10015/event_month=2021-06-01/",
//      s"$readPath/pilot_id=10015/event_month=2021-07-01/",
//      s"$readPath/pilot_id=10015/event_month=2021-08-01/",
//      s"$readPath/pilot_id=10015/event_month=2021-09-01/",
//      s"$readPath/pilot_id=10015/event_month=2021-10-01/",
//      s"$readPath/pilot_id=10015/event_month=2021-11-01/",
//      s"$readPath/pilot_id=10015/event_month=2021-12-01/",
//      s"$readPath/pilot_id=10015/event_month=2022-01-01/",
//      s"$readPath/pilot_id=10015/event_month=2022-02-01/",
//      s"$readPath/pilot_id=10015/event_month=2022-03-01/",
//      s"$readPath/pilot_id=10015/event_month=2022-04-01/",
//      s"$readPath/pilot_id=10015/event_month=2022-05-01/",
//      s"$readPath/pilot_id=10015/event_month=2022-06-01/",
//      s"$readPath/pilot_id=10015/event_month=2022-07-01/",
//      s"$readPath/pilot_id=10015/event_month=2022-08-01/",
//      s"$readPath/pilot_id=10015/event_month=2022-09-01/",
//      s"$readPath/pilot_id=10015/event_month=2022-10-01/",
//      s"$readPath/pilot_id=10015/event_month=2022-11-01/",
//      s"$readPath/pilot_id=10015/event_month=2022-12-01/",
//      s"$readPath/pilot_id=10015/event_month=2023-01-01/",
//      s"$readPath/pilot_id=10015/event_month=2023-02-01/",
//      s"$readPath/pilot_id=10015/event_month=2023-03-01/",
//      s"$readPath/pilot_id=10015/event_month=2023-04-01/",
//      s"$readPath/pilot_id=10015/event_month=2023-05-01/",
//      s"$readPath/pilot_id=10015/event_month=2023-06-01/",
//      s"$readPath/pilot_id=10015/event_month=2023-07-01/",
//      s"$readPath/pilot_id=10015/event_month=2023-08-01/",
//      s"$readPath/pilot_id=10015/event_month=2023-09-01/",
//      s"$readPath/pilot_id=10015/event_month=2023-10-01/",
//      s"$readPath/pilot_id=10015/event_month=2023-11-01/",
//      s"$readPath/pilot_id=10015/event_month=2023-12-01/",
//      s"$readPath/pilot_id=10015/event_month=2024-01-01/",
//      s"$readPath/pilot_id=10015/event_month=2024-02-01/",
//      s"$readPath/pilot_id=10015/event_month=2024-03-01/"
//    )

    val insertPaths_dedup = insertPaths_history

    val df_dedup_temp = loadData(spark, insertPaths_dedup, "event_date", "yyyy-MM")
    val df_dedup = df_dedup_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))

    val hudiOptions_dedup = getHudiOptions(
      tableName = "hudi_exp1_dedup",
      databaseName = "default",
      recordKeyField = "uuid",
      precombineField = "last_updated_timestamp",
      partitionPathField = "pilot_id",
      writeOperation = "insert",
      indexType = None,                    // Some("GLOBAL_SIMPLE")
      drop_duplicates = "true",
      writePath = writePath_dedup
    )

    startTime = System.nanoTime()
    writeHudiTable(spark, df_dedup, writePath_dedup, SaveMode.Overwrite, hudiOptions_dedup, Some(clusteringOptions), None)
    endTime = System.nanoTime()
    println(s"Hudi: Time taken for write (historic load dedup) operation:  ${(endTime - startTime) / 1e9} seconds")




    // INSERTING DATA OF A MONTH (DEDUP)
    val paths_dedup = Seq(
      s"$readPath/pilot_id=10010/event_month=2024-05-01/",
      s"$readPath/pilot_id=10013/event_month=2024-01-01/",
      s"$readPath/pilot_id=10015/event_month=2024-03-01/"
    )


    val hudiOptions_dedup_upsert = getHudiOptions(
      tableName = "hudi_exp1_dedup",
      databaseName = "default",
      recordKeyField = "uuid",
      precombineField = "last_updated_timestamp",
      partitionPathField = "pilot_id",
      writeOperation = "upsert",
      indexType = None,                    // Some("GLOBAL_SIMPLE")
      drop_duplicates = "true",
      writePath = writePath_dedup
    )

    paths_dedup.foreach { path_dedup =>
      // Load data
      val df_temp = loadData(spark, Seq(path_dedup), "event_date", "yyyy-MM")
      val df = df_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))

      // Write to Hudi Table
      startTime = System.nanoTime()
      writeHudiTable(spark, df, writePath_dedup, SaveMode.Append, hudiOptions_dedup_upsert, Some(clusteringOptions), None)
      endTime = System.nanoTime()
      println(s"Hudi: Time taken for write (a month dedup) operation: ${(endTime - startTime) / 1e9} seconds")
    }


    //    timeTravelQuery(spark, writePath_history, "2024-06-26")

    spark.stop()
  }
}
