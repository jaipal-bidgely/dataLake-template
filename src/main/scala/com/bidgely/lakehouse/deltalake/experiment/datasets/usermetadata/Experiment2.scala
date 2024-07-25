package com.bidgely.lakehouse.deltalake.experiment.datasets.usermetadata

import com.bidgely.lakehouse.DeltaLake.{compactDeltaTable, createSparkSession, loadData, timeTravelQuery, upsertDeltaLakeTable, vacuumDeltaTable, writeDeltaLakeTable, zOrderClustering}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, input_file_name, row_number, udf}

/*
  user_meta_data with 4 pilot ids

  pilot_id=10010/         14.2 GB       9495 objects
  pilot_id=10011/         10.5 GB       9214 objects
  pilot_id=10013/         35.5 GB       9562 objects
  pilot_id=10015/         9.7 GB        9260 objects

  Total                   69.9 GB
*/


object Experiment2 {
  def main(args: Array[String]) = {

    val spark = createSparkSession()

    val readPath = "s3://bidgely-data-warehouse-prod-na/user-home-data/home-meta-data/v3/"
    val writePath_history = "s3://bidgely-lakehouse-pocs/experiments/experiment2/deltalake/_history"
    val writePath_dedup = "s3://bidgely-lakehouse-pocs/experiments/experiment2/deltalake/dedup"

    // set the insert path as needed
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
//      s"$readPath/pilot_id=10010/event_month=2024-03-01/",
      s"$readPath/pilot_id=10010/event_month=2024-05-01/",
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

    // load the data from the insertPaths_history, give the partition column and set the format of it
    var startTime = System.nanoTime()
    val df_history_temp = loadData(spark, insertPaths_history, "event_date", "yyyy-MM")
    var endTime = System.nanoTime()
    println(s"Delta: Time taken for loading data (historic load) operation: ${(endTime - startTime) / 1e9} seconds")

    // to find pilot id corresponding the path of file
    val pilotIdRegex = "(?<=pilot_id=)\\d+".r
    val extractPilotIdUDF = udf((path: String) => pilotIdRegex.findFirstIn(path).getOrElse(""))




    // INSERTING HISTORIC BULK LOAD
    val df_history = df_history_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))
    startTime = System.nanoTime()
    writeDeltaLakeTable(spark, df_history, writePath_history, SaveMode.Overwrite, "pilot_id")
    endTime = System.nanoTime()
    println(s"Delta: Time taken for write (historic load) operation: ${(endTime - startTime) / 1e9} seconds")
    // compaction
    startTime = System.nanoTime()
    compactDeltaTable(spark, writePath_history)
    endTime = System.nanoTime()
    println(s"Delta: Time taken for compaction: ${(endTime - startTime) / 1e9} seconds")

    // z-order clustering
    startTime = System.nanoTime()
    zOrderClustering(spark, writePath_history, "uuid")
    endTime = System.nanoTime()
    println(s"Delta: Time taken for z-order clustering: ${(endTime - startTime) / 1e9} seconds")
//
//    // cleaning, give retention period in hours
//    startTime = System.nanoTime()
//    vacuumDeltaTable(spark, writePath_history, 1)
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for cleaning: ${(endTime - startTime) / 1e9} seconds")




    // INSERTING DATA FOR ONE MONTH (HISTORY)
    val paths = Seq(
      s"$readPath/pilot_id=10010/event_month=2024-05-01/",
      s"$readPath/pilot_id=10013/event_month=2024-01-01/",
      s"$readPath/pilot_id=10015/event_month=2024-03-01/"
    )

    paths.foreach { path =>
      // Load data
      val df_temp = loadData(spark, Seq(path), "event_date", "yyyy-MM")
      val df = df_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))


      // Write to Delta Lake
      startTime = System.nanoTime()
      writeDeltaLakeTable(spark, df, writePath_history, SaveMode.Append, "pilot_id")
      endTime = System.nanoTime()
      println(s"\nDelta: Time taken for write (a month) operation: ${(endTime - startTime) / 1e9} seconds")

      // Compaction
      startTime = System.nanoTime()
      compactDeltaTable(spark, writePath_history)
      endTime = System.nanoTime()
      println(s"Delta: Time taken for compaction: ${(endTime - startTime) / 1e9} seconds")

      // Z-order clustering
      startTime = System.nanoTime()
      zOrderClustering(spark, writePath_history, "uuid")
      endTime = System.nanoTime()
      println(s"Delta: Time taken for z-order clustering: ${(endTime - startTime) / 1e9} seconds")
//
//      // Cleaning
//      startTime = System.nanoTime()
//      vacuumDeltaTable(spark, writePath_history, 1)
//      endTime = System.nanoTime()
//      println(s"Delta: Time taken for cleaning: ${(endTime - startTime) / 1e9} seconds")
    }





//    // CODE FOR DEDUP DATA
//
////    val insertPaths_dedup = Seq(
////      s"$readPath/pilot_id=10010/event_month=2020-12-01/",
////      s"$readPath/pilot_id=10010/event_month=2021-01-01/",
////      s"$readPath/pilot_id=10010/event_month=2021-02-01/",
////      s"$readPath/pilot_id=10010/event_month=2021-03-01/",
////      s"$readPath/pilot_id=10010/event_month=2021-04-01/",
////      s"$readPath/pilot_id=10010/event_month=2021-05-01/",
////      s"$readPath/pilot_id=10010/event_month=2021-06-01/",
////      s"$readPath/pilot_id=10010/event_month=2021-07-01/",
////      s"$readPath/pilot_id=10010/event_month=2021-08-01/",
////      s"$readPath/pilot_id=10010/event_month=2021-09-01/",
////      s"$readPath/pilot_id=10010/event_month=2021-10-01/",
////      s"$readPath/pilot_id=10010/event_month=2021-11-01/",
////      s"$readPath/pilot_id=10010/event_month=2021-12-01/",
////      s"$readPath/pilot_id=10010/event_month=2022-01-01/",
////      s"$readPath/pilot_id=10010/event_month=2022-02-01/",
////      s"$readPath/pilot_id=10010/event_month=2022-03-01/",
////      s"$readPath/pilot_id=10010/event_month=2022-04-01/",
////      s"$readPath/pilot_id=10010/event_month=2022-05-01/",
////      s"$readPath/pilot_id=10010/event_month=2022-06-01/",
////      s"$readPath/pilot_id=10010/event_month=2022-07-01/",
////      s"$readPath/pilot_id=10010/event_month=2022-08-01/",
////      s"$readPath/pilot_id=10010/event_month=2022-09-01/",
////      s"$readPath/pilot_id=10010/event_month=2022-10-01/",
////      s"$readPath/pilot_id=10010/event_month=2022-11-01/",
////      s"$readPath/pilot_id=10010/event_month=2022-12-01/",
////      s"$readPath/pilot_id=10010/event_month=2023-01-01/",
////      s"$readPath/pilot_id=10010/event_month=2023-02-01/",
////      s"$readPath/pilot_id=10010/event_month=2023-03-01/",
////      s"$readPath/pilot_id=10010/event_month=2023-04-01/",
////      s"$readPath/pilot_id=10010/event_month=2023-05-01/",
////      s"$readPath/pilot_id=10010/event_month=2023-06-01/",
////      s"$readPath/pilot_id=10010/event_month=2023-07-01/",
////      s"$readPath/pilot_id=10010/event_month=2023-08-01/",
////      s"$readPath/pilot_id=10010/event_month=2023-09-01/",
////      s"$readPath/pilot_id=10010/event_month=2023-10-01/",
////      s"$readPath/pilot_id=10010/event_month=2023-12-01/",
////      s"$readPath/pilot_id=10010/event_month=2024-01-01/",
////      s"$readPath/pilot_id=10010/event_month=2024-03-01/",
////      s"$readPath/pilot_id=10010/event_month=2024-05-01/",
////      s"$readPath/pilot_id=10011/event_month=2020-12-01/",
////      s"$readPath/pilot_id=10011/event_month=2021-01-01/",
////      s"$readPath/pilot_id=10011/event_month=2021-02-01/",
////      s"$readPath/pilot_id=10011/event_month=2021-03-01/",
////      s"$readPath/pilot_id=10011/event_month=2021-04-01/",
////      s"$readPath/pilot_id=10011/event_month=2021-05-01/",
////      s"$readPath/pilot_id=10011/event_month=2021-06-01/",
////      s"$readPath/pilot_id=10011/event_month=2021-07-01/",
////      s"$readPath/pilot_id=10011/event_month=2021-08-01/",
////      s"$readPath/pilot_id=10011/event_month=2021-09-01/",
////      s"$readPath/pilot_id=10011/event_month=2021-10-01/",
////      s"$readPath/pilot_id=10011/event_month=2021-11-01/",
////      s"$readPath/pilot_id=10011/event_month=2021-12-01/",
////      s"$readPath/pilot_id=10011/event_month=2022-01-01/",
////      s"$readPath/pilot_id=10011/event_month=2022-02-01/",
////      s"$readPath/pilot_id=10011/event_month=2022-03-01/",
////      s"$readPath/pilot_id=10011/event_month=2022-04-01/",
////      s"$readPath/pilot_id=10011/event_month=2022-05-01/",
////      s"$readPath/pilot_id=10011/event_month=2022-06-01/",
////      s"$readPath/pilot_id=10011/event_month=2022-07-01/",
////      s"$readPath/pilot_id=10011/event_month=2022-08-01/",
////      s"$readPath/pilot_id=10011/event_month=2022-09-01/",
////      s"$readPath/pilot_id=10011/event_month=2022-10-01/",
////      s"$readPath/pilot_id=10011/event_month=2022-11-01/",
////      s"$readPath/pilot_id=10011/event_month=2022-12-01/",
////      s"$readPath/pilot_id=10011/event_month=2023-01-01/",
////      s"$readPath/pilot_id=10011/event_month=2023-02-01/",
////      s"$readPath/pilot_id=10011/event_month=2023-03-01/",
////      s"$readPath/pilot_id=10011/event_month=2023-04-01/",
////      s"$readPath/pilot_id=10011/event_month=2023-05-01/",
////      s"$readPath/pilot_id=10011/event_month=2023-06-01/",
////      s"$readPath/pilot_id=10011/event_month=2023-07-01/",
////      s"$readPath/pilot_id=10011/event_month=2023-08-01/",
////      s"$readPath/pilot_id=10011/event_month=2023-09-01/",
////      s"$readPath/pilot_id=10011/event_month=2023-10-01/",
////      s"$readPath/pilot_id=10011/event_month=2024-01-01/",
////      s"$readPath/pilot_id=10011/event_month=2024-02-01/",
////      s"$readPath/pilot_id=10011/event_month=2024-03-01/",
////      s"$readPath/pilot_id=10013/event_month=2020-12-01/",
////      s"$readPath/pilot_id=10013/event_month=2021-01-01/",
////      s"$readPath/pilot_id=10013/event_month=2021-02-01/",
////      s"$readPath/pilot_id=10013/event_month=2021-03-01/",
////      s"$readPath/pilot_id=10013/event_month=2021-04-01/",
////      s"$readPath/pilot_id=10013/event_month=2021-05-01/",
////      s"$readPath/pilot_id=10013/event_month=2021-06-01/",
////      s"$readPath/pilot_id=10013/event_month=2021-07-01/",
////      s"$readPath/pilot_id=10013/event_month=2021-08-01/",
////      s"$readPath/pilot_id=10013/event_month=2021-09-01/",
////      s"$readPath/pilot_id=10013/event_month=2021-10-01/",
////      s"$readPath/pilot_id=10013/event_month=2021-11-01/",
////      s"$readPath/pilot_id=10013/event_month=2021-12-01/",
////      s"$readPath/pilot_id=10013/event_month=2022-01-01/",
////      s"$readPath/pilot_id=10013/event_month=2022-02-01/",
////      s"$readPath/pilot_id=10013/event_month=2022-03-01/",
////      s"$readPath/pilot_id=10013/event_month=2022-04-01/",
////      s"$readPath/pilot_id=10013/event_month=2022-05-01/",
////      s"$readPath/pilot_id=10013/event_month=2022-06-01/",
////      s"$readPath/pilot_id=10013/event_month=2022-07-01/",
////      s"$readPath/pilot_id=10013/event_month=2022-08-01/",
////      s"$readPath/pilot_id=10013/event_month=2022-09-01/",
////      s"$readPath/pilot_id=10013/event_month=2022-10-01/",
////      s"$readPath/pilot_id=10013/event_month=2022-11-01/",
////      s"$readPath/pilot_id=10013/event_month=2022-12-01/",
////      s"$readPath/pilot_id=10013/event_month=2023-01-01/",
////      s"$readPath/pilot_id=10013/event_month=2023-02-01/",
////      s"$readPath/pilot_id=10013/event_month=2023-03-01/",
////      s"$readPath/pilot_id=10013/event_month=2023-04-01/",
////      s"$readPath/pilot_id=10013/event_month=2023-05-01/",
////      s"$readPath/pilot_id=10013/event_month=2023-06-01/",
////      s"$readPath/pilot_id=10013/event_month=2023-07-01/",
////      s"$readPath/pilot_id=10013/event_month=2023-08-01/",
////      s"$readPath/pilot_id=10013/event_month=2023-09-01/",
////      s"$readPath/pilot_id=10013/event_month=2023-10-01/",
////      s"$readPath/pilot_id=10013/event_month=2023-11-01/",
////      s"$readPath/pilot_id=10013/event_month=2023-12-01/",
////      s"$readPath/pilot_id=10013/event_month=2024-01-01/",
////      s"$readPath/pilot_id=10015/event_month=2020-12-01/",
////      s"$readPath/pilot_id=10015/event_month=2021-01-01/",
////      s"$readPath/pilot_id=10015/event_month=2021-02-01/",
////      s"$readPath/pilot_id=10015/event_month=2021-03-01/",
////      s"$readPath/pilot_id=10015/event_month=2021-04-01/",
////      s"$readPath/pilot_id=10015/event_month=2021-05-01/",
////      s"$readPath/pilot_id=10015/event_month=2021-06-01/",
////      s"$readPath/pilot_id=10015/event_month=2021-07-01/",
////      s"$readPath/pilot_id=10015/event_month=2021-08-01/",
////      s"$readPath/pilot_id=10015/event_month=2021-09-01/",
////      s"$readPath/pilot_id=10015/event_month=2021-10-01/",
////      s"$readPath/pilot_id=10015/event_month=2021-11-01/",
////      s"$readPath/pilot_id=10015/event_month=2021-12-01/",
////      s"$readPath/pilot_id=10015/event_month=2022-01-01/",
////      s"$readPath/pilot_id=10015/event_month=2022-02-01/",
////      s"$readPath/pilot_id=10015/event_month=2022-03-01/",
////      s"$readPath/pilot_id=10015/event_month=2022-04-01/",
////      s"$readPath/pilot_id=10015/event_month=2022-05-01/",
////      s"$readPath/pilot_id=10015/event_month=2022-06-01/",
////      s"$readPath/pilot_id=10015/event_month=2022-07-01/",
////      s"$readPath/pilot_id=10015/event_month=2022-08-01/",
////      s"$readPath/pilot_id=10015/event_month=2022-09-01/",
////      s"$readPath/pilot_id=10015/event_month=2022-10-01/",
////      s"$readPath/pilot_id=10015/event_month=2022-11-01/",
////      s"$readPath/pilot_id=10015/event_month=2022-12-01/",
////      s"$readPath/pilot_id=10015/event_month=2023-01-01/",
////      s"$readPath/pilot_id=10015/event_month=2023-02-01/",
////      s"$readPath/pilot_id=10015/event_month=2023-03-01/",
////      s"$readPath/pilot_id=10015/event_month=2023-04-01/",
////      s"$readPath/pilot_id=10015/event_month=2023-05-01/",
////      s"$readPath/pilot_id=10015/event_month=2023-06-01/",
////      s"$readPath/pilot_id=10015/event_month=2023-07-01/",
////      s"$readPath/pilot_id=10015/event_month=2023-08-01/",
////      s"$readPath/pilot_id=10015/event_month=2023-09-01/",
////      s"$readPath/pilot_id=10015/event_month=2023-10-01/",
////      s"$readPath/pilot_id=10015/event_month=2023-11-01/",
////      s"$readPath/pilot_id=10015/event_month=2023-12-01/",
////      s"$readPath/pilot_id=10015/event_month=2024-01-01/",
////      s"$readPath/pilot_id=10015/event_month=2024-02-01/",
////      //      s"$readPath/pilot_id=10015/event_month=2024-03-01/"
////    )
//    val insertPaths_dedup = insertPaths_history
//
//    val df_dedup_temp = loadData(spark, insertPaths_dedup, "event_date", "yyyy-MM")
//    val df_dedup_ = df_dedup_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))
//
//    // deduplicating
//    startTime = System.nanoTime()
//    val windowSpec = Window.partitionBy("uuid").orderBy(col("last_updated_timestamp").desc)
//    val df_dedup = df_dedup_.withColumn("rank", row_number.over(windowSpec))
//      .filter(col("rank") === 1)
//      .drop("rank")
//    endTime = System.nanoTime()
//    println(s"\nDelta: Time taken for dedup operation: ${(endTime - startTime) / 1e9} seconds")
//
//    startTime = System.nanoTime()
//    writeDeltaLakeTable(spark, df_dedup, writePath_dedup, SaveMode.Overwrite, "pilot_id")
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for write (historic load dedup) operation: ${(endTime - startTime) / 1e9} seconds")
//
//    // Compaction
//    startTime = System.nanoTime()
//    compactDeltaTable(spark, writePath_dedup)
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for compaction: ${(endTime - startTime) / 1e9} seconds")
//
//    // Z-order clustering
//    startTime = System.nanoTime()
//    zOrderClustering(spark, writePath_dedup, "uuid")
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for z-order clustering: ${(endTime - startTime) / 1e9} seconds")
////
////    // Cleaning
////    startTime = System.nanoTime()
////    vacuumDeltaTable(spark, writePath_dedup, 1)
////    endTime = System.nanoTime()
////    println(s"Delta: Time taken for cleaning: ${(endTime - startTime) / 1e9} seconds")
//
//
//
//
//    // INSERTING DATA FOR ONE MONTH (HISTORY)
//    val paths_dedup = Seq(
//      s"$readPath/pilot_id=10010/event_month=2024-05-01/",
//      s"$readPath/pilot_id=10013/event_month=2024-01-01/",
//      s"$readPath/pilot_id=10015/event_month=2024-03-01/"
//    )
//
//    paths_dedup.foreach { path_dedup =>
//      // Load data
//      val df_temp = loadData(spark, Seq(path_dedup), "event_date", "yyyy-MM")
//      val df_ = df_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))
//      val df = df_.withColumn("rank", row_number.over(windowSpec))
//        .filter(col("rank") === 1)
//        .drop("rank")
//
//      // Write to Delta Lake
//      startTime = System.nanoTime()
//      upsertDeltaLakeTable(spark, df, writePath_dedup, "uuid", "last_updated_timestamp")
//      endTime = System.nanoTime()
//      println(s"\nDelta: Time taken for write (a month) operation: ${(endTime - startTime) / 1e9} seconds")
//
//      // Compaction
//      startTime = System.nanoTime()
//      compactDeltaTable(spark, writePath_dedup)
//      endTime = System.nanoTime()
//      println(s"Delta: Time taken for compaction: ${(endTime - startTime) / 1e9} seconds")
//
//      // Z-order clustering
//      startTime = System.nanoTime()
//      zOrderClustering(spark, writePath_dedup, "uuid")
//      endTime = System.nanoTime()
//      println(s"Delta: Time taken for z-order clustering: ${(endTime - startTime) / 1e9} seconds")
////
////      // Cleaning
////      startTime = System.nanoTime()
////      vacuumDeltaTable(spark, writePath_dedup, 1)
////      endTime = System.nanoTime()
////      println(s"Delta: Time taken for cleaning: ${(endTime - startTime) / 1e9} seconds")
//    }
//
//
//


//    // Compaction
//    startTime = System.nanoTime()
//    compactDeltaTable(spark, writePath_dedup)
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for compaction: ${(endTime - startTime) / 1e9} seconds")
//
//    // Z-order clustering
//    startTime = System.nanoTime()
//    zOrderClustering(spark, writePath_dedup, "uuid")
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for z-order clustering: ${(endTime - startTime) / 1e9} seconds")

//    // Cleaning
//    startTime = System.nanoTime()
//    vacuumDeltaTable(spark, writePath_dedup, 1)
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for cleaning: ${(endTime - startTime) / 1e9} seconds")

//    val insertPaths_dedup1 = Seq(s"$readPath/pilot_id=10015/event_month=2024-03-01/")
//    val df_dedup1_temp = loadData(spark, insertPaths_dedup1, "event_date", "yyyy-MM")
//    val df_dedup1_ = df_dedup1_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))
//    val df_dedup1_deduped = df_dedup1_.withColumn("rank", row_number.over(windowSpec))
//      .filter(col("rank") === 1)
//      .drop("rank")
//
//    // filtering data of one day
//    val firstDate = df_dedup1_deduped
//      .select("event_date")
//      .distinct()
//      .orderBy("event_date")
//      .first()
//      .getAs[java.sql.Date]("event_date")
//
//    val df_dedup1 = df_dedup1_deduped.filter(col("event_date") === firstDate)
//    startTime = System.nanoTime()
//    writeDeltaLakeTable(spark, df_dedup1, writePath_dedup, SaveMode.Overwrite, "pilot_id")
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for write (1 day dedup) operation: ${(endTime - startTime) / 1e9} seconds")
//
//    // filtering data of any other day
//    val secondDate = df_dedup1_
//      .select("event_date")
//      .distinct()
//      .orderBy("event_date")
//      .filter(col("event_date") =!= firstDate)
//      .first()
//      .getAs[java.sql.Date]("event_date")
//
//    val df_dedup2 = df_dedup1_.filter(col("event_date") === secondDate)
//    startTime = System.nanoTime()
//    writeDeltaLakeTable(spark, df_dedup2, writePath_dedup, SaveMode.Overwrite, "pilot_id")
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for write (1 day dedup) operation: ${(endTime - startTime) / 1e9} seconds")
//
    //      upsertDeltaLakeTable(spark, df_dedup, writePath_history, "uuid", "last_updated_timestamp")




    // time traveling, last argument is version
//    timeTravelQuery(spark, writePath_history, 0)



    spark.stop()

  }
}
