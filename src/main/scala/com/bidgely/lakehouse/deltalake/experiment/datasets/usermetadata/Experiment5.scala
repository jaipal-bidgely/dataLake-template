package com.bidgely.lakehouse.deltalake.experiment.datasets.usermetadata

import com.bidgely.lakehouse.DeltaLake.{compactDeltaTable, createSparkSession, upsertDeltaLakeTable, writeDeltaLakeTable, zOrderClustering}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, input_file_name, row_number, udf}


/*
  utility-billing-data with 3 pilot ids

  pilot_id=10010/         71.5 GB       341643 objects
  pilot_id=10011/         14.8 GB       279710 objects
  pilot_id=10015/         24.9 GB       196684 objects

  Total                   111.2 GB
*/


object Experiment5 {
  def main(args: Array[String]) = {

    val spark = createSparkSession()

    val readPath = "s3://bidgely-data-warehouse-prod-na/user-billing-data/utility-billing-data/v4/"
    val writePath_history = "s3://bidgely-lakehouse-pocs/experiments/experiment5/deltalake/history"
    val writePath_dedup = "s3://bidgely-lakehouse-pocs/experiments/experiment5/deltalake/dedup"

    // set the insert path as needed
    val pilotIds = Seq("10010", "10011", "10015")


    var startTime = System.nanoTime()
    val df_history_temp = spark
      .read
      .option("basePath", readPath)
      .parquet(s"$readPath/pilot_id={10010,10011,10015}/")
    val df_filtered = df_history_temp.filter(col("bill_start_month") =!= "2023-08-01")
    var endTime = System.nanoTime()
    println(s"Delta: Time taken for loading data (historic load) operation: ${(endTime - startTime) / 1e9} seconds")

    // to find pilot id corresponding the path of file
    val pilotIdRegex = "(?<=pilot_id=)\\d+".r
    val extractPilotIdUDF = udf((path: String) => pilotIdRegex.findFirstIn(path).getOrElse(""))




    // INSERTING HISTORIC BULK LOAD
    val df_history = df_filtered.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))
    startTime = System.nanoTime()
    writeDeltaLakeTable(spark, df_history, writePath_history, SaveMode.Overwrite, Seq("pilot_id"))
    endTime = System.nanoTime()
    println(s"Delta: Time taken for write (historic load) operation: ${(endTime - startTime) / 1e9} seconds")

//    // compaction
//    startTime = System.nanoTime()
//    compactDeltaTable(spark, writePath_history)
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for compaction: ${(endTime - startTime) / 1e9} seconds")

//    // z-order clustering
//    startTime = System.nanoTime()
//    zOrderClustering(spark, writePath_history, "uuid")
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for z-order clustering: ${(endTime - startTime) / 1e9} seconds")
    //
    //    // cleaning, give retention period in hours
    //    startTime = System.nanoTime()
    //    vacuumDeltaTable(spark, writePath_history, 1)
    //    endTime = System.nanoTime()
    //    println(s"Delta: Time taken for cleaning: ${(endTime - startTime) / 1e9} seconds")




    // INSERTING DATA FOR ONE MONTH (HISTORY)
    val paths = Seq(
      s"$readPath/pilot_id=10010/bill_start_month=2023-08-01/",
      s"$readPath/pilot_id=10011/bill_start_month=2023-08-01/",
      s"$readPath/pilot_id=10015/bill_start_month=2023-08-01/"
    )

    paths.foreach { path =>
      // Load data
      val df_temp = spark
        .read
        .option("basePath", readPath)
        .parquet(path)
      val df = df_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))

      // Write to Delta Lake
      startTime = System.nanoTime()
      writeDeltaLakeTable(spark, df, writePath_history, SaveMode.Append, Seq("pilot_id"))
      endTime = System.nanoTime()
      println(s"\nDelta: Time taken for write (bill_start_month=2023-08-01) history operation: ${(endTime - startTime) / 1e9} seconds")

//      // Compaction
//      startTime = System.nanoTime()
//      compactDeltaTable(spark, writePath_history)
//      endTime = System.nanoTime()
//      println(s"Delta: Time taken for compaction: ${(endTime - startTime) / 1e9} seconds")

//      // Z-order clustering
//      startTime = System.nanoTime()
//      zOrderClustering(spark, writePath_history, "uuid")
//      endTime = System.nanoTime()
//      println(s"Delta: Time taken for z-order clustering: ${(endTime - startTime) / 1e9} seconds")
      //
      //      // Cleaning
      //      startTime = System.nanoTime()
      //      vacuumDeltaTable(spark, writePath_history, 1)
      //      endTime = System.nanoTime()
      //      println(s"Delta: Time taken for cleaning: ${(endTime - startTime) / 1e9} seconds")
    }





    // CODE FOR DEDUP DATA



    val df_dedup_temp = spark
      .read
      .option("basePath", readPath)
      .parquet(s"$readPath/pilot_id={10010,10011,10015}/")
    val df_filtered_dedup = df_dedup_temp.filter(col("bill_start_month") =!= "2023-08-01")

    val df_dedup_ = df_filtered_dedup.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))

    // deduplicating
    startTime = System.nanoTime()
    val windowSpec = Window.partitionBy("uuid").orderBy(col("lastupdate").desc)
    val df_dedup = df_dedup_.withColumn("rank", row_number.over(windowSpec))
      .filter(col("rank") === 1)
      .drop("rank")
    endTime = System.nanoTime()
    println(s"\nDelta: Time taken for dedup operation: ${(endTime - startTime) / 1e9} seconds")

    startTime = System.nanoTime()
    writeDeltaLakeTable(spark, df_dedup, writePath_dedup, SaveMode.Overwrite, Seq("pilot_id"))
    endTime = System.nanoTime()
    println(s"Delta: Time taken for write (historic load dedup) operation: ${(endTime - startTime) / 1e9} seconds")

//    // Compaction
//    startTime = System.nanoTime()
//    compactDeltaTable(spark, writePath_dedup)
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for compaction: ${(endTime - startTime) / 1e9} seconds")

//    // Z-order clustering
//    startTime = System.nanoTime()
//    zOrderClustering(spark, writePath_dedup, "uuid")
//    endTime = System.nanoTime()
//    println(s"Delta: Time taken for z-order clustering: ${(endTime - startTime) / 1e9} seconds")
    //
    //    // Cleaning
    //    startTime = System.nanoTime()
    //    vacuumDeltaTable(spark, writePath_dedup, 1)
    //    endTime = System.nanoTime()
    //    println(s"Delta: Time taken for cleaning: ${(endTime - startTime) / 1e9} seconds")




    // INSERTING DATA FOR ONE MONTH (HISTORY)
    val paths_dedup = Seq(
      s"$readPath/pilot_id=10010/bill_start_month=2023-08-01/",
      s"$readPath/pilot_id=10011/bill_start_month=2023-08-01/",
      s"$readPath/pilot_id=10015/bill_start_month=2023-08-01/"
    )

    paths_dedup.foreach { path_dedup =>
      // Load data
      val df_temp = spark
        .read
        .option("basePath", readPath)
        .parquet(path_dedup)

      val df_ = df_temp.withColumn("pilot_id", extractPilotIdUDF(input_file_name()))
      val df = df_.withColumn("rank", row_number.over(windowSpec))
        .filter(col("rank") === 1)
        .drop("rank")

      // Write to Delta Lake
      startTime = System.nanoTime()
      upsertDeltaLakeTable(spark, df, writePath_dedup, "uuid", "lastupdate")
      endTime = System.nanoTime()
      println(s"\nDelta: Time taken for write (bill_start_month=2023-08-01) dedup operation: ${(endTime - startTime) / 1e9} seconds")

//      // Compaction
//      startTime = System.nanoTime()
//      compactDeltaTable(spark, writePath_dedup)
//      endTime = System.nanoTime()
//      println(s"Delta: Time taken for compaction: ${(endTime - startTime) / 1e9} seconds")

//      // Z-order clustering
//      startTime = System.nanoTime()
//      zOrderClustering(spark, writePath_dedup, "uuid")
//      endTime = System.nanoTime()
//      println(s"Delta: Time taken for z-order clustering: ${(endTime - startTime) / 1e9} seconds")
      //
      //      // Cleaning
      //      startTime = System.nanoTime()
      //      vacuumDeltaTable(spark, writePath_dedup, 1)
      //      endTime = System.nanoTime()
      //      println(s"Delta: Time taken for cleaning: ${(endTime - startTime) / 1e9} seconds")
    }

    // time traveling, last argument is version
    //    timeTravelQuery(spark, writePath_history, 0)


    spark.stop()

  }
}
