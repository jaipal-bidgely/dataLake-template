package com.bidgely.lakehouse.deltalake.experiment.datasets.usermetadata

import com.bidgely.lakehouse.DeltaLake.{compactDeltaTable, createSparkSession, loadData, timeTravelQuery, upsertDeltaLakeTable, vacuumDeltaTable, writeDeltaLakeTable, zOrderClustering}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}


object Experiment1 {
  /**
   * Describe
   * @param args
   */
  def main(args: Array[String]) = {

//    write
//    deltaWrite
//    clean
//    clustering yes
//      precombine no

      val spark = createSparkSession()

      val readPath = "s3://bidgely-adhoc-dev/dhruv/read"
      val writePath_history = "s3://bidgely-adhoc-dev/dhruv/delta/Experiment1_history"
      val writePath_dedup = "s3://bidgely-adhoc-dev/dhruv/delta/Experiment1_dedup"

      // set the insert path as needed
      val insertPaths_history = Seq(
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
        s"$readPath/event_month=2021-12-01/",
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
      )

      // load the data from the insertPaths_history, give the partition column and set the format of it
      val df_history = loadData(spark, insertPaths_history, "event_date", "yyyy-MM")
      writeDeltaLakeTable(spark, df_history, writePath_history, SaveMode.Overwrite, "partitionpath")


      val insertPaths_dedup = Seq(
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
        s"$readPath/event_month=2021-12-01/",
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
      )

      val df_dedup = loadData(spark, insertPaths_dedup, "event_date", "yyyy-MM")

      // deduplicating
      val windowSpec = Window.partitionBy("uuid").orderBy(col("last_updated_timestamp").desc)
      val df_dedup1 = df_dedup.withColumn("rank", row_number.over(windowSpec))
        .filter(col("rank") === 1)
        .drop("rank")
      writeDeltaLakeTable(spark, df_dedup1, writePath_dedup, SaveMode.Overwrite, "partitionpath")

//      upsertDeltaLakeTable(spark, df_dedup, writePath_history, "uuid", "last_updated_timestamp")

      // compaction
      compactDeltaTable(spark, writePath_history)

      // z-order clustering
      zOrderClustering(spark, writePath_history, "uuid")

      // time traveling, last argument is version
      timeTravelQuery(spark, writePath_history, 0)

      // cleaning, give retention period in seconds
      vacuumDeltaTable(spark, writePath_history, 3600)

      // for dedup data
      compactDeltaTable(spark, writePath_dedup)
      zOrderClustering(spark, writePath_dedup, "uuid")
      vacuumDeltaTable(spark, writePath_dedup, 3600)

      spark.stop()

  }
}