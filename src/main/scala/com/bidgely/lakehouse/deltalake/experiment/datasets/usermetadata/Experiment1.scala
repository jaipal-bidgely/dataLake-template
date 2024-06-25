package com.bidgely.lakehouse.deltalake.experiment.datasets.usermetadata

import com.bidgely.lakehouse.DeltaLake.{compactDeltaTable, createSparkSession, loadData, timeTravelQuery, upsertDeltaLakeTable, vacuumDeltaTable, writeDeltaLakeTable, zOrderClustering}
import org.apache.spark.sql.SaveMode


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

    def main(args: Array[String]): Unit = {
      val spark = createSparkSession()

      val readPath = "s3://bidgely-adhoc-dev/dhruv/read"
      val writePath = "s3://bidgely-adhoc-dev/dhruv/delta/write"

      // set the insert path as needed
      val insertPaths = Seq(
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
      )

      // load the data from the insertpaths, give the partition column and set the format of it
      val data2020_2021 = loadData(spark, insertPaths, "event_date", "yyyy-MM")
      writeDeltaLakeTable(spark, data2020_2021, writePath, SaveMode.Overwrite)

      // set the upsert paths as needed
      val upsertPaths = Seq(
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

      val data2022 = loadData(spark, upsertPaths, "event_date", "yyyy-MM")
      upsertDeltaLakeTable(spark, data2022, writePath, "uuid", "last_updated_timestamp")

      // compaction
      compactDeltaTable(spark, writePath)

      // z-order clustering
      zOrderClustering(spark, writePath, "uuid")

      // time traveling, last argument is version
      timeTravelQuery(spark, writePath, 0)

      // cleaning, give retention period in seconds
      vacuumDeltaTable(spark, writePath, 3600)

      spark.stop()
    }

  }
}