package com.bidgely.lakehouse.hudi.experiment.datasets.usermetadata

import com.bidgely.lakehouse.Hudi.{createSparkSession, getCleaningOptions, getClusteringOptions, getHudiOptions, loadData, timeTravelQuery, writeHudiTable}
import org.apache.spark.sql.SaveMode

object Experiment1 {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    val readPath = "s3://bidgely-adhoc-dev/dhruv/read"
    val writePath = "s3://bidgely-adhoc-dev/dhruv/write"

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
    val df2020_2021 = loadData(spark, insertPaths, "event_date", "yyyy-MM")

    val hudiOptions = getHudiOptions(
      tableName = "Test",
      recordKeyField = "uuid",
      precombineField = "last_updated_timestamp",
      partitionPathField = "partitionpath",
      saveMode = SaveMode.Overwrite,
      indexType = Some("BLOOM")
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
      policy = "KEEP_LATEST_COMMITS",
      retained = 10
    )

    writeHudiTable(spark, df2020_2021, writePath, SaveMode.Overwrite, hudiOptions, Some(clusteringOptions), Some(cleaningOptions))    // Clustering and Cleaning
    // writeHudiTable(spark, df2020_2021, writePath, SaveMode.Overwrite, hudiOptions, None, Some(cleaningOptions))                    // only Cleaning
    // writeHudiTable(spark, df2020_2021, writePath, SaveMode.Overwrite, hudiOptions, Some(clusteringOptions), None)                  // only Clustering
    // writeHudiTable(spark, df2020_2021, writePath, SaveMode.Overwrite, hudiOptions, None, None)                                     // none

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
    val df2022 = loadData(spark, upsertPaths, "event_date", "yyyy-MM")

    writeHudiTable(spark, df2022, writePath, SaveMode.Append, hudiOptions, Some(clusteringOptions), Some(cleaningOptions))

    timeTravelQuery(spark, writePath, "2024-06-26")

    spark.stop()
  }
}
