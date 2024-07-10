package com.bidgely.lakehouse.hudi.experiment.datasets.usermetadata

import com.bidgely.lakehouse.Hudi.{createSparkSession, getCleaningOptions, getClusteringOptions, getHudiOptions, loadData, timeTravelQuery, writeHudiTable}
import org.apache.spark.sql.SaveMode

/*

*/

object Experiment1 {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    val readPath = "s3://bidgely-adhoc-dev/dhruv/read"
    val writePath_history = "s3://bidgely-adhoc-dev/dhruv/hudi/Experiment1_history"
    val writePath_dedup = "s3://bidgely-adhoc-dev/dhruv/hudi/Experiment1_dedup"

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
    val df_history = loadData(spark, insertPaths_history, "event_date", "yyyy-MM")

    val hudiOptions = getHudiOptions(
      tableName = "hudi_exp1_history",
      databaseName = "lakehouse_pocs",
      recordKeyField = "uuid",
      precombineField = "last_updated_timestamp",
      partitionPathField = "partitionpath",
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

    writeHudiTable(spark, df_history, writePath_history, SaveMode.Overwrite, hudiOptions, Some(clusteringOptions), Some(cleaningOptions))    // Clustering and Cleaning
    // writeHudiTable(spark, df_history, writePath_history, SaveMode.Overwrite, hudiOptions, None, Some(cleaningOptions))                    // only Cleaning
    // writeHudiTable(spark, df_history, writePath_history, SaveMode.Overwrite, hudiOptions, Some(clusteringOptions), None)                  // only Clustering
    // writeHudiTable(spark, df_history, writePath_history, SaveMode.Overwrite, hudiOptions, None, None)                                     // none

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
    val hudiOptions_dedup = getHudiOptions(
      tableName = "hudi_exp1_dedup",
      databaseName = "lakehouse_pocs",
      recordKeyField = "uuid",
      precombineField = "last_updated_timestamp",
      partitionPathField = "partitionpath",
      writeOperation = "insert",
      indexType = None,                    // Some("GLOBAL_SIMPLE")
      drop_duplicates = "true",
      writePath = writePath_dedup
    )

    writeHudiTable(spark, df_dedup, writePath_dedup, SaveMode.Overwrite, hudiOptions_dedup, Some(clusteringOptions), Some(cleaningOptions))

//    timeTravelQuery(spark, writePath_history, "2024-06-26")

    spark.stop()
  }
}
