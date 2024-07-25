package com.bidgely.lakehouse

import io.delta.tables._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, date_format, row_number}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DeltaLake {

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("DeltaLakeExample")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") // Disable retention duration check
      .getOrCreate()
  }

//  def loadData(
//                spark: SparkSession,
//                basePath: String,
//                selectColumns: Seq[String],
//                paths: String*
//              ): DataFrame = {
//    val df = spark.read.format("parquet").load(paths.map(basePath + "/" + _): _*)
//    df.select(selectColumns.head, selectColumns.tail: _*)
//  }

  def loadData(
                spark: SparkSession,
                readPaths: Seq[String],
                partitionCol: String,
                partitionFormat: String
              ): DataFrame = {
    val df = spark.read.option("mergeSchema", "true").parquet(readPaths: _*)
//    df.withColumn("partitionpath", date_format(col(partitionCol), partitionFormat))
    df
  }

  def writeDeltaLakeTable(
                           spark: SparkSession,
                           df: DataFrame,
                           writePath: String,
                           saveMode: SaveMode,
                           partitionKey: String
                         ): Unit = {
    df.write
      .format("delta")
      .mode(saveMode)
      .partitionBy(partitionKey)
      .option("overwriteSchema", "true")
      .save(writePath)
  }

  def writeDeltaLakeTable(
                           spark: SparkSession,
                           df: DataFrame,
                           writePath: String,
                           saveMode: SaveMode,
                           partitionKeys: Seq[String]
                         ): Unit = {
    df.write
      .format("delta")
      .mode(saveMode)
      .partitionBy(partitionKeys: _*)
      .option("overwriteSchema", "true")
      .save(writePath)
  }

  def upsertDeltaLakeTable(
                            spark: SparkSession,
                            df: DataFrame,
                            writePath: String,
                            primaryKey: String,
                            precombineKey: String
                          ): Unit = {
    val windowSpec = Window.partitionBy(primaryKey).orderBy(col(precombineKey).desc)
    val deduplicatedDF = df.withColumn("rank", row_number.over(windowSpec))
      .filter(col("rank") === 1)
      .drop("rank")

    val deltaTable = DeltaTable.forPath(writePath)
    val columns = deduplicatedDF.columns
    val updateMap = columns.filterNot(Set(primaryKey)).map(colName => colName -> s"s.$colName").toMap

    deltaTable.as("t")
      .merge(
        deduplicatedDF.as("s"),
        s"t.$primaryKey = s.$primaryKey"
      )
      .whenMatched(s"t.$precombineKey < s.$precombineKey")
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  def compactDeltaTable(spark: SparkSession, tablePath: String): Unit = {
    val deltaTable = DeltaTable.forPath(spark, tablePath)
    deltaTable.optimize().executeCompaction()
  }

  def zOrderClustering(spark: SparkSession, tablePath: String, columnName: String): Unit = {
    val deltaTable = DeltaTable.forPath(spark, tablePath)
    deltaTable.optimize().executeZOrderBy(columnName)
  }

  def timeTravelQuery(spark: SparkSession, writePath: String, version: Int): Unit = {
    val dfVersion = spark.read.format("delta").option("versionAsOf", version).load(writePath)
    dfVersion.show()
  }

  def vacuumDeltaTable(spark: SparkSession, tablePath: String, retentionPeriodSeconds: Long): Unit = {
    val deltaTable = DeltaTable.forPath(spark, tablePath)
    deltaTable.vacuum(retentionPeriodSeconds)
  }

}
