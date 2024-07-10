package com.bidgely.lakehouse

import org.apache.hudi.config.HoodieClusteringConfig
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Hudi {

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("Hudi")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
//      .config("spark.sql.hive.metastore.jars", "/usr/lib/hive/lib/*")
//      .config("spark.hadoop.hive.metastore.uris", "thrift://ip-10-0-0-73.us-west-2.compute.internal:9083") // Update with your metastore URI
//      .enableHiveSupport()
      .getOrCreate()
  }

  def loadData(
                spark: SparkSession,
                readPaths: Seq[String],
                partitionCol: String,
                partitionFormat: String
              ): DataFrame = {
    val df = spark.read.parquet(readPaths: _*)
//    df.withColumn("partitionpath", date_format(col(partitionCol), partitionFormat))
    df
  }

  def getHudiOptions(
                      tableName: String,
                      databaseName: String,
                      recordKeyField: String,
                      precombineField: String,
                      partitionPathField: String,
                      writeOperation: String,
                      indexType: Option[String],
                      drop_duplicates: String,
                      writePath: String
                    ): Map[String, String] = {
    val baseOptions = Map[String, String](
      "hoodie.table.name" -> tableName,
      "hoodie.datasource.write.recordkey.field" -> recordKeyField,
      "hoodie.datasource.write.precombine.field" -> precombineField,
      "hoodie.datasource.write.partitionpath.field" -> partitionPathField,
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> writeOperation,
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.datasource.write.insert.drop.duplicates" -> drop_duplicates,
//      "hoodie.datasource.hive_sync.enable" -> "true",
//      "hoodie.datasource.hive_sync.mode" -> "hms",
//      "hoodie.datasource.hive_sync.sync_as_datasource" -> "false",
//      "hoodie.datasource.hive_sync.database" -> databaseName,
//      "hoodie.datasource.hive_sync.table" -> tableName,
//      "hoodie.datasource.hive_sync.partition_fields" -> partitionPathField,
//      "hoodie.datasource.hive_sync.use_jdbc" -> "false",
//      "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.MultiPartKeysValueExtractor",
//      "hoodie.datasource.hive_sync.hive_style_partitioning" -> "true",
      "hoodie.datasource.write.hive_style_partitioning" -> "true",
//      "path" -> writePath
    )

    val indexTypeOption = indexType match {
      case Some(it) => Map("hoodie.index.type" -> it)
      case None => Map.empty[String, String]
    }

    baseOptions ++ indexTypeOption
  }

  def getClusteringOptions(
                            layoutOptStrategy: String,
                            smallFileLimit: Long,
                            targetFileMaxBytes: Long,
                            maxNumGroups: Int,
                            sortColumns: String
                          ): Map[String, String] = {
    Map[String, String](
      "hoodie.clustering.inline" -> "true",
      "hoodie.clustering.inline.max.commits" -> "1",
      "hoodie.clustering.plan.strategy.small.file.limit" -> smallFileLimit.toString, // small file limit
      "hoodie.clustering.plan.strategy.target.file.max.bytes" -> targetFileMaxBytes.toString, // target file max bytes
      "hoodie.clustering.plan.strategy.max.num.groups" -> maxNumGroups.toString, // max num groups
      HoodieClusteringConfig.LAYOUT_OPTIMIZE_ENABLE.key -> "true",
      HoodieClusteringConfig.LAYOUT_OPTIMIZE_STRATEGY.key -> layoutOptStrategy, // layout optimize strategy
      HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key -> sortColumns // sort columns
    )
  }

  def getCleaningOptions(automatic: Boolean, async: Boolean, policy: String, retained: Int): Map[String, String] = {
    val commonOptions = Map[String, String](
      "hoodie.clean.automatic" -> automatic.toString,
      "hoodie.clean.async" -> async.toString,
      "hoodie.clean.policy" -> policy
    )

    val specificOptions = policy match {
      case "KEEP_LATEST_COMMITS" =>
        Map("hoodie.cleaner.commits.retained" -> retained.toString)
      case "KEEP_LATEST_FILE_VERSIONS" =>
        Map("hoodie.cleaner.fileversions.retained" -> retained.toString)
      case "KEEP_LATEST_BY_HOURS" =>
        Map("hoodie.cleaner.hours.retained" -> retained.toString)
      case _ =>
        throw new IllegalArgumentException(s"Unknown cleaning policy: $policy")
    }

    commonOptions ++ specificOptions
  }

  def writeHudiTable(
                      spark: SparkSession,
                      df: DataFrame,
                      writePath: String,
                      saveMode: SaveMode,
                      hudiOptions: Map[String, String],
                      clusteringOptions: Option[Map[String, String]],
                      cleaningOptions: Option[Map[String, String]]
                    ) = {
    val writer = df.write
      .format("hudi")
      .options(hudiOptions)

    val writerWithClustering = clusteringOptions match {
      case Some(options) => writer.options(options)
      case None => writer
    }

    val finalWriter = cleaningOptions match {
      case Some(options) => writerWithClustering.options(options)
      case None => writerWithClustering
    }

    finalWriter
      .mode(saveMode)
//      .save()
      .save(s"$writePath")
  }

  def timeTravelQuery(spark: SparkSession, writePath: String, asOfInstant: String) = {
    val timeTravelDF = spark.read.format("hudi")
      .option("as.of.instant", asOfInstant)
      .load(s"$writePath")

    timeTravelDF.show()
  }
}
