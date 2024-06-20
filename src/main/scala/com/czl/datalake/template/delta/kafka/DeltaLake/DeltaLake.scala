package main.scala.com.czl.datalake.template.delta.kafka.DeltaLake

import io.delta.tables._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}

object DeltaLake {

  def main(args: Array[String]): Unit = {

//    val spark = SparkSession.builder()
//      .appName("DeltaLakeExample")
////      .master("local[*]")
//      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
//      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
//      .getOrCreate()
//
//    // Create a sample DataFrame
//    import spark.implicits._
//    val sampleData = Seq(
//      ("1", "2023-06-01 00:00:00", "John", "Doe"),
//      ("2", "2023-06-01 01:00:00", "Jane", "Smith"),
//      ("3", "2023-06-01 02:00:00", "Sam", "Johnson")
//    ).toDF("id", "timestamp", "first_name", "last_name")
//
//    // Write the sample data to Delta Lake
//    sampleData.write
//      .format("delta")
//      .mode(SaveMode.Overwrite)
//      .save("s3://bidgely-adhoc-dev/dhruv/delta/sample_table")
//
//    // Read the Delta table
//    val deltaTable = spark.read.format("delta").load("s3://bidgely-adhoc-dev/dhruv/delta/sample_table")
//    deltaTable.show()
//
//    // New data for upsert
//    val newData = Seq(
//      ("1", "2023-06-01 00:00:01", "John", "DoeUpdated"),
//      ("2", "2023-06-01 01:00:00", "Jane", "Smith"),
//      ("4", "2023-06-01 03:00:00", "Alice", "Williams")
//    ).toDF("id", "timestamp", "first_name", "last_name")
//
//    // Perform upsert (merge) operation
//    // https://docs.delta.io/latest/delta-update.html#slowly-changing-data-scd-type-2-operation-into-delta-tables
//    val deltaTableInstance = DeltaTable.forPath("s3://bidgely-adhoc-dev/dhruv/delta/sample_table")
//
//    deltaTableInstance
//      .as("t")
//      .merge(
//        newData.as("s"),
//        "t.id = s.id"
//      )
//      .whenMatched("t.timestamp < s.timestamp")
//      .updateExpr(
//        Map(
//          "timestamp" -> "s.timestamp",
//          "first_name" -> "s.first_name",
//          "last_name" -> "s.last_name"
//        )
//      )
//      .whenNotMatched()
//      .insertAll()
//      .execute()
//
//    val mergeTable = spark.read.format("delta").load("s3://bidgely-adhoc-dev/dhruv/delta/sample_table")
//    mergeTable.show()
//
//    // Compaction
//    val deltaTableToCompact = DeltaTable.forPath(spark, "s3://bidgely-adhoc-dev/dhruv/delta/sample_table")
//    deltaTableToCompact.optimize().executeCompaction()
//
//    // Verify the final table after compaction
//    val finalTable = spark.read.format("delta").load("s3://bidgely-adhoc-dev/dhruv/delta/sample_table")
//    finalTable.show()
//
//    // Z-ORDER clustering
//    val deltaTableForClustering = DeltaTable.forPath(spark, "s3://bidgely-adhoc-dev/dhruv/delta/sample_table")
//    deltaTableForClustering.optimize().executeZOrderBy("timestamp", "id") // example
//
//    // Time travel
//    val dfVersion = spark.read.format("delta").option("versionAsOf", 0).load("s3://bidgely-adhoc-dev/dhruv/delta/sample_table")
//    dfVersion.show()
//
//    //    // Query by timestamp (this thing requires exact timestamp)
//    //    val dfTimestamp = spark.read.format("delta").option("timestampAsOf", "2024-06-12 00:00:00").load("/tmp/delta/sample_table")
//    //    dfTimestamp.show()
//
//    // Cleaning / VACUUM
//    deltaTableToCompact.vacuum()
//
//    // VACUUM with a custom retention period (e.g., 1 hour)
//    deltaTableToCompact.vacuum(3600) // 3600 seconds = 1 hour
//
//    spark.stop()


    val spark = SparkSession.builder()
      .appName("DeltaLakeExample")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._

    // Function to extract year and month from event_date
    val extractYearMonth = udf((eventDate: String) => eventDate.substring(0, 7))

    // Load data from S3, filter by year, and extract year and month for partitioning
    val data2020_2021 = spark.read.format("parquet")
      .load("s3://bidgely-adhoc-dev/dhruv/read/event_month=2020-*",
        "s3://bidgely-adhoc-dev/dhruv/read/event_month=2021-*")
      .withColumn("event_date", extractYearMonth(col("event_date")))

    val data2022 = spark.read.format("parquet")
      .load("s3://bidgely-adhoc-dev/dhruv/read/event_month=2022-*")
      .withColumn("event_date", extractYearMonth(col("event_date")))

    // Write the data from 2020 and 2021 to Delta Lake
    data2020_2021.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .partitionBy("event_date")
      .option("overwriteSchema", "true")
      .save("s3://bidgely-adhoc-dev/dhruv/delta/write")

    // Read the Delta table
    val deltaTable = spark.read.format("delta").load("s3://bidgely-adhoc-dev/dhruv/delta/write")
    deltaTable.show()

    // Perform upsert (merge) operation with 2022 data
    val deltaTableInstance = DeltaTable.forPath("s3://bidgely-adhoc-dev/dhruv/delta/write")

    // Preprocess data2022 to remove duplicates
    val windowSpec = Window.partitionBy("uuid").orderBy(col("last_updated_timestamp").desc)
    val deduplicatedData2022 = data2022.withColumn("rank", row_number.over(windowSpec))
      .filter(col("rank") === 1)
      .drop("rank")

    deltaTableInstance
      .as("t")
      .merge(
        deduplicatedData2022.as("s"),
        "t.uuid = s.uuid"
      )
      .whenMatched("t.last_updated_timestamp < s.last_updated_timestamp")
      .updateExpr(
        Map(
          "last_updated_timestamp" -> "s.last_updated_timestamp",
          "event_id" -> "s.event_id",
          "source" -> "s.source",
          "version" -> "s.version",
          "uuid" -> "s.uuid",
          "last_updated_timestamp" -> "s.last_updated_timestamp",
          "dashboardLoaded" -> "s.dashboardLoaded",
          "webDataFlowTimeStamp" -> "s.webDataFlowTimeStamp",
          "mobileDataFlowTimeStamp" -> "s.mobileDataFlowTimeStamp",
          "locality" -> "s.locality",
          "city" -> "s.city",
          "state" -> "s.state",
          "timezone" -> "s.timezone",
          "zip" -> "s.zip",
          "country" -> "s.country",
          "regionCode" -> "s.regionCode",
          "ratesSchedule" -> "s.ratesSchedule",
          "addressType" -> "s.addressType",
          "partnerUserHomeId" -> "s.partnerUserHomeId",
          "livingArea" -> "s.livingArea",
          "lastProfileTime" -> "s.lastProfileTime",
    "meterId" -> "s.meterId",
    "defNTypeId" -> "s.defNTypeId",
    "defNhoodId" -> "s.defNhoodId",
    "nhood" -> "s.nhood",
    "supplementaryNhoodIds" -> "s.supplementaryNhoodIds",
    "utilityUserName" -> "s.utilityUserName",
    "lseId" -> "s.lseId",
    "billstart" -> "s.billstart",
    "lastRawDataArchivedTS" -> "s.lastRawDataArchivedTS",
    "dwelling" -> "s.dwelling",
    "yearBuilt" -> "s.yearBuilt",
    "numberOfAdults" -> "s.numberOfAdults",
    "solarUser" -> "s.solarUser",
    "adrUser" -> "s.adrUser",
    "showDevicePairing" -> "s.showDevicePairing",
    "numberOfChildren" -> "s.numberOfChildren",
    "enrollmentTime" -> "s.enrollmentTime",
    "lotSize" -> "s.lotSize",
    "bathrooms" -> "s.bathrooms",
    "bedrooms" -> "s.bedrooms",
    "floors" -> "s.floors",
    "totalRooms" -> "s.totalRooms",
    "occupantType" -> "s.occupantType",
    "numOccupants" -> "s.numOccupants",
    "ownershipType" -> "s.ownershipType",
    "spaceHeatingType" -> "s.spaceHeatingType",
    "waterHeatingType" -> "s.waterHeatingType",
    "qualifiedForSurvey" -> "s.qualifiedForSurvey",
    "sentSurvey1" -> "s.sentSurvey1",
    "sentMissingOutHan" -> "s.sentMissingOutHan",
    "sentMissingData" -> "s.sentMissingData",
    "sentSurvey2" -> "s.sentSurvey2",
    "sentSurvey3" -> "s.sentSurvey3",
    "serial" -> "s.serial",
    "thermostats" -> "s.thermostats",
    "lastMonthBill" -> "s.lastMonthBill",
    "lastMonthKwh" -> "s.lastMonthKwh",
    "firstDataProcessTime" -> "s.firstDataProcessTime",
    "plannumber" -> "s.plannumber",
    "utilityunknown" -> "s.utilityunknown",
    "utilityName" -> "s.utilityName",
    "planunknown" -> "s.planunknown",
    "ratePlanId" -> "s.ratePlanId",
    "billCycleCode" -> "s.billCycleCode",
    "rateType" -> "s.rateType",
    "defaultRate" -> "s.defaultRate",
    "rateQuery" -> "s.rateQuery",
    "masterTariffId" -> "s.masterTariffId",
    "emailsBlocked" -> "s.emailsBlocked",
    "notificationsBlocked" -> "s.notificationsBlocked",
    "dailyTierChangeAlertList" -> "s.dailyTierChangeAlertList",
    "weeklySummaryList" -> "s.weeklySummaryList",
    "monthlySummaryList" -> "s.monthlySummaryList",
    "disconnectsSentPerWeek" -> "s.disconnectsSentPerWeek",
    "dailyTierChangeAlert" -> "s.dailyTierChangeAlert",
    "dailyUsageUpAlert" -> "s.dailyUsageUpAlert",
    "dailyUsageDownAlert" -> "s.dailyUsageDownAlert",
    "dailyPeakToUUsageUpAlert" -> "s.dailyPeakToUUsageUpAlert",
    "dailyUsageUpAlertList" -> "s.dailyUsageUpAlertList",
    "dailyUsageUpAlertPercent" -> "s.dailyUsageUpAlertPercent",
    "dailyUsageDownAlertList" -> "s.dailyUsageDownAlertList",
    "dailyUsageDownAlertPercent" -> "s.dailyUsageDownAlertPercent",
    "dailyPeakToUUsageUpAlertPercent" -> "s.dailyPeakToUUsageUpAlertPercent",
    "dailyPeakToUUsageUpAlertList" -> "s.dailyPeakToUUsageUpAlertList",
    "notifyEmailStatusChange" -> "s.notifyEmailStatusChange",
    "notifyEmailState" -> "s.notifyEmailState",
    "notifyEmailGBState" -> "s.notifyEmailGBState",
    "notifyEmailWeeklyStatus" -> "s.notifyEmailWeeklyStatus",
    "notifyEmailMonthlyStatus" -> "s.notifyEmailMonthlyStatus",
    "firstTimeHanConnectEmailed" -> "s.firstTimeHanConnectEmailed",
    "firstTimeGBConnectEmailed" -> "s.firstTimeGBConnectEmailed",
    "firstTimeGBConnectNotified" -> "s.firstTimeGBConnectNotified",
    "blackList" -> "s.blackList",
    "releaseList" -> "s.releaseList",
    "nsp" -> "s.nsp",
    "nst" -> "s.nst",
    "checkedDILSO" -> "s.checkedDILSO",
    "latitude" -> "s.latitude",
    "longitude" -> "s.longitude",
    "budgetThresholdAmount" -> "s.budgetThresholdAmount",
    "s3FileDumpState" -> "s.s3FileDumpState",
    "lastStreamedBillProjection" -> "s.lastStreamedBillProjection",
    "businessName" -> "s.businessName",
    "userSegment" -> "s.userSegment",
    "userBusinessType" -> "s.userBusinessType",
    "partnerProvidedUserBusinessType" -> "s.partnerProvidedUserBusinessType",
    "userBusinessSubType" -> "s.userBusinessSubType",
    "businessTimings" -> "s.businessTimings",
    "smartBudgetEnabled" -> "s.smartBudgetEnabled",
    "businessCode" -> "s.businessCode",
    "addressMatchingThirdPartyData" -> "s.addressMatchingThirdPartyData",
    "billingSchedule" -> "s.billingSchedule",
    "status" -> "s.status",
    "crud_op" -> "s.crud_op",
    "event_date" -> "s.event_date"
        )
      )
      .whenNotMatched()
      .insertAll()
      .execute()

    val mergeTable = spark.read.format("delta").load("s3://bidgely-adhoc-dev/dhruv/delta/write")
    mergeTable.show()

    // Compaction
    val deltaTableToCompact = DeltaTable.forPath(spark, "s3://bidgely-adhoc-dev/dhruv/delta/write")
    deltaTableToCompact.optimize().executeCompaction()

    // Verify the final table after compaction
    val finalTable = spark.read.format("delta").load("s3://bidgely-adhoc-dev/dhruv/delta/write")
    finalTable.show()

    // Z-ORDER clustering
    val deltaTableForClustering = DeltaTable.forPath(spark, "s3://bidgely-adhoc-dev/dhruv/delta/write")
    deltaTableForClustering.optimize().executeZOrderBy("last_updated_timestamp", "uuid") // example

    // Time travel
    val dfVersion = spark.read.format("delta").option("versionAsOf", 0).load("s3://bidgely-adhoc-dev/dhruv/delta/write")
    dfVersion.show()

    // Cleaning / VACUUM
    deltaTableToCompact.vacuum()

    // VACUUM with a custom retention period (e.g., 1 hour)
    deltaTableToCompact.vacuum(3600) // 3600 seconds = 1 hour

    spark.stop()

  }
}
