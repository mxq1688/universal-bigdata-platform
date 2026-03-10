package com.bigdata.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/**
 * Spark 工具类
 * 提供常用的数仓 ETL 辅助方法
 */
object SparkUtils {

  private val LOG = LoggerFactory.getLogger(getClass)

  /**
   * 创建 SparkSession（开启 Hive 支持）
   */
  def createSparkSession(appName: String, master: String = ""): SparkSession = {
    val builder = SparkSession.builder()
      .appName(appName)
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()

    if (master.nonEmpty) {
      builder.master(master)
    }

    val spark = builder.getOrCreate()
    LOG.info(s"SparkSession 创建成功: $appName")
    spark
  }

  /**
   * 读取 Hive 分区表
   */
  def readPartition(spark: SparkSession, tableName: String, dt: String): DataFrame = {
    val df = spark.sql(s"SELECT * FROM $tableName WHERE dt = '$dt'")
    val count = df.count()
    LOG.info(s"读取 $tableName (dt=$dt): $count 行")
    df
  }

  /**
   * 覆盖写入 Hive 分区（先删后写，防止重复）
   */
  def overwritePartition(df: DataFrame, tableName: String, dt: String): Unit = {
    val spark = df.sparkSession
    spark.sql(s"ALTER TABLE $tableName DROP IF EXISTS PARTITION (dt='$dt')")

    df.withColumn("dt", lit(dt))
      .write
      .mode("append")
      .insertInto(tableName)

    val count = df.count()
    LOG.info(s"写入 $tableName (dt=$dt): $count 行")
  }

  /**
   * 数据去重（按主键取最新一条）
   */
  def deduplicateByKey(df: DataFrame, keyColumns: Seq[String], orderColumn: String): DataFrame = {
    import df.sparkSession.implicits._
    val windowSpec = org.apache.spark.sql.expressions.Window
      .partitionBy(keyColumns.map(col): _*)
      .orderBy(col(orderColumn).desc)

    df.withColumn("_rn", row_number().over(windowSpec))
      .filter(col("_rn") === 1)
      .drop("_rn")
  }

  /**
   * 空值填充（数值型填 0，字符串型填空串）
   */
  def fillNulls(df: DataFrame): DataFrame = {
    val numericCols = df.schema.fields
      .filter(f => f.dataType.typeName.startsWith("int") ||
        f.dataType.typeName.startsWith("long") ||
        f.dataType.typeName.startsWith("double") ||
        f.dataType.typeName.startsWith("float") ||
        f.dataType.typeName.startsWith("decimal"))
      .map(_.name)

    val stringCols = df.schema.fields
      .filter(_.dataType.typeName == "string")
      .map(_.name)

    var result = df
    if (numericCols.nonEmpty) {
      result = result.na.fill(0, numericCols)
    }
    if (stringCols.nonEmpty) {
      result = result.na.fill("", stringCols)
    }
    result
  }

  /**
   * 数据质量快速检查（返回各列空值数和唯一值数）
   */
  def quickProfile(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val total = df.count()
    val profileData = df.columns.map { colName =>
      val nullCount = df.filter(col(colName).isNull || col(colName) === "").count()
      val distinctCount = df.select(colName).distinct().count()
      (colName, total, nullCount, distinctCount,
        f"${nullCount.toDouble / total * 100}%.2f%%")
    }

    profileData.toSeq.toDF("column_name", "total_rows", "null_count", "distinct_count", "null_rate")
  }

  /**
   * 打印 DataFrame 概要
   */
  def printSummary(df: DataFrame, name: String): Unit = {
    val count = df.count()
    val colCount = df.columns.length
    LOG.info(s"[$name] 行数=$count, 列数=$colCount, 列=${df.columns.mkString(",")}")
    df.printSchema()
    df.show(5, truncate = false)
  }
}
