package com.bigdata.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * Hive 工具类
 * 提供 DDL / 分区管理 / 元数据操作
 */
object HiveUtils {

  private val LOG = LoggerFactory.getLogger(getClass)

  /**
   * 创建分区表（如果不存在）
   */
  def createTableIfNotExists(spark: SparkSession, tableName: String, schema: StructType,
                             format: String = "orc", partitionCol: String = "dt"): Unit = {
    val colDefs = schema.fields
      .filter(_.name != partitionCol)
      .map(f => s"${f.name} ${sparkTypeToHive(f.dataType)}")
      .mkString(",\n  ")

    val sql =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |  $colDefs
         |)
         |PARTITIONED BY ($partitionCol STRING)
         |STORED AS $format
         |TBLPROPERTIES ('orc.compress'='SNAPPY')
       """.stripMargin

    spark.sql(sql)
    LOG.info(s"表创建成功 (如不存在): $tableName")
  }

  /**
   * 添加分区
   */
  def addPartition(spark: SparkSession, tableName: String, dt: String,
                   location: Option[String] = None): Unit = {
    val locationClause = location.map(l => s"LOCATION '$l'").getOrElse("")
    spark.sql(s"ALTER TABLE $tableName ADD IF NOT EXISTS PARTITION (dt='$dt') $locationClause")
    LOG.info(s"添加分区: $tableName/dt=$dt")
  }

  /**
   * 删除分区
   */
  def dropPartition(spark: SparkSession, tableName: String, dt: String): Unit = {
    spark.sql(s"ALTER TABLE $tableName DROP IF EXISTS PARTITION (dt='$dt')")
    LOG.info(s"删除分区: $tableName/dt=$dt")
  }

  /**
   * 获取表的最新分区日期
   */
  def getLatestPartition(spark: SparkSession, tableName: String): Option[String] = {
    try {
      val partitions = spark.sql(s"SHOW PARTITIONS $tableName")
        .collect()
        .map(_.getString(0).replace("dt=", ""))
        .sorted
      partitions.lastOption
    } catch {
      case _: Exception => None
    }
  }

  /**
   * 检查分区是否存在
   */
  def partitionExists(spark: SparkSession, tableName: String, dt: String): Boolean = {
    try {
      val count = spark.sql(s"SELECT 1 FROM $tableName WHERE dt='$dt' LIMIT 1").count()
      count > 0
    } catch {
      case _: Exception => false
    }
  }

  /**
   * 获取分区数据量
   */
  def getPartitionCount(spark: SparkSession, tableName: String, dt: String): Long = {
    spark.sql(s"SELECT COUNT(*) FROM $tableName WHERE dt='$dt'").collect()(0).getLong(0)
  }

  /**
   * Spark DataType → Hive 类型映射
   */
  private def sparkTypeToHive(dataType: DataType): String = dataType match {
    case StringType => "STRING"
    case IntegerType => "INT"
    case LongType => "BIGINT"
    case DoubleType => "DOUBLE"
    case FloatType => "FLOAT"
    case BooleanType => "BOOLEAN"
    case TimestampType => "TIMESTAMP"
    case DateType => "DATE"
    case _: DecimalType => "DECIMAL(18,2)"
    case _ => "STRING"
  }
}
