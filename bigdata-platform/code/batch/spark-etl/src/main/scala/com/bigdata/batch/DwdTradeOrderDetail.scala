package com.bigdata.batch

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * DWD 层: 交易订单明细清洗
 * ODS → DWD 加工流程:
 * 1. 去重、过滤脏数据
 * 2. 关联用户、商品维度表
 * 3. 标准化字段
 * 4. 计算衍生字段
 */
object DwdTradeOrderDetail {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: DwdTradeOrderDetail <date> [env]")
      println("Example: DwdTradeOrderDetail 20260101 dev")
      System.exit(1)
    }

    val runDate = args(0)
    val env = if (args.length >= 2) args(1) else "dev"

    // ====================== 1. 初始化 SparkSession ======================
    val spark = SparkSession.builder()
      .appName(s"DwdTradeOrderDetail_$runDate")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    try {
      // ====================== 2. 读取 ODS 层数据 ======================
      println("[INFO] 读取 ODS 层订单数据...")
      val odsOrderDF = spark.sql(
        s"""
          |SELECT 
          |  order_id,
          |  user_id,
          |  store_id,
          |  total_amount,
          |  status,
          |  payment_method,
          |  city,
          |  device,
          |  create_time,
          |  update_time
          |FROM ods_mysql.ods_mysql_orders
          |WHERE dt = '$runDate'
        """.stripMargin
      )

      val odsOrderItemDF = spark.sql(
        s"""
          |SELECT 
          |  order_item_id,
          |  order_id,
          |  product_id,
          |  quantity,
          |  price,
          |  create_time
          |FROM ods_mysql.ods_mysql_order_items
          |WHERE dt = '$runDate'
        """.stripMargin
      )

      val odsUserDF = spark.sql(
        s"""
          |SELECT 
          |  user_id,
          |  user_name,
          |  phone,
          |  gender,
          |  age,
          |  register_time,
          |  register_channel
          |FROM ods_mysql.ods_mysql_users
          |WHERE dt = '$runDate'
        """.stripMargin
      )

      val odsProductDF = spark.sql(
        s"""
          |SELECT 
          |  product_id,
          |  product_name,
          |  category,
          |  brand,
          |  supplier_id,
          |  cost_price,
          |  market_price,
          |  status as product_status
          |FROM ods_mysql.ods_mysql_products
          |WHERE dt = '$runDate'
        """.stripMargin
      )

      // ====================== 3. 数据清洗与过滤 ======================
      println("[INFO] 数据清洗与过滤...")
      
      // 订单表去重与过滤
      val cleanedOrderDF = odsOrderDF
        .filter(col("order_id").isNotNull && col("order_id").rlike("^ord_\\d+"))
        .filter(col("user_id").isNotNull)
        .filter(col("total_amount").>=(0))
        .dropDuplicates("order_id")

      // 订单项过滤
      val cleanedItemDF = odsOrderItemDF
        .filter(col("order_item_id").isNotNull)
        .filter(col("order_id").isNotNull)
        .filter(col("product_id").isNotNull)
        .filter(col("quantity").>(0))
        .filter(col("price").>=(0))

      // ====================== 4. 维度关联 ======================
      println("[INFO] 关联维度表...")
      
      // 订单 + 订单项
      val orderWithItemDF = cleanedOrderDF
        .join(cleanedItemDF, Seq("order_id"), "inner")
        .drop(cleanedItemDF("create_time"))

      // + 用户维度
      val orderWithUserDF = orderWithItemDF
        .join(odsUserDF, Seq("user_id"), "left_outer")

      // + 商品维度
      val finalDF = orderWithUserDF
        .join(odsProductDF, Seq("product_id"), "left_outer")

      // ====================== 5. 衍生字段计算 ======================
      println("[INFO] 计算衍生字段...")
      
      val resultDF = finalDF
        // 计算商品总价
        .withColumn("item_total_amount", round(col("quantity") * col("price"), 2))
        // 计算商品利润
        .withColumn("item_profit", round(col("quantity") * (col("price") - col("cost_price")), 2))
        // 判断是否为新用户（7天内注册）
        .withColumn("is_new_user", 
          datediff(to_date(col("create_time")), to_date(col("register_time"))) <= 7
        )
        // 订单时长（秒）
        .withColumn("order_duration_second", 
          when(col("status") === "completed", 
            unix_timestamp(col("update_time")) - unix_timestamp(col("create_time")))
          .otherwise(lit(null))
        )
        // 时间分区字段
        .withColumn("dt", lit(runDate))
        .withColumn("hour", hour(col("create_time")))
        // 数据来源标识
        .withColumn("data_source", lit("mysql"))
        // 处理时间
        .withColumn("etl_time", current_timestamp())

      // ====================== 6. 字段选择与重命名 ======================
      val selectDF = resultDF
        .select(
          // 主键
          col("order_id").alias("order_sk"),
          col("order_item_id").alias("order_item_sk"),
          
          // 订单基本信息
          col("user_id"),
          col("store_id"),
          col("total_amount"),
          col("status").alias("order_status"),
          col("payment_method"),
          col("city"),
          col("device"),
          
          // 商品信息
          col("product_id"),
          col("product_name"),
          col("category"),
          col("brand"),
          col("quantity"),
          col("price"),
          col("item_total_amount"),
          col("item_profit"),
          
          // 用户信息
          col("user_name"),
          col("gender"),
          col("age"),
          col("register_channel"),
          col("is_new_user"),
          
          // 时间信息
          col("create_time").alias("order_create_time"),
          col("update_time").alias("order_update_time"),
          col("order_duration_second"),
          
          // 元数据
          col("dt"),
          col("hour"),
          col("data_source"),
          col("etl_time")
        )

      // ====================== 7. 输出到 DWD 层 ======================
      println("[INFO] 输出到 DWD 层...")
      
      val outputPath = s"/warehouse/dwd/dwd_trade_order_detail/dt=$runDate"
      val tableName = "dwd.dwd_trade_order_detail"

      // 写入 HDFS
      selectDF.write
        .mode(SaveMode.Overwrite)
        .option("path", outputPath)
        .partitionBy("dt", "hour")
        .saveAsTable(tableName)

      // 打印统计信息
      println(s"[INFO] 处理完成! 写入记录数: ${selectDF.count()}")
      println(s"[INFO] 输出路径: $outputPath")
      println(s"[INFO] Hive 表: $tableName")

    } finally {
      spark.stop()
    }
  }

  /**
   * 数据质量校验
   */
  def validateData(spark: SparkSession, runDate: String): Unit = {
    // 校验 ODS 层数据是否存在
    val odsCount = spark.sql(s"SELECT COUNT(*) FROM ods_mysql.ods_mysql_orders WHERE dt = '$runDate'")
      .head()
      .getLong(0)

    if (odsCount == 0) {
      println(s"[ERROR] ODS 层订单数据为空! date: $runDate")
      System.exit(1)
    }

    // 校验数据格式
    val invalidData = spark.sql(
      s"""
        |SELECT COUNT(*) 
        |FROM ods_mysql.ods_mysql_orders 
        |WHERE dt = '$runDate'
        |  AND (order_id IS NULL OR order_id = '')
        |  OR (user_id IS NULL OR user_id = '')
        |  OR total_amount < 0
      """.stripMargin
    ).head().getLong(0)

    if (invalidData > 0) {
      println(s"[WARN] 发现脏数据: $invalidData 条")
    }
  }
}
