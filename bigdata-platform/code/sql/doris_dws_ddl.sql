-- =================================================================
-- Doris DWS 汇总层 建表语句
-- =================================================================

CREATE DATABASE IF NOT EXISTS dws;

-- DWS 门店日汇总
CREATE TABLE IF NOT EXISTS dws.dws_trade_store_1d (
    store_id        VARCHAR(32) NOT NULL COMMENT '门店ID',
    store_name      VARCHAR(128) COMMENT '门店名称',
    city            VARCHAR(64) COMMENT '城市',
    province        VARCHAR(64) COMMENT '省份',
    store_type      VARCHAR(32) COMMENT '门店类型',
    order_cnt       BIGINT COMMENT '订单数',
    pay_user_cnt    BIGINT COMMENT '支付用户数',
    total_amount    DECIMAL(18,2) COMMENT '总金额',
    pay_amount      DECIMAL(18,2) COMMENT '实付金额',
    refund_cnt      BIGINT COMMENT '退款订单数',
    refund_amount   DECIMAL(18,2) COMMENT '退款金额',
    avg_order_amount DECIMAL(10,2) COMMENT '客单价',
    dt              DATE NOT NULL COMMENT '统计日期'
) ENGINE=OLAP
AGGREGATE KEY(store_id, store_name, city, province, store_type,
              order_cnt, pay_user_cnt, total_amount, pay_amount,
              refund_cnt, refund_amount, avg_order_amount, dt)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(store_id) BUCKETS 4
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "replication_num" = "1"
);

-- DWS 用户行为日汇总
CREATE TABLE IF NOT EXISTS dws.dws_user_behavior_1d (
    user_id         VARCHAR(32) NOT NULL COMMENT '用户ID',
    pv              BIGINT COMMENT '浏览量',
    uv_flag         TINYINT COMMENT '是否活跃 (1=是)',
    click_cnt       BIGINT COMMENT '点击次数',
    cart_cnt        BIGINT COMMENT '加购次数',
    order_cnt       BIGINT COMMENT '下单次数',
    pay_cnt         BIGINT COMMENT '支付次数',
    search_cnt      BIGINT COMMENT '搜索次数',
    session_cnt     BIGINT COMMENT '会话数',
    avg_session_dur DECIMAL(10,2) COMMENT '平均会话时长(秒)',
    dt              DATE NOT NULL COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(user_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "replication_num" = "1"
);

-- DWS 用户价值日汇总
CREATE TABLE IF NOT EXISTS dws.dws_user_value_1d (
    user_id         VARCHAR(32) NOT NULL COMMENT '用户ID',
    vip_level       INT COMMENT 'VIP等级',
    city            VARCHAR(64) COMMENT '城市',
    total_orders    BIGINT COMMENT '累计订单数',
    total_amount    DECIMAL(18,2) COMMENT '累计消费金额',
    avg_order_amount DECIMAL(10,2) COMMENT '客单价',
    last_order_time DATETIME COMMENT '最近下单时间',
    recency_days    INT COMMENT '距今天数',
    frequency       INT COMMENT '近30天下单次数',
    monetary        DECIMAL(18,2) COMMENT '近30天消费金额',
    rfm_score       INT COMMENT 'RFM 总分',
    user_tag        VARCHAR(32) COMMENT '用户标签: 高价值/潜力/低活跃/沉默',
    dt              DATE NOT NULL COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(user_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "replication_num" = "1"
);

-- DWS 实时订单监控 (Flink 写入)
CREATE TABLE IF NOT EXISTS dws.dws_realtime_order_monitor_1min (
    window_time     DATETIME NOT NULL COMMENT '窗口结束时间',
    order_cnt       BIGINT COMMENT '窗口内订单数',
    gmv             DECIMAL(18,2) COMMENT '窗口内 GMV',
    refund_rate     DECIMAL(8,4) COMMENT '退款率',
    create_time     DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间'
) ENGINE=OLAP
DUPLICATE KEY(window_time)
PARTITION BY RANGE(window_time) ()
DISTRIBUTED BY HASH(window_time) BUCKETS 2
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "1",
    "dynamic_partition.prefix" = "p",
    "replication_num" = "1"
);

-- DWS 实时 PV/UV 统计 (Flink 写入)
CREATE TABLE IF NOT EXISTS dws.dws_realtime_pv_uv_1min (
    window_time     DATETIME NOT NULL COMMENT '窗口结束时间',
    pv              BIGINT COMMENT '窗口内 PV',
    uv              BIGINT COMMENT '窗口内 UV',
    create_time     DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间'
) ENGINE=OLAP
DUPLICATE KEY(window_time)
PARTITION BY RANGE(window_time) ()
DISTRIBUTED BY HASH(window_time) BUCKETS 2
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "1",
    "dynamic_partition.prefix" = "p",
    "replication_num" = "1"
);
