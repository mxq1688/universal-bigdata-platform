-- =================================================================
-- Doris DWD 明细层 建表语句
-- =================================================================

CREATE DATABASE IF NOT EXISTS dwd;

-- DWD 交易订单明细
CREATE TABLE IF NOT EXISTS dwd.dwd_trade_order_detail (
    order_id        VARCHAR(32) NOT NULL COMMENT '订单ID',
    user_id         VARCHAR(32) NOT NULL COMMENT '用户ID',
    store_id        VARCHAR(32) COMMENT '门店ID',
    product_id      VARCHAR(32) COMMENT '商品ID',
    product_name    VARCHAR(256) COMMENT '商品名称',
    category        VARCHAR(64) COMMENT '商品分类',
    brand           VARCHAR(128) COMMENT '品牌',
    quantity        INT COMMENT '数量',
    unit_price      DECIMAL(10,2) COMMENT '单价',
    total_amount    DECIMAL(10,2) COMMENT '总金额',
    discount_amount DECIMAL(10,2) COMMENT '优惠金额',
    pay_amount      DECIMAL(10,2) COMMENT '实付金额',
    status          VARCHAR(16) COMMENT '订单状态',
    payment_method  VARCHAR(32) COMMENT '支付方式',
    city            VARCHAR(64) COMMENT '下单城市',
    province        VARCHAR(64) COMMENT '省份',
    platform        VARCHAR(16) COMMENT '下单平台',
    user_vip_level  INT COMMENT '用户VIP等级',
    store_type      VARCHAR(32) COMMENT '门店类型',
    create_time     DATETIME COMMENT '下单时间',
    pay_time        DATETIME COMMENT '支付时间',
    dt              DATE NOT NULL COMMENT '分区日期'
) ENGINE=OLAP
DUPLICATE KEY(order_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-60",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "replication_num" = "1"
);

-- DWD 用户行为明细
CREATE TABLE IF NOT EXISTS dwd.dwd_user_behavior_detail (
    user_id       VARCHAR(32) NOT NULL COMMENT '用户ID',
    action        VARCHAR(16) NOT NULL COMMENT '行为类型: view/click/cart/order/pay',
    product_id    VARCHAR(32) COMMENT '商品ID',
    product_name  VARCHAR(256) COMMENT '商品名称',
    category      VARCHAR(64) COMMENT '商品分类',
    device        VARCHAR(32) COMMENT '设备类型',
    city          VARCHAR(64) COMMENT '城市',
    session_id    VARCHAR(64) COMMENT '会话ID',
    event_time    DATETIME COMMENT '事件时间',
    dt            DATE NOT NULL COMMENT '分区日期'
) ENGINE=OLAP
DUPLICATE KEY(user_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-60",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "replication_num" = "1"
);
