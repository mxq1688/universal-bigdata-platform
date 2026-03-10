-- =================================================================
-- Doris ADS 报表层 建表语句
-- =================================================================

CREATE DATABASE IF NOT EXISTS ads;

-- ADS 每日经营概览
CREATE TABLE IF NOT EXISTS ads.ads_daily_overview (
    dt                    DATE NOT NULL COMMENT '统计日期',
    total_orders          BIGINT COMMENT '订单总数',
    total_gmv             DECIMAL(18,2) COMMENT 'GMV (总交易额)',
    total_pay_amount      DECIMAL(18,2) COMMENT '实付总额',
    avg_order_amount      DECIMAL(10,2) COMMENT '客单价',
    pay_user_cnt          BIGINT COMMENT '支付用户数',
    new_user_cnt          BIGINT COMMENT '新注册用户数',
    refund_cnt            BIGINT COMMENT '退款订单数',
    refund_rate           DECIMAL(8,4) COMMENT '退款率',
    total_discount        DECIMAL(18,2) COMMENT '优惠总额',
    active_store_cnt      BIGINT COMMENT '活跃门店数',
    gmv_vs_yesterday      DECIMAL(8,4) COMMENT 'GMV 环比 (vs 昨天)',
    orders_vs_yesterday   DECIMAL(8,4) COMMENT '订单数环比'
) ENGINE=OLAP
DUPLICATE KEY(dt)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- ADS 日活统计
CREATE TABLE IF NOT EXISTS ads.ads_dau (
    dt              DATE NOT NULL COMMENT '统计日期',
    dau             BIGINT COMMENT '日活跃用户数 (DAU)',
    new_users       BIGINT COMMENT '新用户数',
    return_users    BIGINT COMMENT '回访用户数',
    avg_session_cnt DECIMAL(10,2) COMMENT '人均会话数',
    avg_pv          DECIMAL(10,2) COMMENT '人均浏览量',
    bounce_rate     DECIMAL(8,4) COMMENT '跳出率',
    avg_duration    DECIMAL(10,2) COMMENT '平均停留时长(秒)'
) ENGINE=OLAP
DUPLICATE KEY(dt)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- ADS 城市排行榜
CREATE TABLE IF NOT EXISTS ads.ads_city_rank (
    dt              DATE NOT NULL COMMENT '统计日期',
    city            VARCHAR(64) NOT NULL COMMENT '城市',
    province        VARCHAR(64) COMMENT '省份',
    order_cnt       BIGINT COMMENT '订单数',
    gmv             DECIMAL(18,2) COMMENT 'GMV',
    pay_user_cnt    BIGINT COMMENT '支付用户数',
    avg_order_amount DECIMAL(10,2) COMMENT '客单价',
    rank_by_gmv     INT COMMENT 'GMV排名'
) ENGINE=OLAP
DUPLICATE KEY(dt, city)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- ADS 用户价值分层
CREATE TABLE IF NOT EXISTS ads.ads_user_value_dist (
    dt              DATE NOT NULL COMMENT '统计日期',
    user_tag        VARCHAR(32) NOT NULL COMMENT '用户标签',
    user_cnt        BIGINT COMMENT '用户数',
    user_pct        DECIMAL(8,4) COMMENT '占比',
    avg_amount      DECIMAL(10,2) COMMENT '平均消费金额',
    avg_orders      DECIMAL(10,2) COMMENT '平均订单数',
    total_gmv       DECIMAL(18,2) COMMENT '该分层总GMV'
) ENGINE=OLAP
DUPLICATE KEY(dt, user_tag)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- ADS 品类转化漏斗
CREATE TABLE IF NOT EXISTS ads.ads_category_funnel (
    dt              DATE NOT NULL COMMENT '统计日期',
    category        VARCHAR(64) NOT NULL COMMENT '品类',
    view_cnt        BIGINT COMMENT '浏览人数',
    click_cnt       BIGINT COMMENT '点击人数',
    cart_cnt        BIGINT COMMENT '加购人数',
    order_cnt       BIGINT COMMENT '下单人数',
    pay_cnt         BIGINT COMMENT '支付人数',
    view_to_click   DECIMAL(8,4) COMMENT '浏览→点击转化率',
    click_to_cart   DECIMAL(8,4) COMMENT '点击→加购转化率',
    cart_to_order   DECIMAL(8,4) COMMENT '加购→下单转化率',
    order_to_pay    DECIMAL(8,4) COMMENT '下单→支付转化率',
    overall_rate    DECIMAL(8,4) COMMENT '整体转化率 (浏览→支付)'
) ENGINE=OLAP
DUPLICATE KEY(dt, category)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- ADS 商品 Top 排行
CREATE TABLE IF NOT EXISTS ads.ads_product_top (
    dt              DATE NOT NULL COMMENT '统计日期',
    product_id      VARCHAR(32) NOT NULL COMMENT '商品ID',
    product_name    VARCHAR(256) COMMENT '商品名称',
    category        VARCHAR(64) COMMENT '品类',
    brand           VARCHAR(128) COMMENT '品牌',
    order_cnt       BIGINT COMMENT '订单数',
    gmv             DECIMAL(18,2) COMMENT 'GMV',
    pay_user_cnt    BIGINT COMMENT '购买用户数',
    rank_by_gmv     INT COMMENT 'GMV 排名'
) ENGINE=OLAP
DUPLICATE KEY(dt, product_id)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- ADS 支付方式分布
CREATE TABLE IF NOT EXISTS ads.ads_payment_dist (
    dt                DATE NOT NULL COMMENT '统计日期',
    payment_method    VARCHAR(32) NOT NULL COMMENT '支付方式',
    order_cnt         BIGINT COMMENT '订单数',
    pay_amount        DECIMAL(18,2) COMMENT '支付金额',
    user_cnt          BIGINT COMMENT '用户数',
    order_pct         DECIMAL(8,4) COMMENT '订单占比',
    amount_pct        DECIMAL(8,4) COMMENT '金额占比'
) ENGINE=OLAP
DUPLICATE KEY(dt, payment_method)
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ("replication_num" = "1");
