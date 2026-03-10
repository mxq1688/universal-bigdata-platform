-- =================================================================
-- MySQL 业务库初始化 (Docker 自动执行)
-- =================================================================

CREATE DATABASE IF NOT EXISTS bigdata_source DEFAULT CHARSET utf8mb4;
USE bigdata_source;

-- 用户表
CREATE TABLE IF NOT EXISTS users (
    user_id       VARCHAR(32) PRIMARY KEY COMMENT '用户ID',
    username      VARCHAR(64) NOT NULL COMMENT '用户名',
    gender        TINYINT COMMENT '性别: 0=未知, 1=男, 2=女',
    age           INT COMMENT '年龄',
    city          VARCHAR(64) COMMENT '城市',
    province      VARCHAR(64) COMMENT '省份',
    register_time DATETIME NOT NULL COMMENT '注册时间',
    vip_level     INT DEFAULT 0 COMMENT 'VIP等级',
    phone         VARCHAR(20) COMMENT '手机号',
    create_time   DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_city (city),
    INDEX idx_register_time (register_time)
) ENGINE=InnoDB COMMENT='用户信息表';

-- 商品表
CREATE TABLE IF NOT EXISTS products (
    product_id    VARCHAR(32) PRIMARY KEY COMMENT '商品ID',
    product_name  VARCHAR(256) NOT NULL COMMENT '商品名称',
    category      VARCHAR(64) COMMENT '一级分类',
    sub_category  VARCHAR(64) COMMENT '二级分类',
    brand         VARCHAR(128) COMMENT '品牌',
    price         DECIMAL(10,2) NOT NULL COMMENT '价格',
    cost          DECIMAL(10,2) COMMENT '成本',
    status        VARCHAR(16) DEFAULT 'active' COMMENT '状态: active/inactive',
    create_time   DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (category),
    INDEX idx_brand (brand)
) ENGINE=InnoDB COMMENT='商品信息表';

-- 门店表
CREATE TABLE IF NOT EXISTS stores (
    store_id      VARCHAR(32) PRIMARY KEY COMMENT '门店ID',
    store_name    VARCHAR(128) NOT NULL COMMENT '门店名称',
    city          VARCHAR(64) COMMENT '城市',
    province      VARCHAR(64) COMMENT '省份',
    district      VARCHAR(64) COMMENT '区县',
    address       VARCHAR(512) COMMENT '详细地址',
    store_type    VARCHAR(32) COMMENT '门店类型: flagship/standard/mini',
    open_date     DATE COMMENT '开业日期',
    status        VARCHAR(16) DEFAULT 'open' COMMENT '状态: open/closed',
    create_time   DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_city (city)
) ENGINE=InnoDB COMMENT='门店信息表';

-- 订单表
CREATE TABLE IF NOT EXISTS orders (
    order_id        VARCHAR(32) PRIMARY KEY COMMENT '订单ID',
    user_id         VARCHAR(32) NOT NULL COMMENT '用户ID',
    store_id        VARCHAR(32) COMMENT '门店ID',
    product_id      VARCHAR(32) NOT NULL COMMENT '商品ID',
    quantity        INT NOT NULL DEFAULT 1 COMMENT '数量',
    unit_price      DECIMAL(10,2) NOT NULL COMMENT '单价',
    total_amount    DECIMAL(10,2) NOT NULL COMMENT '总金额',
    discount_amount DECIMAL(10,2) DEFAULT 0 COMMENT '优惠金额',
    pay_amount      DECIMAL(10,2) NOT NULL COMMENT '实付金额',
    status          VARCHAR(16) NOT NULL COMMENT '状态: created/paid/shipped/completed/refunded',
    payment_method  VARCHAR(32) COMMENT '支付方式: alipay/wechat/card/cash',
    city            VARCHAR(64) COMMENT '下单城市',
    platform        VARCHAR(16) COMMENT '平台: app/web/mini_program',
    create_time     DATETIME NOT NULL COMMENT '下单时间',
    pay_time        DATETIME COMMENT '支付时间',
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_store_id (store_id),
    INDEX idx_create_time (create_time),
    INDEX idx_status (status)
) ENGINE=InnoDB COMMENT='订单表';
