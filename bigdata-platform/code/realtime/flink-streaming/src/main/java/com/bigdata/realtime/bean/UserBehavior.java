package com.bigdata.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * 用户行为实体类
 * 对应 Kafka 消息格式
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserBehavior implements Serializable {

    private static final long serialVersionUID = 1L;

    // 事件类型（固定值）
    private String eventType;
    
    // 用户 ID
    private String userId;
    
    // 商品 ID
    private String productId;
    
    // 商品名称
    private String productName;
    
    // 商品分类
    private String category;
    
    // 用户行为类型: view, search, cart, order, pay, refund
    private String action;
    
    // 设备类型: iOS, Android, Web, MiniApp
    private String device;
    
    // 渠道来源: organic, paid, social, direct, email
    private String channel;
    
    // 城市
    private String city;
    
    // 会话 ID
    private String sessionId;
    
    // 时间戳 (毫秒)
    private long timestamp;

    // 正则校验
    private static final Pattern USER_ID_PATTERN = Pattern.compile("user_\\d{5}");
    private static final Pattern PROD_ID_PATTERN = Pattern.compile("prod_\\d{4}");
    private static final Pattern VALID_ACTIONS = Pattern.compile("^(view|search|cart|order|pay|refund)$");

    /**
     * 数据有效性校验
     */
    public boolean isValid() {
        // 1. 基础字段非空校验
        if (userId == null || userId.trim().isEmpty()) return false;
        if (productId == null || productId.trim().isEmpty()) return false;
        if (action == null || action.trim().isEmpty()) return false;

        // 2. 格式校验
        if (!USER_ID_PATTERN.matcher(userId).matches()) {
            System.err.println("用户 ID 格式错误: " + userId);
            return false;
        }

        if (!PROD_ID_PATTERN.matcher(productId).matches()) {
            System.err.println("商品 ID 格式错误: " + productId);
            return false;
        }

        if (!VALID_ACTIONS.matcher(action).matches()) {
            System.err.println("行为类型错误: " + action);
            return false;
        }

        // 3. 时间戳校验 (1 年内有效)
        long currentTime = System.currentTimeMillis();
        if (timestamp <= 0 || timestamp > currentTime || (currentTime - timestamp) > 365 * 24 * 3600 * 1000L) {
            System.err.println("时间戳无效: " + timestamp);
            return false;
        }

        // 4. 其他可选字段校验
        if (device != null && !device.matches("^(iOS|Android|Web|MiniApp)$")) {
            System.err.println("设备类型错误: " + device);
        }

        if (channel != null && !channel.matches("^(organic|paid|social|direct|email)$")) {
            System.err.println("渠道类型错误: " + channel);
        }

        return true;
    }

    /**
     * 获取小时时间戳 (用于窗口对齐)
     */
    public long getHourTimestamp() {
        return timestamp / 3600000 * 3600000;
    }

    /**
     * 获取日期字符串 (yyyyMMdd)
     */
    public String getDateStr() {
        long time = timestamp;
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTimeInMillis(time);
        return String.format("%04d%02d%02d",
                cal.get(java.util.Calendar.YEAR),
                cal.get(java.util.Calendar.MONTH) + 1,
                cal.get(java.util.Calendar.DAY_OF_MONTH));
    }

    /**
     * 获取用户行为权重（用于价值计算）
     */
    public int getActionWeight() {
        switch (action) {
            case "pay":
                return 10;
            case "order":
                return 8;
            case "cart":
                return 3;
            case "search":
                return 2;
            case "view":
            default:
                return 1;
        }
    }
}
