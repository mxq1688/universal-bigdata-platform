package com.bigdata.realtime.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * JSON 工具类
 */
public class JsonUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);
    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = JsonMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
                .configure(JsonReadFeature.ALLOW_SINGLE_QUOTES, true)
                .configure(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES, true)
                .build();

        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * 对象转 JSON
     */
    public static <T> String toJson(T obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            LOG.error("对象转 JSON 失败: {}", obj, e);
            return null;
        }
    }

    /**
     * JSON 转对象
     */
    public static <T> T parseJson(String json, Class<T> clazz) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            LOG.error("JSON 转对象失败: {}", json, e);
            return null;
        }
    }

    /**
     * JSON 转对象列表
     */
    public static <T> List<T> parseJsonList(String json, Class<T> clazz) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(json,
                    OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, clazz));
        } catch (JsonProcessingException e) {
            LOG.error("JSON 转列表失败: {}", json, e);
            return null;
        }
    }

    /**
     * 格式化 JSON 字符串（美化输出）
     */
    public static String formatJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            Object obj = OBJECT_MAPPER.readValue(json, Object.class);
            return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            LOG.error("JSON 格式化失败: {}", json, e);
            return json;
        }
    }

    /**
     * 深拷贝对象
     */
    @SuppressWarnings("unchecked")
    public static <T> T deepCopy(T obj) {
        if (obj == null) {
            return null;
        }
        try {
            return (T) OBJECT_MAPPER.readValue(toJson(obj), obj.getClass());
        } catch (Exception e) {
            LOG.error("对象深拷贝失败: {}", obj, e);
            return null;
        }
    }

    /**
     * 安全解析 JSON (不抛异常)
     */
    public static <T> T safeParseJson(String json, Class<T> clazz) {
        try {
            return parseJson(json, clazz);
        } catch (Exception e) {
            LOG.warn("安全解析 JSON 失败: {}", json != null ? json.substring(0, Math.min(json.length(), 50)) : "null");
            return null;
        }
    }
}
