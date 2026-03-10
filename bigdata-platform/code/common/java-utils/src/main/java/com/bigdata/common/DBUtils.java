package com.bigdata.common;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

/**
 * 数据库连接池工具类
 * 使用 HikariCP 高性能连接池
 */
public class DBUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DBUtils.class);

    // 单例连接池
    private static volatile HikariDataSource dataSource;

    /**
     * 获取连接池（单例）
     */
    public static HikariDataSource getDataSource(String jdbcUrl, String username, String password) {
        if (dataSource == null) {
            synchronized (DBUtils.class) {
                if (dataSource == null) {
                    dataSource = createDataSource(jdbcUrl, username, password);
                }
            }
        }
        return dataSource;
    }

    /**
     * 创建连接池
     */
    public static HikariDataSource createDataSource(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();

        // 基础配置
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);

        // 连接池配置
        config.setMinimumIdle(5);  // 最小空闲连接
        config.setMaximumPoolSize(20); // 最大连接数
        config.setConnectionTimeout(30000); // 连接超时 30s
        config.setIdleTimeout(600000); // 空闲连接超时 10分钟
        config.setMaxLifetime(1800000); // 连接最大生命周期 30分钟
        config.setAutoCommit(true); // 默认自动提交

        // 性能优化
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("useLocalSessionState", "true");
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        config.addDataSourceProperty("cacheResultSetMetadata", "true");
        config.addDataSourceProperty("cacheServerConfiguration", "true");
        config.addDataSourceProperty("elideSetAutoCommits", "true");
        config.addDataSourceProperty("maintainTimeStats", "false");

        return new HikariDataSource(config);
    }

    /**
     * 获取连接
     */
    public static Connection getConnection() throws SQLException {
        if (dataSource == null) {
            throw new SQLException("连接池未初始化");
        }
        return dataSource.getConnection();
    }

    // ====================== 执行查询 ======================

    /**
     * 执行查询（单行结果）
     */
    public static <T> T queryForObject(String sql, Object[] params, RowMapper<T> rowMapper) {
        List<T> results = query(sql, params, rowMapper);
        return results != null && !results.isEmpty() ? results.get(0) : null;
    }

    /**
     * 执行查询（列表结果）
     */
    public static <T> List<T> query(String sql, Object[] params, RowMapper<T> rowMapper) {
        List<T> results = new ArrayList<>();

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            // 设置参数
            setParameters(stmt, params);

            // 执行查询
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(rowMapper.mapRow(rs, results.size()));
                }
            }

            LOG.debug("执行查询成功: {} 条结果", results.size());

        } catch (SQLException e) {
            LOG.error("执行查询失败: {}", sql, e);
            throw new RuntimeException("数据库查询失败", e);
        }

        return results;
    }

    /**
     * 执行更新（插入/更新/删除）
     */
    public static int update(String sql, Object[] params) {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            // 设置参数
            setParameters(stmt, params);

            // 执行更新
            int affectedRows = stmt.executeUpdate();

            LOG.debug("执行更新成功: {} 条受影响", affectedRows);
            return affectedRows;

        } catch (SQLException e) {
            LOG.error("执行更新失败: {}", sql, e);
            throw new RuntimeException("数据库更新失败", e);
        }
    }

    /**
     * 批量更新
     */
    public static int[] batchUpdate(String sql, List<Object[]> paramsList) {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            // 添加批量参数
            for (Object[] params : paramsList) {
                setParameters(stmt, params);
                stmt.addBatch();
            }

            // 执行批量更新
            int[] affectedRows = stmt.executeBatch();

            LOG.debug("执行批量更新成功: {} 条受影响", affectedRows.length);
            return affectedRows;

        } catch (SQLException e) {
            LOG.error("执行批量更新失败: {}", sql, e);
            throw new RuntimeException("数据库批量更新失败", e);
        }
    }

    // ====================== 事务 ======================

    /**
     * 执行事务
     */
    public static <T> T executeTransaction(TransactionCallback<T> callback) {
        Connection conn = null;
        boolean autoCommit = true;

        try {
            conn = getConnection();
            autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false); // 关闭自动提交

            // 执行事务逻辑
            T result = callback.doInTransaction(conn);

            conn.commit(); // 提交事务
            LOG.debug("事务提交成功");
            return result;

        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback(); // 回滚事务
                    LOG.debug("事务已回滚");
                } catch (SQLException ex) {
                    LOG.error("事务回滚失败", ex);
                }
            }
            LOG.error("事务执行失败", e);
            throw new RuntimeException("数据库事务失败", e);
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(autoCommit); // 恢复自动提交
                    conn.close();
                } catch (SQLException e) {
                    LOG.error("关闭连接失败", e);
                }
            }
        }
    }

    // ====================== 辅助方法 ======================

    /**
     * 设置 SQL 参数
     */
    private static void setParameters(PreparedStatement stmt, Object[] params) throws SQLException {
        if (params != null && params.length > 0) {
            for (int i = 0; i < params.length; i++) {
                Object param = params[i];
                if (param == null) {
                    stmt.setNull(i + 1, Types.NULL);
                } else {
                    stmt.setObject(i + 1, param);
                }
            }
        }
    }

    /**
     * 关闭资源
     */
    public static void close(ResultSet rs, Statement stmt, Connection conn) {
        try {
            if (rs != null) rs.close();
        } catch (SQLException e) {
            LOG.error("关闭 ResultSet 失败", e);
        }

        try {
            if (stmt != null) stmt.close();
        } catch (SQLException e) {
            LOG.error("关闭 Statement 失败", e);
        }

        try {
            if (conn != null) conn.close();
        } catch (SQLException e) {
            LOG.error("关闭 Connection 失败", e);
        }
    }

    // ====================== 回调接口 ======================

    /**
     * 行映射器接口
     */
    @FunctionalInterface
    public interface RowMapper<T> {
        T mapRow(ResultSet rs, int rowNum) throws SQLException;
    }

    /**
     * 事务回调接口
     */
    @FunctionalInterface
    public interface TransactionCallback<T> {
        T doInTransaction(Connection conn) throws SQLException;
    }

    // ====================== 关闭连接池 ======================

    /**
     * 关闭连接池
     */
    public static void closeDataSource() {
        if (dataSource != null) {
            try {
                dataSource.close();
                LOG.info("数据库连接池已关闭");
            } catch (Exception e) {
                LOG.error("关闭连接池失败", e);
            }
        }
    }

    // ====================== 工厂方法 ======================

    /**
     * 创建 MySQL 连接
     */
    public static HikariDataSource createMySQLDataSource(String host, int port, String database, 
                                                       String username, String password) {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=Asia/Shanghai",
                host, port, database);
        return createDataSource(jdbcUrl, username, password);
    }

    /**
     * 创建 PostgreSQL 连接
     */
    public static HikariDataSource createPostgreSQLDataSource(String host, int port, String database,
                                                            String username, String password) {
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s?currentSchema=public",
                host, port, database);
        return createDataSource(jdbcUrl, username, password);
    }

    /**
     * 创建 Doris 连接
     */
    public static HikariDataSource createDorisDataSource(String host, int port, 
                                                       String username, String password) {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d", host, port);
        return createDataSource(jdbcUrl, username, password);
    }
}
