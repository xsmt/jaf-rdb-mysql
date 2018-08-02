package cn.jcloud.jaf.rdb.upgrade;

import cn.jcloud.jaf.common.exception.JafI18NException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * Created by Wei Han on 2016-07-27.
 */
public class DBUtils {

    private DBUtils() {

    }

    public static boolean isEmptyDatabase(DataSource dataSource) {
        String sql = "show tables";
        return getResultList(dataSource, sql).size() <= 1;
    }

    public static Map<String, Object> getSingleResult(DataSource dataSource, String sql, Object... params) {
        Connection connection = DataSourceUtils.getConnection(dataSource);
        try {
            return getSingleResult(connection, sql, params);
        } finally {
            DataSourceUtils.releaseConnection(connection, dataSource);
        }
    }

    public static Map<String, Object> getSingleResult(Connection connection, String sql, Object... params) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.prepareStatement(sql);
            for (int i = 0, l = params.length; i < l; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            rs = stmt.executeQuery();
            if (rs.next()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                Map<String, Object> result = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    result.put(metaData.getColumnLabel(i), rs.getObject(i));
                }
                return result;
            }
            return Collections.emptyMap();
        } catch (SQLException e) {
            throw JafI18NException.of(e.getMessage(), e);
        } finally {
            closeResultSet(rs);
            closeStatement(stmt);
        }
    }

    public static List<Map<String, Object>> getResultList(DataSource dataSource, String sql, Object... params) {
        Connection connection = DataSourceUtils.getConnection(dataSource);
        try {
            return getResultList(connection, sql, params);
        } finally {
            DataSourceUtils.releaseConnection(connection, dataSource);
        }
    }

    public static List<Map<String, Object>> getResultList(Connection connection, String sql, Object... params) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.prepareStatement(sql);
            for (int i = 0, l = params.length; i < l; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            rs = stmt.executeQuery();
            List<Map<String, Object>> result = new ArrayList<>();
            while (rs.next()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                Map<String, Object> element = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    element.put(metaData.getColumnLabel(i), rs.getObject(i));
                }
                result.add(element);
            }
            return result;
        } catch (SQLException e) {
            throw JafI18NException.of(e.getMessage(), e);
        } finally {
            closeResultSet(rs);
            closeStatement(stmt);
        }
    }

    public static void execute(DataSource dataSource, String... sqlArr) {
        Connection connection = DataSourceUtils.getConnection(dataSource);
        Statement stmt = null;
        String currentSql = null;
        try {
            connection.setAutoCommit(false);
            stmt = connection.createStatement();
            for (String sql : sqlArr) {
                if (StringUtils.isBlank(sql)) {
                    continue;
                }
                currentSql = sql;
                stmt.addBatch(sql);
            }
            stmt.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection);
            throw JafI18NException.of("current sql:" + currentSql + ";" + e.getMessage(), e);
        } finally {
            closeStatement(stmt);
            DataSourceUtils.releaseConnection(connection, dataSource);
        }
    }

    public static void execute(DataSource dataSource, String sql, Object... params) {
        Connection connection = DataSourceUtils.getConnection(dataSource);
        try {
            execute(connection, sql, params);
        } finally {
            DataSourceUtils.releaseConnection(connection, dataSource);
        }
    }

    public static void execute(Connection connection, String sql, Object... params) {
        PreparedStatement stmt = null;
        try {
            connection.setAutoCommit(false);
            stmt = connection.prepareStatement(sql);
            for (int i = 0, l = params.length; i < l; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            stmt.execute();
            connection.commit();
        } catch (SQLException e) {
            rollback(connection);
            throw JafI18NException.of(e.getMessage(), e);
        } finally {
            closeStatement(stmt);
        }
    }

    private static void closeStatement(Statement stmt) {
        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void closeResultSet(ResultSet rs) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void rollback(Connection connection) {
        if (null != connection) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }
}
