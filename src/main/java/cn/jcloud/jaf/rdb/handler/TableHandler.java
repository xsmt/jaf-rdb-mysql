package cn.jcloud.jaf.rdb.handler;

import cn.jcloud.jaf.common.constant.ErrorCode;
import cn.jcloud.jaf.common.exception.JafI18NException;
import cn.jcloud.jaf.common.handler.TenantHandler;
import cn.jcloud.jaf.common.tenant.domain.Tenant;
import cn.jcloud.jaf.common.tenant.event.TenantCreateEvent;
import cn.jcloud.jaf.common.tenant.service.TenantSupport;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.ImprovedNamingStrategy;
import org.hibernate.dialect.Dialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * 表处理类
 * Created by Wei Han on 2016/1/5.
 */
public class TableHandler {


    private static final Logger LOG = LoggerFactory.getLogger(TableHandler.class);

    @Autowired
    private DataSource dataSource;

    @Autowired(required = false)
    private List<TenantSupport> tenantSupports;

    private Set<Class> entityClasses;

    private Dialect dialect;

    public TableHandler(Dialect dialect) {
        this.dialect = dialect;
    }

    @PostConstruct
    public void postConstruct() {
        entityClasses = new HashSet<>();
        if (tenantSupports == null) {
            tenantSupports = Collections.emptyList();
            return;
        }
        for (TenantSupport tenantSupport : tenantSupports) {
            if (tenantSupport.getEntities() == null) {
                continue;
            }
            entityClasses.addAll(Arrays.asList(tenantSupport.getEntities()));
        }
    }

    /**
     * 处理公司新增事件，该处理将于公司新增的保存提交后执行
     *
     * @param event 事件
     */

    @Order(0)
    @EventListener
    public void handleTenantCreate(TenantCreateEvent event) {
        createTable(entityClasses, event.getTenant());
    }

    public void createTable(Set<Class> entityClasses, Tenant tenant) {
        TenantHandler.setTenant(tenant);
        innerCreateTable(entityClasses, TenantHandler.PREFIX, TenantHandler.getTablePrefix());
        TenantHandler.clear();
    }


    private void innerCreateTable(Set<Class> entityClasses, String... keyValues) {
        if (keyValues.length % 2 == 1) {
            throw JafI18NException.of("创建表时需要替换的字符与值应成对出现", ErrorCode.INVALID_QUERY);
        }

        String[] sqlArr = entity2Sql(dialect, entityClasses);

        Connection connection = DataSourceUtils.getConnection(dataSource);
        try (Statement stmt = connection.createStatement()) {
            for (String sql : sqlArr) {
                for (int i = 0; i < keyValues.length; i = i + 2) {
                    String key = keyValues[i];
                    String value = keyValues[i + 1];
                    sql = sql.replace(key, value);
                }
                sql = make(sql);
                LOG.info("建表sql:{}", sql);
                executeSql(stmt, sql);

            }
        } catch (SQLException e) {
            String msg = "创建表失败";
            throw JafI18NException.of(msg, ErrorCode.INVALID_QUERY, e);
        } finally {
            DataSourceUtils.releaseConnection(connection, dataSource);
        }
    }

    private static void executeSql(Statement stmt, String sql) {
        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            //忽略改表与创建索引失败，因为此类创建无法使用if not exists语法
            if (!sql.startsWith("alter table") && !sql.startsWith("create index")) {
                String msg = "创建表失败";
                throw JafI18NException.of(msg, ErrorCode.INVALID_QUERY, e);
            }
            LOG.warn(e.getMessage());
        }
    }

    /**
     * 对sql进行特殊处理
     */
    private static String make(String sql) {
        //增加if not exists
        if (sql.startsWith("create table")) {
            return sql.replace("create table", "create table if not exists");
        }
        //重命名约束名
        if (sql.contains("add constraint")) {
            String[] fields = sql.split("\\s");
            fields[5] = fields[5] + TenantHandler.getTableNum();
            return StringUtils.join(fields, " ");
        }
        return sql;
    }

    private static String[] entity2Sql(Dialect dialect, Set<Class> entityClasses) {
        Configuration cfg = new Configuration();
        cfg.setNamingStrategy(new ImprovedNamingStrategy());
        for (Class entityClass : entityClasses) {
            cfg.addAnnotatedClass(entityClass);
        }
        return cfg.generateSchemaCreationScript(dialect);
    }
}
