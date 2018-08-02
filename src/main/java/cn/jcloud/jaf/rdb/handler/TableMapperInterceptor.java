package cn.jcloud.jaf.rdb.handler;

import cn.jcloud.jaf.common.constant.ErrorCode;
import cn.jcloud.jaf.common.exception.JafI18NException;
import cn.jcloud.jaf.common.handler.TenantHandler;
import org.hibernate.EmptyInterceptor;

/**
 * 表映射拦截器
 * Created by Wei Han on 2016/1/4.
 */
public class TableMapperInterceptor extends EmptyInterceptor {

    @Override
    public String onPrepareStatement(String sql) {
        return make(sql);
    }

    /**
     * 对sql的表前缀进行替换
     *
     * @param sql
     * @return
     */
    public static String make(String sql) {
        if (sql.contains(TenantHandler.PREFIX)) {
            String tablePrefix = TenantHandler.getTablePrefix();
            if (TenantHandler.DEFAULT_PREFIX.equals(tablePrefix)) {
                throw JafI18NException.of("操作的表为租户业务表，但上下文未找到租户信息", ErrorCode.MISSING_ORG_ID);
            }
            return sql.replaceAll(TenantHandler.PREFIX, tablePrefix);
        }
        return sql;
    }
}
