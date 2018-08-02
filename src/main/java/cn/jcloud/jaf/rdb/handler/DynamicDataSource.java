package cn.jcloud.jaf.rdb.handler;

import cn.jcloud.jaf.common.handler.TenantHandler;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

/**
 * Created by Wei Han on 2016/2/3.
 */
public class DynamicDataSource extends AbstractRoutingDataSource {
    @Override
    protected Object determineCurrentLookupKey() {
        return TenantHandler.getDbConn();
    }
}
