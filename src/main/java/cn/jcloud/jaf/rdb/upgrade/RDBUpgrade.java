package cn.jcloud.jaf.rdb.upgrade;

import cn.jcloud.gaea.util.WafJsonMapper;
import cn.jcloud.jaf.common.exception.JafI18NException;
import cn.jcloud.jaf.common.handler.TenantHandler;
import cn.jcloud.jaf.common.tenant.domain.Tenant;
import cn.jcloud.jaf.rdb.handler.TableMapperInterceptor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;

/**
 * 数据库升级
 * Created by Wei Han on 2016-07-19.
 */
public class RDBUpgrade {

    private static final String TENANCY_SQL = "select db_conn as d,tenancy as t from t_tenant where id in(select max(id) as t from t_tenant group by db_conn,tenancy)";

    private DataSource dataSource;

    private List<DBVersion> fixScripts;
    private List<DBVersion> installScripts;
    private DBVersionDao dbVersionDao;
    private ObjectMapper mapper = WafJsonMapper.getMapper();

    public RDBUpgrade(DataSource dataSource) {
        this.dataSource = dataSource;
        this.dbVersionDao = new DBVersionDao(dataSource);
        this.fixScripts = getFiles(UpgradeType.FIX);
        this.installScripts = getFiles(UpgradeType.INSTALL);
        dbVersionDao.createTable();
    }

    /**
     * 执行修复操作
     */
    public void fix() {
        execute(UpgradeType.FIX, fixScripts);
    }

    /**
     * 执行安装操作
     */
    public void install() {
        execute(UpgradeType.INSTALL, installScripts);
    }

    private void execute(UpgradeType upgradeType, List<DBVersion> scripts) {
        if (CollectionUtils.isEmpty(scripts)) {
            return;
        }
        dbVersionDao.lock();
        try {
            final DBVersion lastDBVersion = dbVersionDao.last(upgradeType);
            if (ignoreHistoryFixScriptWhenNewDB(upgradeType, scripts, lastDBVersion)) {
                return;
            }

            List<DBVersion> unExecutedScripts = getUnExecutedScripts(scripts, lastDBVersion);
            for (DBVersion dbVersion : unExecutedScripts) {
                executeScript(dbVersion);
            }
        } finally {
            dbVersionDao.unlock();
        }
    }

    private void executeScript(DBVersion dbVersion) {
        String scriptFileContent;
        try {
            scriptFileContent = FileUtils.readFileToString(dbVersion.getScriptFile());
        } catch (IOException e) {
            throw JafI18NException.of("unable to read upgrade file [" + dbVersion.getScriptFile().getName() + "]");
        }
        String[] sqlArr = scriptFileContent.split(";;");
        if (dbVersion.isMultiTenancy()) {
            executeMultiTenancyScript(dbVersion, sqlArr);
        } else {
            executeNormalScript(dbVersion, sqlArr);
        }
    }

    /**
     * 执行非租户升级脚本
     */
    private void executeNormalScript(DBVersion dbVersion, String[] sqlArr) {
        startExecute(dbVersion);
        try {
            DBUtils.execute(dataSource, sqlArr);
        } catch (Exception e) {
            endExecute(dbVersion, Status.FAIL);
            throw JafI18NException.of(e.getMessage(), e);
        }
        endExecute(dbVersion, Status.FINISHED);
    }

    /**
     * 执行租户升级脚本
     */
    private void executeMultiTenancyScript(DBVersion dbVersion, String[] sqlArr) {
        List<Map<String, Object>> tenants;
        if (Status.EXECUTING == dbVersion.getStatus() || Status.FAIL == dbVersion.getStatus()) {
            tenants = json2List(dbVersion.getAddition());
        } else {
            tenants = DBUtils.getResultList(dataSource, TENANCY_SQL);
        }
        for (int i = 0, s = tenants.size(); i < s; i++) {
            Map<String, Object> map = tenants.get(i);
            Tenant tenant = map2Tenant(map);
            String addition = list2Json(tenants.subList(i, s));
            dbVersion.setAddition(addition);
            startExecute(dbVersion);
            TenantHandler.setTenant(tenant);
            String[] tenancySqlArr = new String[sqlArr.length];
            for (int j = 0, l = tenancySqlArr.length; j < l; j++) {
                tenancySqlArr[j] = TableMapperInterceptor.make(sqlArr[j]);
            }
            try {
                DBUtils.execute(dataSource, tenancySqlArr);
            } catch (Exception e) {
                endExecute(dbVersion, Status.FAIL);
                throw JafI18NException.of(e.getMessage(), e);
            }
            TenantHandler.clear();
            addition = list2Json(tenants.subList(i + 1, s));
            dbVersion.setAddition(addition);
            startExecute(dbVersion);
        }
        endExecute(dbVersion, Status.FINISHED);
    }

    private void startExecute(DBVersion dbVersion) {
        dbVersion.setStatus(Status.EXECUTING);
        dbVersion.setExecuteBeginTime(new Date());
        dbVersionDao.save(dbVersion);
    }

    private void endExecute(DBVersion dbVersion, Status status) {
        dbVersion.setStatus(status);
        dbVersion.setExecuteEndTime(new Date());
        dbVersionDao.save(dbVersion);
    }

    /**
     * 获取未执行的暂本列表
     */
    private List<DBVersion> getUnExecutedScripts(List<DBVersion> scripts, final DBVersion lastDBVersion) {
        if (CollectionUtils.isEmpty(scripts)) {
            return Collections.emptyList();
        } else if (null == lastDBVersion) {
            return scripts;
        } else {
            return FluentIterable.from(scripts)
                    .filter(new Predicate<DBVersion>() {
                        @Override
                        public boolean apply(DBVersion input) {
                            if (input.compareTo(lastDBVersion) > 0) {
                                return true;
                            } else if (Status.FINISHED != lastDBVersion.getStatus()
                                    && input.equals(lastDBVersion)) {
                                BeanUtils.copyProperties(lastDBVersion, input);
                                return true;
                            }
                            return false;
                        }
                    }).toList();
        }
    }

    /**
     * 当前已有fix脚本且是新库的场景这些脚本均不执行
     * lastDBVersion==null表示连初始化的记录也没有，防止多实例重复执行
     */
    private boolean ignoreHistoryFixScriptWhenNewDB(UpgradeType upgradeType, List<DBVersion> scripts, DBVersion lastDBVersion) {
        //当前已有fix脚本且是新库的场景这些脚本均不执行
        //lastDBVersion==null表示连初始化的记录也没有，防止多实例重复执行
        if (null == lastDBVersion
                && UpgradeType.FIX == upgradeType
                && DBUtils.isEmptyDatabase(dataSource)) {
            //插入一条比最后一个版本的fix版本一样大，且rank大的记录，以限制后续fix脚本的执行
            //fix 与 install版本是分开的，两套体系
            DBVersion dbVersion = scripts.get(scripts.size() - 1);
            dbVersion.setFilename(null);
            dbVersion.setRank((byte) (dbVersion.getRank() + 1));
            dbVersion.setDescription("initial");
            Date now = new Date();
            dbVersion.setExecuteBeginTime(now);
            dbVersion.setExecuteEndTime(now);
            dbVersion.setStatus(Status.FINISHED);
            dbVersionDao.add(dbVersion);
            return true;
        }
        return false;
    }

    private List<Map<String, Object>> json2List(String json) {
        if (StringUtils.isBlank(json)) {
            return Collections.emptyList();
        }
        try {
            return mapper.readValue(json,
                    mapper.getTypeFactory()
                            .constructCollectionType(List.class, Map.class));
        } catch (IOException e) {
            throw JafI18NException.of(e.getMessage(), e);
        }
    }

    private String list2Json(List<Map<String, Object>> list) {
        if (CollectionUtils.isEmpty(list)) {
            return "";
        }
        try {
            return mapper.writeValueAsString(list);
        } catch (JsonProcessingException e) {
            throw JafI18NException.of(e.getMessage(), e);
        }
    }

    private Tenant map2Tenant(Map<String, Object> map) {
        Tenant tenant = new Tenant();
        tenant.setDbConn(map.get("d").toString());
        tenant.setTenancy(Long.parseLong(map.get("t").toString()));
        return tenant;
    }

    /**
     * 获取升级脚本，返回按版本号排序的文件列表
     */
    private static List<DBVersion> getFiles(final UpgradeType upgradeType) {
        URL resource = RDBUpgrade.class.getClassLoader().getResource(upgradeType.getDir());
        if (null == resource) {
            return Collections.emptyList();
        }
        String sqlDirPath = resource.getPath();
        return FluentIterable.from(FileUtils.listFiles(new File(sqlDirPath), null, false))
                .transform(new Function<File, DBVersion>() {
                    @Override
                    public DBVersion apply(File input) {
                        return DBVersion.parse(input, upgradeType);
                    }
                }).toSortedList(new Comparator<DBVersion>() {
                    @Override
                    public int compare(DBVersion o1, DBVersion o2) {
                        int result = o1.compareTo(o2);
                        if (result == 0) {
                            throw JafI18NException.of(
                                    "upgrade script file [" + o1.getFilename()
                                            + "] is the same [version] and [rank] as ["
                                            + o2.getFilename() + "]");
                        }
                        return result;
                    }
                });
    }
}
