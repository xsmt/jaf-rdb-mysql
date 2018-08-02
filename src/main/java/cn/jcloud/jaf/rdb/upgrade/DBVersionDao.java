package cn.jcloud.jaf.rdb.upgrade;

import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Map;

/**
 * Created by Wei Han on 2016-07-27.
 */
public class DBVersionDao {

    private DataSource dataSource;

    private Connection connection;

    private static final String LAST_SQL = "SELECT * FROM db_version WHERE UPGRADE_TYPE = ? ORDER BY ID DESC LIMIT 1";

    private static final String INSERT_SQL =
            "INSERT INTO db_version(\n" +
                    "VERSION,            RANK,\n" +
                    "UPGRADE_TYPE,       SCRIPT_TYPE,\n" +
                    "FILENAME,           DESCRIPTION,\n" +
                    "MULTI_TENANCY,      ADDITION,\n" +
                    "EXECUTE_BEGIN_TIME, EXECUTE_END_TIME,\n" +
                    "STATUS,             ID) \n" +
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String UPDATE_SQL =
            "UPDATE db_version SET \n" +
                    "  VERSION = ?,            RANK = ?,\n" +
                    "  UPGRADE_TYPE = ?,       SCRIPT_TYPE = ?,\n" +
                    "  FILENAME = ?,           DESCRIPTION = ?,\n" +
                    "  MULTI_TENANCY = ?,      ADDITION = ?,\n" +
                    "  EXECUTE_BEGIN_TIME = ?, EXECUTE_END_TIME = ?,\n" +
                    "  STATUS = ? \n" +
                    "WHERE ID = ?";

    public DBVersionDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private static final String CREATE_DB_VERSION_SQL =
            "create table if not exists db_version(\n" +
                    "`id` int not null comment \"id\",\n" +
                    "`version` smallint not null comment \"版本号\",\n" +
                    "`rank` tinyint not null comment \"顺序号\",\n" +
                    "`upgrade_type` tinyint not null comment \"类型，目前分为0-fix，1-install\",\n" +
                    "`script_type` tinyint comment \"脚本类型,0-sql脚本\",\n" +
                    "`filename` varchar(120) comment \"执行升级的文件名\",\n" +
                    "`description` varchar(100) comment \"id\",\n" +
                    "`multi_tenancy` bit not null default 0 comment \"id\",\n" +
                    "`execute_begin_time` datetime not null comment \"执行开始时间\",\n" +
                    "`execute_end_time` datetime comment \"执行结束时间\",\n" +
                    "`status` tinyint not null comment \"状态\",\n" +
                    "`addition` varchar(600),\n" +
                    "primary key (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

    public void createTable() {
        DBUtils.execute(dataSource, CREATE_DB_VERSION_SQL);
    }

    public DBVersion last(UpgradeType upgradeType) {
        Map<String, Object> map = DBUtils.getSingleResult(connection, LAST_SQL, upgradeType.ordinal());
        return DBVersion.parse(map);
    }

    public DBVersion save(DBVersion dbVersion) {
        if (null == dbVersion.getId()) {
            return add(dbVersion);
        }
        return update(dbVersion);
    }

    public DBVersion add(DBVersion dbVersion) {
        int id = (dbVersion.getVersion() << 9)
                + (dbVersion.getRank() << 1)
                + dbVersion.getUpgradeType().ordinal();
        dbVersion.setId(id);
        Object[] params = params(dbVersion);
        DBUtils.execute(connection, INSERT_SQL, params);
        return dbVersion;
    }

    public DBVersion update(DBVersion dbVersion) {
        Object[] params = params(dbVersion);
        DBUtils.execute(connection, UPDATE_SQL, params);
        return dbVersion;
    }

    public void lock() {
        this.connection = DataSourceUtils.getConnection(dataSource);
        DBUtils.execute(connection, "LOCK TABLE db_version WRITE");
    }

    public void unlock() {
        DBUtils.execute(connection, "UNLOCK TABLE");
        DataSourceUtils.releaseConnection(connection, dataSource);
    }

    private Object[] params(DBVersion dbVersion) {
        Integer scriptType = dbVersion.getScriptType() == null
                ? null : dbVersion.getScriptType().ordinal();
        Integer upgradeType = dbVersion.getUpgradeType() == null
                ? null : dbVersion.getUpgradeType().ordinal();
        int multiTenancy = dbVersion.isMultiTenancy() ? 1 : 0;
        Integer status = dbVersion.getStatus() == null ? null : dbVersion.getStatus().ordinal();
        return new Object[]{dbVersion.getVersion(), dbVersion.getRank(),
                upgradeType, scriptType,
                dbVersion.getFilename(), dbVersion.getDescription(),
                multiTenancy, dbVersion.getAddition(),
                dbVersion.getExecuteBeginTime(), dbVersion.getExecuteEndTime(),
                status, dbVersion.getId()};
    }
}
