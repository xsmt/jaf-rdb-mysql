package cn.jcloud.jaf.rdb.upgrade;

import cn.jcloud.gaea.util.WafJsonMapper;
import cn.jcloud.jaf.common.exception.JafI18NException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.Map;

/**
 * 升级脚本实体
 * Created by Wei Han on 2016-07-20.
 */
public class DBVersion implements Comparable<DBVersion> {

    public static final String MULTI_TENANCY_FLAG = "MT";

    private static final String FILE_NAME_FORMAT = "V{version}[_{rank}][_MT]__{description}.sql";

    private static final String WRONG_FILE_FORMAT = "wrong upgrade script file name,format must [" + FILE_NAME_FORMAT + "]";

    private Integer id;

    private short version;

    private byte rank;

    private UpgradeType upgradeType;

    private ScriptType scriptType;

    private String description;

    private String filename;

    private boolean multiTenancy;

    private Date executeBeginTime;

    private Date executeEndTime;

    private Status status;

    private String addition;

    private transient File scriptFile;

    public static DBVersion parse(File scriptFile, UpgradeType upgradeType) {
        String filename = scriptFile.getName();
        String fileExtension = FilenameUtils.getExtension(filename);
        ScriptType scriptType = ScriptType.of(fileExtension);

        String fileBaseName = FilenameUtils.getBaseName(filename);
        String[] info = fileBaseName.split("__");
        DBVersion script = new DBVersion();
        if (info.length == 2) {
            script.description = info[1].replaceAll("_", " ");
        } else if (info.length < 1) {
            throw JafI18NException.of(WRONG_FILE_FORMAT);
        }

        String[] info0 = info[0].split("_");
        script.version = parseVersion(info0);
        script.rank = parseRank(info0);
        script.multiTenancy = parseMultiTenancy(info0);
        script.scriptType = scriptType;
        script.upgradeType = upgradeType;
        script.filename = filename;
        script.scriptFile = scriptFile;
        return script;
    }

    public static DBVersion parse(Map<String, Object> map) {
        if (CollectionUtils.isEmpty(map)) {
            return null;
        }
        try {
            DBVersion dbVersion = new DBVersion();
            String json = WafJsonMapper.getMapper().writeValueAsString(map);
            WafJsonMapper.getMapper().readerForUpdating(dbVersion).readValue(json);
            return dbVersion;
        } catch (IOException e) {
            throw JafI18NException.of(e.getMessage(), e);
        }
    }

    @Override
    public int compareTo(DBVersion o) {
        return ((this.version << 8) + this.rank) - ((o.version << 8) + o.rank);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DBVersion dbVersion = (DBVersion) o;

        if (version != dbVersion.version) return false;
        return rank == dbVersion.rank;

    }

    @Override
    public int hashCode() {
        int result = (int) version;
        result = 31 * result + (int) rank;
        return result;
    }

    public Integer getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public short getVersion() {
        return version;
    }

    public void setVersion(short version) {
        this.version = version;
    }

    public byte getRank() {
        return rank;
    }

    public void setRank(byte rank) {
        this.rank = rank;
    }

    public UpgradeType getUpgradeType() {
        return upgradeType;
    }

    public void setUpgradeType(UpgradeType upgradeType) {
        this.upgradeType = upgradeType;
    }

    public ScriptType getScriptType() {
        return scriptType;
    }

    public void setScriptType(ScriptType scriptType) {
        this.scriptType = scriptType;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public boolean isMultiTenancy() {
        return multiTenancy;
    }

    public void setMultiTenancy(boolean multiTenancy) {
        this.multiTenancy = multiTenancy;
    }

    public Date getExecuteBeginTime() {
        return executeBeginTime;
    }

    public void setExecuteBeginTime(Date executeBeginTime) {
        this.executeBeginTime = executeBeginTime;
    }

    public Date getExecuteEndTime() {
        return executeEndTime;
    }

    public void setExecuteEndTime(Date executeEndTime) {
        this.executeEndTime = executeEndTime;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getAddition() {
        return addition;
    }

    public void setAddition(String addition) {
        this.addition = addition;
    }

    public File getScriptFile() {
        if (null != scriptFile) {
            return scriptFile;
        }
        if (filename == null) {
            return null;
        }
        String path = upgradeType.getDir() + filename;
        URL resource = this.getClass().getClassLoader().getResource(path);
        if (null == resource) {
            throw JafI18NException.of("upgrade file [" + path + "] not found");
        }
        this.scriptFile = new File(resource.getPath());
        return this.scriptFile;
    }

    private static byte parseVersion(String[] info) {
        String version = info[0].toLowerCase().replace("v", "");
        if (!NumberUtils.isDigits(version)) {
            throw JafI18NException.of(WRONG_FILE_FORMAT);
        }
        return NumberUtils.toByte(version);
    }

    private static byte parseRank(String[] info) {
        if (info.length > 1) {
            String rank = info[1];
            if (NumberUtils.isDigits(rank)) {
                return NumberUtils.toByte(rank);
            }
        }
        return 0;
    }

    private static boolean parseMultiTenancy(String[] info) {
        if (info.length == 1) {
            return false;
        } else if (info.length == 2) {
            if (MULTI_TENANCY_FLAG.equals(info[1])) {
                return true;
            }
        } else if (info.length == 3) {
            if (MULTI_TENANCY_FLAG.equals(info[2])) {
                return true;
            }
        }
        return false;
    }
}
