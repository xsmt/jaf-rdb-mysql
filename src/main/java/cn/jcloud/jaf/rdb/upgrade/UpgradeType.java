package cn.jcloud.jaf.rdb.upgrade;

/**
 * Created by Wei Han on 2016-07-27.
 */
public enum UpgradeType {
    FIX("fix/"),
    INSTALL("install/");

    private String dir;

    UpgradeType(String dir) {
        this.dir = dir;
    }

    public String getDir() {
        return dir;
    }
}
