package cn.jcloud.jaf.rdb.upgrade;

import cn.jcloud.jaf.common.exception.JafI18NException;

/**
 * Created by Wei Han on 2016-07-27.
 */
public enum ScriptType {

    SQL_SCRIPT("sql");

    private String extension;

    ScriptType(String extension) {
        this.extension = extension;
    }

    public String getExtension() {
        return extension;
    }

    public static ScriptType of(String extension) {
        if ("sql".equals(extension)) {
            return SQL_SCRIPT;
        }
        throw JafI18NException.of("unknown upgrade script extension [" + extension + "]");
    }

}
