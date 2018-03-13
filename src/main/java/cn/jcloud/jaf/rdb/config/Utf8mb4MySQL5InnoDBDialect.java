package cn.jcloud.jaf.rdb.config;

import org.hibernate.dialect.MySQL5InnoDBDialect;

/**
 * hibernate方言，支持utf8mb4
 * Created by Zhang Jinlong(150429) on 2016/5/6.
 */
public class Utf8mb4MySQL5InnoDBDialect extends MySQL5InnoDBDialect {
    @Override
    public String getTableTypeString() {
        return " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    }
}