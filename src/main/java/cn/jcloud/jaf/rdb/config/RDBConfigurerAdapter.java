package cn.jcloud.jaf.rdb.config;

import cn.jcloud.jaf.rdb.base.repository.BaseRepositoryImpl;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.stereotype.Repository;

/**
 * 关系型数据库配置基础类
 * Created by Wei Han on 2016/4/8.
 */
@EnableJpaRepositories(value = "cn.jcloud",
        includeFilters = {@ComponentScan.Filter(Repository.class)},
        repositoryBaseClass = BaseRepositoryImpl.class)
public class RDBConfigurerAdapter extends AbstractRDBConfigurerAdapter {
}
