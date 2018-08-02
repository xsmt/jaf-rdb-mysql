package cn.jcloud.jaf.rdb.config;

import cn.jcloud.jaf.common.constant.ErrorCode;
import cn.jcloud.jaf.common.constant.WafStage;
import cn.jcloud.jaf.common.exception.JafI18NException;
import cn.jcloud.jaf.rdb.handler.DynamicDataSource;
import cn.jcloud.jaf.rdb.handler.TableHandler;
import cn.jcloud.jaf.rdb.handler.TableMapperInterceptor;
import cn.jcloud.jaf.rdb.upgrade.RDBUpgrade;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.vendor.MySqlValidConnectionChecker;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.Dialect;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.persistence.EntityManagerFactory;
import javax.persistence.ValidationMode;
import javax.sql.DataSource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Wei Han on 2016/5/9.
 */
@EnableTransactionManagement(proxyTargetClass = true)
@EnableJpaAuditing
public class AbstractRDBConfigurerAdapter {

    protected String configFileNamePrefix() {
        return "rdb";
    }

    protected String configFileNameSeparator() {
        return "-";
    }

    protected String getMappingBasePackage() {
        return "cn.jcloud";
    }

    @Bean
    public Dialect dialect() {
        return new Utf8mb4MySQL5InnoDBDialect();
    }

    @Bean
    public TableHandler tableHandler() {
        return new TableHandler(dialect());
    }

    @Bean
    public DataSource dataSource() {
        DynamicDataSource dynamicDataSource = new DynamicDataSource();
        Map<Object, Object> dataSourceMap = new HashMap<>();
        Resource[] dataSourceResources = getDataSourceResources();
        checkDataSourceResources(dataSourceResources);
        for (Resource resource : dataSourceResources) {
            String resourceName = resource.getFilename();
            DataSource dataSource = getDataSource(resource);
            dataSourceMap.put(getResourceCode(resourceName), dataSource);
            if (isDefaultDs(resourceName)) {
                dynamicDataSource.setDefaultTargetDataSource(dataSource);
            }
        }
        dynamicDataSource.setTargetDataSources(dataSourceMap);
        dynamicDataSource.afterPropertiesSet();
        RDBUpgrade rdbUpgrade = new RDBUpgrade(dynamicDataSource);
        rdbUpgrade.fix();
        rdbUpgrade.install();
        return dynamicDataSource;
    }

    @Bean
    public JdbcTemplate jdbcTemplate() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(dataSource());
        return jdbcTemplate;
    }

    protected boolean generateDdl() {
        return true;
    }

    protected boolean showSql() {
        //默认仅开发环境打印sql
        return WafStage.DEVELOPMENT.is();
    }

    @Bean
    public EntityManagerFactory entityManagerFactory() {
        RDBUpgrade rdbUpgrade = new RDBUpgrade(dataSource());
        rdbUpgrade.fix();
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setGenerateDdl(generateDdl());
        vendorAdapter.setShowSql(showSql());

        LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();

        factory.setJpaVendorAdapter(vendorAdapter);
        factory.setPackagesToScan(getMappingBasePackage());
        factory.setDataSource(dataSource());
        factory.getJpaPropertyMap().put("hibernate.ejb.naming_strategy", "org.hibernate.cfg.ImprovedNamingStrategy");
        factory.getJpaPropertyMap().put("hibernate.ejb.interceptor", TableMapperInterceptor.class.getCanonicalName());
        factory.getJpaPropertyMap().put(Environment.DIALECT, dialect().getClass().getCanonicalName());
        factory.setValidationMode(ValidationMode.NONE);
        factory.afterPropertiesSet();
        return factory.getObject();
    }

    @Bean
    public PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        JpaTransactionManager txManager = new JpaTransactionManager();
        txManager.setEntityManagerFactory(entityManagerFactory);
        return txManager;
    }

    private Resource[] getDataSourceResources() {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        try {
            return resolver.getResources(configFileNamePrefix() + "*.properties");
        } catch (IOException e) {
            throw JafI18NException.of(ErrorCode.CONFIG_LOADING_FAIL, configFileNamePrefix());
        }
    }

    private void checkDataSourceResources(Resource[] dataSourceResources) {
        if (dataSourceResources == null || dataSourceResources.length == 0) {
            String message = String.format(
                    "database config missing，config name must be format [%1$s] or [%1$s%2$s{datasource-name}]",
                    configFileNamePrefix(), configFileNameSeparator());
            throw JafI18NException.of(message, ErrorCode.CONFIG_MISSING);
        }
        String defaultDsResourceName = null;
        for (Resource resource : dataSourceResources) {
            String resourceName = resource.getFilename();
            if (isDefaultDs(resourceName)) {
                if (defaultDsResourceName != null) {
                    String message = String.format(
                            "find more than one default database config【%s】and【%s】",
                            defaultDsResourceName, resourceName);
                    throw JafI18NException.of(message, ErrorCode.CONFIG_LOADING_FAIL);
                }
                defaultDsResourceName = resourceName;
            }
        }
        if (defaultDsResourceName == null) {
            String message = String.format(
                    "database default config missing，config name must be format [%1$s] or [%1$s%2$sdefault]",
                    configFileNamePrefix(), configFileNameSeparator());
            throw JafI18NException.of(message, ErrorCode.CONFIG_MISSING);
        }
    }

    private String getResourceCode(String resourceName) {
        if (isDefaultDs(resourceName)) {
            return "default";
        }
        return resourceName.replace(configFileNamePrefix() + configFileNameSeparator(), "")
                .replace(".properties", "");
    }

    private boolean isDefaultDs(String resourceName) {
        return String.format("%s%sdefault.properties", configFileNamePrefix(), configFileNameSeparator())
                .equals(resourceName)
                || String.format("%s.properties", configFileNamePrefix())
                .equals(resourceName);
    }

    private DataSource getDataSource(Resource resource) {
        Properties properties;
        try {
            properties = PropertiesLoaderUtils.loadProperties(resource);
        } catch (IOException e) {
            throw JafI18NException.of(ErrorCode.CONFIG_LOADING_FAIL, e, resource.getFilename());
        }
        try {
            DruidDataSource dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
            MySqlValidConnectionChecker validConnectionChecker = new MySqlValidConnectionChecker();
            validConnectionChecker.setUsePingMethod(false);
            dataSource.setValidConnectionChecker(validConnectionChecker);
            return dataSource;
        } catch (Exception e) {
            String message = "create datasource with config [" + resource.getFilename() + "] failure";
            throw JafI18NException.of(message, ErrorCode.CONFIG_LOADING_FAIL, e);
        }
    }
}
