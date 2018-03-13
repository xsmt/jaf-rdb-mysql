package cn.jcloud.jaf.rdb.config;

import cn.jcloud.jaf.common.constant.ErrorCode;
import cn.jcloud.jaf.common.exception.JafI18NException;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.vendor.MySqlValidConnectionChecker;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.Dialect;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
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
import java.util.Properties;

/**
 * Created by wei Han on 2016/5/9.
 */
@EnableTransactionManagement(proxyTargetClass = true)
@EnableJpaAuditing
public class AbstractRDBConfigurerAdapter {

    public static final String JAF_RDB_PROPERTIES_FILE_NAME = "rdb.properties";

    @Bean
    public Dialect dialect() {
        return new Utf8mb4MySQL5InnoDBDialect();
    }

    @Bean
    public DataSource dataSource() {
        return getDataSource();
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
        return true;
    }

    protected String getMappingBasePackage() {
        return "cn.jcloud";
    }

    @Bean
    public EntityManagerFactory entityManagerFactory() {
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setGenerateDdl(generateDdl());
        vendorAdapter.setShowSql(showSql());

        LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();

        factory.setJpaVendorAdapter(vendorAdapter);
        factory.setPackagesToScan(getMappingBasePackage());
        factory.setDataSource(dataSource());
        factory.getJpaPropertyMap().put("hibernate.ejb.naming_strategy", "org.hibernate.cfg.ImprovedNamingStrategy");
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

    private DataSource getDataSource() {
        Properties properties;
        try {
            properties = PropertiesLoaderUtils.loadProperties(new ClassPathResource(JAF_RDB_PROPERTIES_FILE_NAME));
        } catch (IOException e) {
            throw JafI18NException.of(ErrorCode.CONFIG_LOADING_FAIL, e, JAF_RDB_PROPERTIES_FILE_NAME);
        }
        try {
            DruidDataSource dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
            MySqlValidConnectionChecker validConnectionChecker = new MySqlValidConnectionChecker();
            validConnectionChecker.setUsePingMethod(false);
            dataSource.setValidConnectionChecker(validConnectionChecker);
            return dataSource;
        } catch (Exception e) {
            String message = "create datasource with config [" + JAF_RDB_PROPERTIES_FILE_NAME + "] failure";
            throw JafI18NException.of(message, ErrorCode.CONFIG_LOADING_FAIL, e);
        }
    }
}
