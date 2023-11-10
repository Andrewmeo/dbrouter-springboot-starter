package com.bree.dbrouter.config;

import com.bree.dbrouter.dynamicSource.DynamicDataSource;
import com.bree.dbrouter.interceptor.DynamicMybatisPlugin;
import com.bree.dbrouter.strategy.IDBRouterStrategy;
import com.bree.dbrouter.strategy.impl.DBRouterStrategyHashCode;
import com.bree.dbrouter.aspect.DBRouterJoinPoint;
import com.bree.dbrouter.versionUtil.PropertyUtil;
import org.apache.ibatis.plugin.Interceptor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据源配置解析以及注册一些Bean
 */
@Configuration
public class DataSourceAutoConfig implements EnvironmentAware {

    /**
     * 数据源配置组
     * value：数  据源详细信息
     */
    private Map<String, Map<String, Object>> dataSourceMap = new HashMap<>();

    /**
     * 默认数据源配置
     */
    private Map<String, Object> defaultDataSourceMap;

    /**
     * 分库数量
     */
    private int dbCount;

    /**
     * 分表数量
     */
    private int tbCount;

    /**
     * 路由字段
     */
    private String routerKey;

    /**
     * AOP，用于分库
     * @param dbRouterConfig
     * @param dbRouterStrategy
     * @return
     */
    @Bean(name = "db-router-point")
    @ConditionalOnMissingBean
    public DBRouterJoinPoint point(DBRouterConfig dbRouterConfig, IDBRouterStrategy dbRouterStrategy) {
        return new DBRouterJoinPoint(dbRouterConfig, dbRouterStrategy);
    }

    /**
     * 将DB的信息注入到spring中，供后续获取
     * @return
     */
    @Bean
    public DBRouterConfig dbRouterConfig() {
        return new DBRouterConfig(dbCount, tbCount, routerKey);
    }

    /**
     * 配置插件bean,用于动态的决定表信息
     * @return
     */
    @Bean
    public Interceptor plugin() {
        return new DynamicMybatisPlugin();
    }

    /**
     * 用于配置 TargetDataSources 以及 DefaultTargetDataSource
     * TargetDataSources: 额外的数据源
     * 可以用指定的key获取其他的数据源来达到动态切换数据源
     * DefaultTargetDataSource: 默认的数据源
     * 如果没有要用的数据源就会使用默认的数据源
     * @return
     */
    @Bean
    public DataSource dataSource() {
        Map<Object, Object> targetDataSources = new HashMap<>();
        for (String dbInfo : dataSourceMap.keySet()) {
            Map<String, Object> objMap = dataSourceMap.get(dbInfo);
            targetDataSources.put(dbInfo, new DriverManagerDataSource(objMap.get("url").toString(), objMap.get("username").toString(), objMap.get("password").toString()));
        }

        // 设置数据源
        DynamicDataSource dynamicDataSource = new DynamicDataSource();
        dynamicDataSource.setTargetDataSources(targetDataSources);
        dynamicDataSource.setDefaultTargetDataSource(new DriverManagerDataSource(defaultDataSourceMap.get("url").toString(), defaultDataSourceMap.get("username").toString(), defaultDataSourceMap.get("password").toString()));

        return dynamicDataSource;
    }

    /**
     * 依赖注入
     * @param dbRouterConfig
     * @return
     */
    @Bean
    public IDBRouterStrategy dbRouterStrategy(DBRouterConfig dbRouterConfig) {
        return new DBRouterStrategyHashCode(dbRouterConfig);
    }

    /**
     * 配置事务
     * @param dataSource
     * @return
     */
    @Bean
    public TransactionTemplate transactionTemplate(DataSource dataSource) {
        DataSourceTransactionManager dataSourceTransactionManager = new DataSourceTransactionManager();
        dataSourceTransactionManager.setDataSource(dataSource);

        TransactionTemplate transactionTemplate = new TransactionTemplate();
        transactionTemplate.setTransactionManager(dataSourceTransactionManager);
        transactionTemplate.setPropagationBehaviorName("PROPAGATION_REQUIRED");
        return transactionTemplate;
    }

    /**
     * 读取yml中的数据源信息
     * @param environment
     */
    @Override
    public void setEnvironment(Environment environment) {
        String prefix = "db-router.jdbc.datasource.";

        dbCount = Integer.valueOf(environment.getProperty(prefix + "dbCount")); // 有多少个数据库
        tbCount = Integer.valueOf(environment.getProperty(prefix + "tbCount")); // 有多少张数据库表
        routerKey = environment.getProperty(prefix + "routerKey"); // 读取全局key，如果注解中未指明key

        // 分库分表数据源
        String dataSources = environment.getProperty(prefix + "list"); // 获取数据库列表，如db01，db02
        assert dataSources != null;
        for (String dbInfo : dataSources.split(",")) {
            //获取db01和db02的信息，如url、username、password。
            Map<String, Object> dataSourceProps = PropertyUtil.handle(environment, prefix + dbInfo, Map.class);
            // 将db01和db02数据库信息装入map集合中
            dataSourceMap.put(dbInfo, dataSourceProps);
        }

        // 默认数据源
        String defaultData = environment.getProperty(prefix + "default"); // 默认数据源，如db00
        defaultDataSourceMap = PropertyUtil.handle(environment, prefix + defaultData, Map.class);

    }

}

