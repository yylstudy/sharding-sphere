/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.shardingjdbc.jdbc.core.datasource;

import io.shardingsphere.api.ConfigMapContext;
import io.shardingsphere.api.config.MasterSlaveRuleConfiguration;
import io.shardingsphere.core.constant.properties.ShardingProperties;
import io.shardingsphere.core.constant.properties.ShardingPropertiesConstant;
import io.shardingsphere.core.rule.MasterSlaveRule;
import io.shardingsphere.shardingjdbc.jdbc.adapter.AbstractDataSourceAdapter;
import io.shardingsphere.shardingjdbc.jdbc.core.connection.MasterSlaveConnection;
import lombok.Getter;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

/**
 * Database that support master-slave.
 *
 * @author zhangliang
 * @author panjuan
 */
@Getter
public class MasterSlaveDataSource extends AbstractDataSourceAdapter implements AutoCloseable {
    /**master、slave的beanName和其DataSource的映射集合*/
    private final Map<String, DataSource> dataSourceMap;
    /**主从规则配置类MasterSlaveRule*/
    private final MasterSlaveRule masterSlaveRule;
    /**props标签的Properties类*/
    private final ShardingProperties shardingProperties;

    /**
     * SpringMasterSlaveDataSource的父类构造器
     * @param dataSourceMap
     * @param masterSlaveRuleConfig
     * @param configMap
     * @param props
     * @throws SQLException
     */
    public MasterSlaveDataSource(final Map<String, DataSource> dataSourceMap, final MasterSlaveRuleConfiguration masterSlaveRuleConfig,
                                 final Map<String, Object> configMap, final Properties props) throws SQLException {
        super(getAllDataSources(dataSourceMap, masterSlaveRuleConfig.getMasterDataSourceName(), masterSlaveRuleConfig.getSlaveDataSourceNames()));
        if (!configMap.isEmpty()) {
            ConfigMapContext.getInstance().getMasterSlaveConfig().putAll(configMap);
        }
        this.dataSourceMap = dataSourceMap;
        this.masterSlaveRule = new MasterSlaveRule(masterSlaveRuleConfig);
        shardingProperties = new ShardingProperties(null == props ? new Properties() : props);
    }
    
    public MasterSlaveDataSource(final Map<String, DataSource> dataSourceMap, final MasterSlaveRule masterSlaveRule,
                                 final Map<String, Object> configMap, final ShardingProperties props) throws SQLException {
        super(getAllDataSources(dataSourceMap, masterSlaveRule.getMasterDataSourceName(), masterSlaveRule.getSlaveDataSourceNames()));
        if (!configMap.isEmpty()) {
            ConfigMapContext.getInstance().getMasterSlaveConfig().putAll(configMap);
        }
        this.dataSourceMap = dataSourceMap;
        this.masterSlaveRule = masterSlaveRule;
        this.shardingProperties = props;
    }

    /**
     * 获取所有的数据库实例
     * @param dataSourceMap master、slave的beanName和其Datasource的映射集合
     * @param masterDataSourceName
     * @param slaveDataSourceNames
     * @return
     */
    private static Collection<DataSource> getAllDataSources(final Map<String, DataSource> dataSourceMap, final String masterDataSourceName, final Collection<String> slaveDataSourceNames) {
        Collection<DataSource> result = new LinkedList<>();
        result.add(dataSourceMap.get(masterDataSourceName));
        for (String each : slaveDataSourceNames) {
            result.add(dataSourceMap.get(each));
        }
        return result;
    }
    
    /**
     * Get map of all actual data source name and all actual data sources.
     *
     * @return map of all actual data source name and all actual data sources
     */
    public Map<String, DataSource> getAllDataSources() {
        Map<String, DataSource> result = new HashMap<>(masterSlaveRule.getSlaveDataSourceNames().size() + 1, 1);
        result.put(masterSlaveRule.getMasterDataSourceName(), getDataSourceMap().get(masterSlaveRule.getMasterDataSourceName()));
        for (String each : masterSlaveRule.getSlaveDataSourceNames()) {
            result.put(each, getDataSourceMap().get(each));
        }
        return result;
    }
    
    private void closeOriginalDataSources() {
        for (DataSource each : getDataSourceMap().values()) {
            try {
                Method closeMethod = each.getClass().getDeclaredMethod("close");
                closeMethod.invoke(each);
            } catch (final ReflectiveOperationException ignored) {
            }
        }
    }

    /**
     * 数据库读写分离最重要的就是需要重写SpringMasterSlaveDataSource的getConnection方法
     * @return
     */
    @Override
    public final MasterSlaveConnection getConnection() {
        /**创建一个MasterSlaveConnection*/
        return new MasterSlaveConnection(this);
    }
    
    @Override
    public final void close() {
        closeOriginalDataSources();
    }
    
    /**
     * Show SQL or not.
     *
     * @return show SQL or not
     */
    public boolean showSQL() {
        return shardingProperties.getValue(ShardingPropertiesConstant.SQL_SHOW);
    }
}

