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

import com.google.common.base.Preconditions;
import io.shardingsphere.api.ConfigMapContext;
import io.shardingsphere.core.constant.properties.ShardingProperties;
import io.shardingsphere.core.constant.properties.ShardingPropertiesConstant;
import io.shardingsphere.core.executor.ShardingExecuteEngine;
import io.shardingsphere.core.rule.ShardingRule;
import io.shardingsphere.shardingjdbc.jdbc.adapter.AbstractDataSourceAdapter;
import io.shardingsphere.shardingjdbc.jdbc.core.ShardingContext;
import io.shardingsphere.shardingjdbc.jdbc.core.connection.ShardingConnection;
import lombok.Getter;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Database that support sharding.
 *
 * @author zhangliang
 * @author zhaojun
 * @author panjuan
 */
@Getter
public class ShardingDataSource extends AbstractDataSourceAdapter implements AutoCloseable {
    /**用于分库的所有数据库的beanName->DataSource的映射集合*/
    private final Map<String, DataSource> dataSourceMap;
    /**sharding的上下文*/
    private final ShardingContext shardingContext;
    /**props标签对象*/
    private final ShardingProperties shardingProperties;
    
    public ShardingDataSource(final Map<String, DataSource> dataSourceMap, final ShardingRule shardingRule) throws SQLException {
        this(dataSourceMap, shardingRule, new ConcurrentHashMap<String, Object>(), new Properties());
    }

    /**
     * SpringShardingDataSource的父类构造
     * @param dataSourceMap 用于分库的所有数据库的beanName->DataSource的映射集合
     * @param shardingRule 分库分表规则类
     * @param configMap
     * @param props
     * @throws SQLException
     */
    public ShardingDataSource(final Map<String, DataSource> dataSourceMap, final ShardingRule shardingRule, final Map<String, Object> configMap, final Properties props) throws SQLException {
        super(dataSourceMap.values());
        /**校验用于分库分表的所有数据库不是MasterSlaveDataSource*/
        checkDataSourceType(dataSourceMap);
        if (!configMap.isEmpty()) {
            ConfigMapContext.getInstance().getShardingConfig().putAll(configMap);
        }
        this.dataSourceMap = dataSourceMap;
        this.shardingProperties = new ShardingProperties(null == props ? new Properties() : props);
        /**分片上下文*/
        this.shardingContext = getShardingContext(shardingRule);
    }
    
    public ShardingDataSource(final Map<String, DataSource> dataSourceMap, final ShardingContext shardingContext, final ShardingProperties shardingProperties) throws SQLException {
        super(dataSourceMap.values());
        this.dataSourceMap = dataSourceMap;
        this.shardingContext = shardingContext;
        this.shardingProperties = shardingProperties;
    }
    
    private void checkDataSourceType(final Map<String, DataSource> dataSourceMap) {
        for (DataSource each : dataSourceMap.values()) {
            Preconditions.checkArgument(!(each instanceof MasterSlaveDataSource), "Initialized data sources can not be master-slave data sources.");
        }
    }

    /**
     *  获取分片上下文
     * @param shardingRule
     * @return
     * @throws SQLException
     */
    private ShardingContext getShardingContext(final ShardingRule shardingRule) throws SQLException {
        /**执行器大小，这个应该是线程数*/
        int executorSize = shardingProperties.getValue(ShardingPropertiesConstant.EXECUTOR_SIZE);
        /**这个属性表示一次查询时每个数据库所允许使用的最大连接数*/
        int maxConnectionsSizePerQuery = shardingProperties.getValue(ShardingPropertiesConstant.MAX_CONNECTIONS_SIZE_PER_QUERY);
        boolean showSQL = shardingProperties.getValue(ShardingPropertiesConstant.SQL_SHOW);
        return new ShardingContext(dataSourceMap, shardingRule, getDatabaseType(), new ShardingExecuteEngine(executorSize), maxConnectionsSizePerQuery, showSQL);
    }

    /**
     * 重写Jdbc的获取数据库连接
     * @return
     */
    @Override
    public final ShardingConnection getConnection() {
        return new ShardingConnection(dataSourceMap, shardingContext);
    }
    
    @Override
    public final void close() {
        closeOriginalDataSources();
        shardingContext.close();
    }
    
    private void closeOriginalDataSources() {
        for (DataSource each : dataSourceMap.values()) {
            try {
                each.getClass().getDeclaredMethod("close").invoke(each);
            } catch (final ReflectiveOperationException ignored) {
            }
        }
    }
}
