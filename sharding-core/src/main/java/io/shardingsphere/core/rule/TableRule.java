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

package io.shardingsphere.core.rule;

import com.google.common.base.Preconditions;
import io.shardingsphere.api.config.TableRuleConfiguration;
import io.shardingsphere.core.exception.ShardingException;
import io.shardingsphere.core.keygen.KeyGenerator;
import io.shardingsphere.core.routing.strategy.ShardingStrategy;
import io.shardingsphere.core.routing.strategy.ShardingStrategyFactory;
import io.shardingsphere.core.util.InlineExpressionParser;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Table rule configuration.
 *
 * @author zhangliang
 */
@Getter
@ToString(exclude = "dataNodeIndexMap")
public final class TableRule {
    /**逻辑表名*/
    private final String logicTable;
    /**实际的表名的数据节点对象*/
    private final List<DataNode> actualDataNodes;
    /**实际的表名和其下标的映射集合*/
    @Getter(AccessLevel.NONE)
    private final Map<DataNode, Integer> dataNodeIndexMap;
    /**分库策略实现 StandardShardingStrategy*/
    private final ShardingStrategy databaseShardingStrategy;
    /**分表策略的实现 StandardShardingStrategy*/
    private final ShardingStrategy tableShardingStrategy;
    /**主键名称*/
    private final String generateKeyColumn;
    /**主键生成器*/
    private final KeyGenerator keyGenerator;
    /**逻辑索引*/
    private final String logicIndex;
    
    public TableRule(final String defaultDataSourceName, final String logicTableName) {
        logicTable = logicTableName.toLowerCase();
        actualDataNodes = Collections.singletonList(new DataNode(defaultDataSourceName, logicTableName));
        dataNodeIndexMap = Collections.emptyMap();
        databaseShardingStrategy = null;
        tableShardingStrategy = null;
        generateKeyColumn = null;
        keyGenerator = null;
        logicIndex = null;
    }
    public static void main(String[] args){
        List<String> dataNodes = new InlineExpressionParser("ds_ms${0..1}.t_order${0..1}").splitAndEvaluate();
        System.out.println(dataNodes);
    }

    /**
     * 创建一个分表规则对象
     * @param tableRuleConfig
     * @param shardingDataSourceNames
     */
    public TableRule(final TableRuleConfiguration tableRuleConfig, final ShardingDataSourceNames shardingDataSourceNames) {
        Preconditions.checkNotNull(tableRuleConfig.getLogicTable(), "Logic table cannot be null.");
        logicTable = tableRuleConfig.getLogicTable().toLowerCase();
        /**表名节点集合，这个应该是databaseName.tableName的集合*/
        List<String> dataNodes = new InlineExpressionParser(tableRuleConfig.getActualDataNodes()).splitAndEvaluate();
        dataNodeIndexMap = new HashMap<>(dataNodes.size(), 1);
        /**实际的数据节点*/
        actualDataNodes = isEmptyDataNodes(dataNodes)
            ? generateDataNodes(tableRuleConfig.getLogicTable(), shardingDataSourceNames.getDataSourceNames())
                : generateDataNodes(dataNodes, shardingDataSourceNames.getDataSourceNames());
        /**分库策略*/
        databaseShardingStrategy = null == tableRuleConfig.getDatabaseShardingStrategyConfig() ? null : ShardingStrategyFactory.newInstance(tableRuleConfig.getDatabaseShardingStrategyConfig());
        tableShardingStrategy = null == tableRuleConfig.getTableShardingStrategyConfig() ? null : ShardingStrategyFactory.newInstance(tableRuleConfig.getTableShardingStrategyConfig());
        generateKeyColumn = tableRuleConfig.getKeyGeneratorColumnName();
        keyGenerator = tableRuleConfig.getKeyGenerator();
        logicIndex = null == tableRuleConfig.getLogicIndex() ? null : tableRuleConfig.getLogicIndex().toLowerCase();
    }
    
    private boolean isEmptyDataNodes(final List<String> dataNodes) {
        return null == dataNodes || dataNodes.isEmpty();
    }
    
    private List<DataNode> generateDataNodes(final String logicTable, final Collection<String> dataSourceNames) {
        List<DataNode> result = new LinkedList<>();
        int index = 0;
        for (String each : dataSourceNames) {
            DataNode dataNode = new DataNode(each, logicTable);
            result.add(dataNode);
            dataNodeIndexMap.put(dataNode, index);
            index++;
        }
        return result;
    }

    /**
     * 生成数据节点
     * @param actualDataNodes 解析出来的所有的实际表名集合
     * @param dataSourceNames 主从配置后的dataSource的名称
     * @return
     */
    private List<DataNode> generateDataNodes(final List<String> actualDataNodes, final Collection<String> dataSourceNames) {
        List<DataNode> result = new LinkedList<>();
        int index = 0;
        for (String each : actualDataNodes) {
            DataNode dataNode = new DataNode(each);
            /**如果不包含数据节点的dataSource，抛出异常*/
            if (!dataSourceNames.contains(dataNode.getDataSourceName())) {
                throw new ShardingException("Cannot find data source in sharding rule, invalid actual data node is: '%s'", each);
            }
            result.add(dataNode);
            dataNodeIndexMap.put(dataNode, index);
            index++;
        }
        return result;
    }
    
    /**
     * 将表的数据节点以库分组（同一个库的表分表）
     *
     * @return data node groups, key is data source name, value is tables belong to this data source
     */
    public Map<String, List<DataNode>> getDataNodeGroups() {
        Map<String, List<DataNode>> result = new LinkedHashMap<>(actualDataNodes.size(), 1);
        for (DataNode each : actualDataNodes) {
            String dataSourceName = each.getDataSourceName();
            if (!result.containsKey(dataSourceName)) {
                result.put(dataSourceName, new LinkedList<DataNode>());
            }
            result.get(dataSourceName).add(each);
        }
        return result;
    }
    
    /**
     * Get actual data source names.
     *
     * @return actual data source names
     */
    public Collection<String> getActualDatasourceNames() {
        Collection<String> result = new LinkedHashSet<>(actualDataNodes.size());
        for (DataNode each : actualDataNodes) {
            result.add(each.getDataSourceName());
        }
        return result;
    }
    
    /**
     * Get actual table names via target data source name.
     *
     * @param targetDataSource target data source name
     * @return names of actual tables
     */
    public Collection<String> getActualTableNames(final String targetDataSource) {
        Collection<String> result = new LinkedHashSet<>(actualDataNodes.size());
        for (DataNode each : actualDataNodes) {
            if (targetDataSource.equals(each.getDataSourceName())) {
                result.add(each.getTableName());
            }
        }
        return result;
    }
    
    int findActualTableIndex(final String dataSourceName, final String actualTableName) {
        DataNode dataNode = new DataNode(dataSourceName, actualTableName);
        return dataNodeIndexMap.containsKey(dataNode) ? dataNodeIndexMap.get(dataNode) : -1;
    }
    
    boolean isExisted(final String actualTableName) {
        for (DataNode each : actualDataNodes) {
            if (each.getTableName().equalsIgnoreCase(actualTableName)) {
                return true;
            }
        }
        return false;
    }
}
