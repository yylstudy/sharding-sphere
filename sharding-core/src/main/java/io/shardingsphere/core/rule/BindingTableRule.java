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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.shardingsphere.core.exception.ShardingConfigurationException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.List;

/**
 * Binding table rule configuration.
 * 逻辑绑定表的分片规则
 * <p>Binding table is same sharding rule with different tables, use one of them can deduce other name of actual tables and data sources.</p>
 * 
 * @author zhangliang
 */
@RequiredArgsConstructor
@Getter
public final class BindingTableRule {
    /**绑定表中具体逻辑表的分片规则*/
    private final List<TableRule> tableRules;
    
    /**
     * Adjust contains this logic table in this rule.
     * 
     * @param logicTableName logic table name
     * @return contains this logic table or not
     */
    public boolean hasLogicTable(final String logicTableName) {
        for (TableRule each : tableRules) {
            if (each.getLogicTable().equals(logicTableName.toLowerCase())) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Deduce actual table name from other actual table name in same binding table rule.
     * 获取绑定表的数据节点，主要就是获取逻辑表的数据节点在分表规则中的位置，再根据这个位置查找绑定表中对应下标的数据节点
     * @param dataSource data source name
     * @param logicTable 要查找的绑定逻辑表的名称
     * @param otherActualTable 逻辑表的真正数据节点的名字 t_order0
     * @return actual table name
     */
    public String getBindingActualTable(final String dataSource, final String logicTable, final String otherActualTable) {
        int index = -1;
        for (TableRule each : tableRules) {
            index = each.findActualTableIndex(dataSource, otherActualTable);
            if (-1 != index) {
                break;
            }
        }
        if (-1 == index) {
            throw new ShardingConfigurationException("Actual table [%s].[%s] is not in table config", dataSource, otherActualTable);
        }
        for (TableRule each : tableRules) {
            if (each.getLogicTable().equals(logicTable.toLowerCase())) {
                return each.getActualDataNodes().get(index).getTableName().toLowerCase();
            }
        }
        throw new ShardingConfigurationException("Cannot find binding actual table, data source: %s, logic table: %s, other actual table: %s", dataSource, logicTable, otherActualTable);
    }
    
    Collection<String> getAllLogicTables() {
        return Lists.transform(tableRules, new Function<TableRule, String>() {
            
            @Override
            public String apply(final TableRule input) {
                return input.getLogicTable().toLowerCase();
            }
        });
    }
}
