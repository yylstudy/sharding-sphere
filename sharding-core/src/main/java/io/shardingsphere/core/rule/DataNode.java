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

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import io.shardingsphere.core.exception.ShardingConfigurationException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.List;

/**
 * Sharding data unit node.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
@Getter
@ToString
public final class DataNode {
    
    private static final String DELIMITER = ".";
    /**数据库的名称，注意这个数据库是主从配置后的数据库名称*/
    private final String dataSourceName;
    /**表名*/
    private final String tableName;
    
    /**
     * Constructs a data node with well-formatted string.
     *
     * @param dataNode string of data node. use {@code .} to split data source name and table name.
     */
    public DataNode(final String dataNode) {
        if (!isValidDataNode(dataNode)) {
            throw new ShardingConfigurationException("Invalid format for actual data nodes: '%s'", dataNode);
        }
        List<String> segments = Splitter.on(DELIMITER).splitToList(dataNode);
        dataSourceName = segments.get(0);
        tableName = segments.get(1);
    }
    
    private static boolean isValidDataNode(final String dataNodeStr) {
        return dataNodeStr.contains(DELIMITER) && 2 == Splitter.on(DELIMITER).splitToList(dataNodeStr).size();
    }
    
    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (null == object || getClass() != object.getClass()) {
            return false;
        }
        DataNode dataNode = (DataNode) object;
        return Objects.equal(this.dataSourceName.toUpperCase(), dataNode.dataSourceName.toUpperCase())
            && Objects.equal(this.tableName.toUpperCase(), dataNode.tableName.toUpperCase());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(dataSourceName.toUpperCase(), tableName.toUpperCase());
    }
}
