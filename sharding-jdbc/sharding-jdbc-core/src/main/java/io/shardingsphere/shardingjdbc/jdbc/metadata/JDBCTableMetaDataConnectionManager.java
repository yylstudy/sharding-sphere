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

package io.shardingsphere.shardingjdbc.jdbc.metadata;

import io.shardingsphere.core.metadata.table.executor.TableMetaDataConnectionManager;
import lombok.RequiredArgsConstructor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Manager of connection which for table meta data loader of JDBC.
 * Jdbc表元数据连接管理器
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class JDBCTableMetaDataConnectionManager implements TableMetaDataConnectionManager {
    /**分片的所有数据库*/
    private final Map<String, DataSource> dataSourceMap;

    /**
     * 获取数据库连接
     * @param dataSourceName data source name
     * @return
     * @throws SQLException
     */
    @Override
    public Connection getConnection(final String dataSourceName) throws SQLException {
        return dataSourceMap.get(dataSourceName).getConnection();
    }
}
