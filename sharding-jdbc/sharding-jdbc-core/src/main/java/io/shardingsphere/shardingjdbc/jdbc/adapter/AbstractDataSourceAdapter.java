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

package io.shardingsphere.shardingjdbc.jdbc.adapter;

import com.google.common.base.Preconditions;
import io.shardingsphere.core.bootstrap.ShardingBootstrap;
import io.shardingsphere.core.constant.DatabaseType;
import io.shardingsphere.shardingjdbc.jdbc.unsupported.AbstractUnsupportedOperationDataSource;
import lombok.Getter;
import lombok.Setter;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.logging.Logger;

/**
 * Adapter for {@code Datasource}.
 * 
 * @author zhangliang
 * @author panjuan
 */
@Getter
@Setter
public abstract class AbstractDataSourceAdapter extends AbstractUnsupportedOperationDataSource {
    /**
     * 执行静态语句块
     */
    static {
        ShardingBootstrap.init();
    }
    /**数据库类型（这个好像获取最后一个slave库的数据库类型）*/
    private final DatabaseType databaseType;
    
    private PrintWriter logWriter = new PrintWriter(System.out);
    
    public AbstractDataSourceAdapter(final Collection<DataSource> dataSources) throws SQLException {
        databaseType = getDatabaseType(dataSources);
    }

    /**
     * 获取数据库类型
     * @param dataSources master、slave数据库实例
     * @return
     * @throws SQLException
     */
    protected final DatabaseType getDatabaseType(final Collection<DataSource> dataSources) throws SQLException {
        DatabaseType result = null;
        for (DataSource each : dataSources) {
            /**获取数据库类型*/
            DatabaseType databaseType = getDatabaseType(each);
            Preconditions.checkState(null == result || result.equals(databaseType), String.format("Database type inconsistent with '%s' and '%s'", result, databaseType));
            result = databaseType;
        }
        return result;
    }

    /**
     * 获取数据库的类型
     * @param dataSource
     * @return
     * @throws SQLException
     */
    private DatabaseType getDatabaseType(final DataSource dataSource) throws SQLException {
        /**如果当前数据库是SpringMasterSlaveDataSource，这个是AbstractDataSourceAdapter的子类，那么直接返回这个的databaseType属性*/
        if (dataSource instanceof AbstractDataSourceAdapter) {
            return ((AbstractDataSourceAdapter) dataSource).databaseType;
        }
        /**否则根据Connection获取数据库类型，可以看到目前只支持四种数据库H2、MySQL、ORACLE、SQLSERVER、PostgreSQL*/
        try (Connection connection = dataSource.getConnection()) {
            return DatabaseType.valueFrom(connection.getMetaData().getDatabaseProductName());
        }
    }
    
    @Override
    public final Logger getParentLogger() {
        return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    }
    
    @Override
    public final Connection getConnection(final String username, final String password) throws SQLException {
        return getConnection();
    }
}
