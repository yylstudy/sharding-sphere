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

package io.shardingsphere.api.algorithm.sharding;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Collection;

/**
 * Sharding value for list values.
 * 
 * @author zhangliang
 */
@RequiredArgsConstructor
@Getter
@ToString
public final class ListShardingValue<T extends Comparable<?>> implements ShardingValue {
    /**逻辑表名*/
    private final String logicTableName;
    /**分库分表依据列名*/
    private final String columnName;
    /**分库分表依据的列的值对象*/
    private final Collection<T> values;
}
