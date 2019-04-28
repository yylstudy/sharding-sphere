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

package io.shardingsphere.api.config.strategy;

import io.shardingsphere.api.algorithm.sharding.standard.PreciseShardingAlgorithm;
import io.shardingsphere.api.algorithm.sharding.standard.RangeShardingAlgorithm;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Standard strategy configuration.
 * 标准分库分表策略策略bean
 * @author zhangliang
 */
@AllArgsConstructor
@RequiredArgsConstructor
@Getter
public final class StandardShardingStrategyConfiguration implements ShardingStrategyConfiguration {
    /**分库分表的依据字段*/
    private final String shardingColumn;
    /**精确分库分表算法的实现*/
    private final PreciseShardingAlgorithm preciseShardingAlgorithm;
    /**范围分库分表算法的实现*/
    private RangeShardingAlgorithm rangeShardingAlgorithm;
}
