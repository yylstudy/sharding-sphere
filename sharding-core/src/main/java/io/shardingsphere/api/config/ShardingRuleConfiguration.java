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

package io.shardingsphere.api.config;

import io.shardingsphere.api.config.strategy.ShardingStrategyConfiguration;
import io.shardingsphere.core.keygen.KeyGenerator;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.LinkedList;

/**
 * 分片规则配置类
 * @author zhangliang
 */
@Getter
@Setter
public final class ShardingRuleConfiguration {
    /**默认的分库数据名的名称 default null*/
    private String defaultDataSourceName;
    /**分表规则配置类*/
    private Collection<TableRuleConfiguration> tableRuleConfigs = new LinkedList<>();
    /** 需要绑定的逻辑表的字符串集合*/
    private Collection<String> bindingTableGroups = new LinkedList<>();
    /**默认的分库策略配置 default null*/
    private ShardingStrategyConfiguration defaultDatabaseShardingStrategyConfig;
    /**默认的分表策略配置 default null*/
    private ShardingStrategyConfiguration defaultTableShardingStrategyConfig;
    /**默认的主键生成器 default null*/
    private KeyGenerator defaultKeyGenerator;
    /**分库规则配置类 */
    private Collection<MasterSlaveRuleConfiguration> masterSlaveRuleConfigs = new LinkedList<>();
}
