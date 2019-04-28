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

package io.shardingsphere.core.routing.router.masterslave;

/**
 * Master data source visited manager.
 * 
 * <p>Trace master data source visited or not in current thread.</p>
 * 
 * @author zhangliang
 */
public final class MasterVisitedManager {
    /**这个线程变量记录了曾经是否有操作过主库，这个应该就是官网上提到的
     * 同一个线程且同一个数据库连接内，如有写入操作，那么后面的读操作均从主库读取，用于保证数据的一致性*/
    private static final ThreadLocal<Boolean> MASTER_VISITED = new ThreadLocal<Boolean>() {
        
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };
    
    /**
     * Adjust master data source visited in current thread.
     * 
     * @return master data source visited or not in current thread
     */
    public static boolean isMasterVisited() {
        return MASTER_VISITED.get();
    }
    
    /**
     * Set master data source visited in current thread.
     */
    public static void setMasterVisited() {
        MASTER_VISITED.set(true);
    }
    
    /**
     * Clear master data source visited.
     */
    public static void clear() {
        MASTER_VISITED.remove();
    }
}
