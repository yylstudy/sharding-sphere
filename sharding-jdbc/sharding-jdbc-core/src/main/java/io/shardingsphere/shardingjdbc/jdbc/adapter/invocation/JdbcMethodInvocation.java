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

package io.shardingsphere.shardingjdbc.jdbc.adapter.invocation;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.lang.reflect.Method;

/**
 * Invocation that reflected call for JDBC method.
 * Connection的方法反射调用对象
 * @author gaohongtao
 */
@RequiredArgsConstructor
public class JdbcMethodInvocation {
    /**connection要执行的方法*/
    @Getter
    private final Method method;
    /**connection要执行方法的参数对象*/
    @Getter
    private final Object[] arguments;
    
    /**
     * Invoke JDBC method.
     * 
     * @param target target object
     */
    /**
     * 调用connection的方法
     * @param target
     */
    @SneakyThrows
    public void invoke(final Object target) {
        method.invoke(target, arguments);
    }
}
