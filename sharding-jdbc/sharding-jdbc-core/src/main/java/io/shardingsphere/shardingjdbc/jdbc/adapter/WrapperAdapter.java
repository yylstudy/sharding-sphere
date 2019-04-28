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

import io.shardingsphere.shardingjdbc.jdbc.adapter.invocation.JdbcMethodInvocation;
import lombok.SneakyThrows;

import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Adapter for {@code java.sql.Wrapper}.
 * 
 * @author zhangliang
 */
public abstract class WrapperAdapter implements Wrapper {
    /**存放Connection未根据sql语句类型解析前，Connection执行过的方法调用对象的集合*/
    private final Collection<JdbcMethodInvocation> jdbcMethodInvocations = new ArrayList<>();
    
    @SuppressWarnings("unchecked")
    @Override
    public final <T> T unwrap(final Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException(String.format("[%s] cannot be unwrapped as [%s]", getClass().getName(), iface.getName()));
    }
    
    @Override
    public final boolean isWrapperFor(final Class<?> iface) {
        return iface.isInstance(this);
    }
    
    /**
     * record method invocation.
     * 
     * @param targetClass target class
     * @param methodName method name
     * @param argumentTypes argument types
     * @param arguments arguments
     */
    /**
     * 记录方法的调用，这个原因是像spring事务这种会先获取Connection，并设置一些属性例如 setAutoCommit等
     * 但此时这个Connection对象是MasterSlaveConnection，此时还未解析成具体的masterConnection或者slaveConnection
     * 所以此时并不能调用具体的Connection去设置属性，所以这里只能记录了曾经对Connection操作过的方法，等到获取Statement的时候
     * sharding-jdbc会解析具体的Connection，再去调用这些方法
     * @param targetClass
     * @param methodName
     * @param argumentTypes
     * @param arguments
     */
    @SneakyThrows
    public final void recordMethodInvocation(final Class<?> targetClass, final String methodName, final Class<?>[] argumentTypes, final Object[] arguments) {
        jdbcMethodInvocations.add(new JdbcMethodInvocation(targetClass.getMethod(methodName, argumentTypes), arguments));
    }
    
    /**
     * Replay methods invocation.
     * 
     * @param target target object
     */
    public final void replayMethodsInvocation(final Object target) {
        for (JdbcMethodInvocation each : jdbcMethodInvocations) {
            each.invoke(target);
        }
    }
}
