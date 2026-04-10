package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.internal.streaming.DirectTopicPartitionChannel;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Proxy;

/**
 * Intercept invocations to {@link SnowflakeConnectionService} and assert that the 'tableName' argument always
 * contains the schema name. Print an error message if the Schema is missing.
 */
public class SchemaChecker implements InvocationHandler {

    private final SnowflakeConnectionService jdbcConnection;

    public static SnowflakeConnectionService createProxy(SnowflakeConnectionService target){
        return (SnowflakeConnectionService) Proxy.newProxyInstance(SchemaChecker.class.getClassLoader(),
                new Class<?>[]{SnowflakeConnectionService.class}, new SchemaChecker(target) );
    }

    private SchemaChecker(SnowflakeConnectionService jdbcConnection) {
        this.jdbcConnection = jdbcConnection;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        boolean ignore = args == null || args.length == 0 || method.getName().equals("migrateStreamingChannelOffsetToken");
        if (ignore){
            // 'migrateStreamingChannelOffsetToken' has an explicit schema argument so ignore it
            return method.invoke(jdbcConnection, args);
        }
        Parameter[] parameters = method.getParameters();
        int tableNameArgIndex = getTableNameArgIndex(parameters);
        if (tableNameArgIndex != -1) {
            String tableName = (String) args[tableNameArgIndex];
            if (!tableName.contains(".")) {
                // it does not contain the schema prefix so print an error message
                LOGGER.error("This is probably a bug for Multischema support. The tableName parameter did not contain a schema: " + tableName + " for method: " + method);
            } else {
                // TODO disable this after initial testing
                LOGGER.info("Schema is in tableName: " + tableName + " for method: " + method);
            }
        } else {
            // TODO disable this after initial testing
            LOGGER.info("No schema check happening for method: " + method);
        }
        return method.invoke(jdbcConnection, args);
    }

    private int getTableNameArgIndex(Parameter[] parameters) {
        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];
            // for this to work the compiler must have "-parameters" set in the java compiler args - see pom.xml maven-compiler-plugin
            if (parameter.getName().equals("tableName") && parameter.getType() == String.class) {
                return i;
            }
        }
        return -1;
    }

    // use this logger because I know it definitely gets printed in aws cloudwatch
    private static final KCLogger LOGGER = new KCLogger(DirectTopicPartitionChannel.class.getName());
}
