package com.sulei.test.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import javax.inject.Named;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author sulei
 * @date 2020/6/12
 * @e-mail 776531804@qq.com
 */

public class UdfFromJar {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        EnvironmentSettings es = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, es);
//        registerUdf(tableEnv);

        KafkaTableSourceSinkFactoryBase kafkaTableSourceSinkFactoryBase = new KafkaTableSourceSinkFactory();
        StreamTableSource<Row> source = kafkaTableSourceSinkFactoryBase.createStreamTableSource(getKafkaSourceProperties());
        tableEnv.registerTableSource("input", source);

        Table table1 = tableEnv.scan("input");

//        table1 = tableEnv.sqlQuery("select imsi, upper(lac) as `en_lac`, `cell`, `proc` from " + table1);

//        table1 = table1.select("imsi, lowerCase(lac) as en_lac, cell");

//        table1 = table1.select("imsi, cell, en_lac");
        table1 = tableEnv.sqlQuery("select `imsi`, `lac`, case when imsi = '1' then '{\"key1\":\"aaa\"}' else '6,7' end as `name`, `cell` from " + table1);
        DataStream<Row> ds2 =  tableEnv.toAppendStream(table1, TypeInformation.of(Row.class));
        ds2.print("case when");

        table1 = table1.filter("");
        DataStream<Row> ds3 =  tableEnv.toAppendStream(table1, TypeInformation.of(Row.class));
        ds3.print("filter");
        table1 = table1.select("name");

        DataStream<Row> ds1 =  tableEnv.toAppendStream(table1, TypeInformation.of(Row.class));
        ds1.print("select");

//        StreamTableSink<Row> sink = kafkaTableSourceSinkFactoryBase.createStreamTableSink(getKafkaSinkProperties());
//        tableEnv.registerTableSink("output", sink);
//
//        table1.insertInto("output");

//        DataStream<Row> ds2 =  tableEnv.toAppendStream(table1, TypeInformation.of(Row.class));
//        ds2.print();

        env.execute("");
    }

    private static Map<String, String> getKafkaSourceProperties() {
        Map<String, String> properties = new HashMap<>(30);
        properties.put("schema.0.name", "imsi");
        properties.put("schema.0.type", "STRING");
        properties.put("schema.1.name", "lac");
        properties.put("schema.1.type", "STRING");
        properties.put("schema.2.name", "cell");
        properties.put("schema.2.type", "STRING");
        properties.put("schema.3.name", "proc");
        properties.put("schema.3.type", "SQL_TIMESTAMP");
        properties.put("schema.3.proctime", "true");

        properties.put("format.type", "csv");
        properties.put("format.schema", "ROW<imsi STRING, lac STRING, cell STRING>");
        properties.put("format.ignore-parse-errors", "true");
        properties.put("format.field-delimiter", ",");

        properties.put("connector.type", "kafka");
        properties.put("connector.topic", "output");
        properties.put("connector.startup-mode", "latest-offset");
        properties.put("connector.properties.0.key", "bootstrap.servers");
        properties.put("connector.properties.0.value", "10.1.236.65:6667");
        properties.put("group.id", "cep");
        return properties;
    }

    private static Map<String, String> getKafkaSinkProperties() {
        Map<String, String> properties = new HashMap<>(30);
        properties.put("schema.0.name", "imsi");
        properties.put("schema.0.type", "STRING");
        properties.put("schema.2.name", "cell");
        properties.put("schema.2.type", "STRING");
        properties.put("schema.1.name", "lac");
        properties.put("schema.1.type", "STRING");

        properties.put("format.type", "csv");
        properties.put("format.schema", "ROW<imsi STRING, lac STRING, cell STRING>");
        properties.put("format.ignore-parse-errors", "true");
        properties.put("format.field-delimiter", ",");

        properties.put("connector.type", "kafka");
        properties.put("connector.topic", "output");
        properties.put("connector.startup-mode", "latest-offset");
        properties.put("connector.properties.0.key", "bootstrap.servers");
        properties.put("connector.properties.0.value", "10.1.236.65:6667");
        properties.put("group.id", "cep");
        return properties;
    }

    public static void registerUdf(StreamTableEnvironment tableEnv) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        String udfString = "com.sulei.test.udf.HttpUdf?http://10.1.236.65:9527/";

        if (!udfString.isEmpty()) {
            String[] udfs = udfString.split(";");
            for (String udf : udfs) {
                udf = udf.trim();
                if (!udf.isEmpty()) {
                    if (udf.contains("?")) {
                        // 调用有参构造函数
                        String[] classNameAndParams = udf.split("\\?");
                        String[] params = classNameAndParams[1].split("&");

                        Class<?>[] paramTypes = new Class<?>[params.length];
                        for (int i = 0; i < params.length; i++) {
                            paramTypes[i] = String.class;
                        }

                        Class<?> udfClazz = Class.forName(classNameAndParams[0]);
                        Constructor<?> constructor = udfClazz.getConstructor(paramTypes);
                        ScalarFunction function = (ScalarFunction) constructor.newInstance(params);
                        String udfName = getUdfName(udfClazz);
                        tableEnv.registerFunction(udfName, function);
                    } else {
                        // 调用无参构造函数
                        Class<?> udfClazz = Class.forName(udf);
                        ScalarFunction function = (ScalarFunction) getUdfClassByName(udfClazz);
                        String udfName = getUdfName(udfClazz);
                        tableEnv.registerFunction(udfName, function);
                    }
                }
            }
        }
    }

    public static Object getUdfClassByName(Class<?> udfClazz) throws IllegalAccessException, InstantiationException {
        return udfClazz.newInstance();
    }

    public static String getUdfName(Class<?> udfClazz) throws ClassNotFoundException {
        Named name = udfClazz.getAnnotation(Named.class);

        if (name == null) {
            return udfClazz.getSimpleName();
        } else {
            return name.value();
        }
    }
}
