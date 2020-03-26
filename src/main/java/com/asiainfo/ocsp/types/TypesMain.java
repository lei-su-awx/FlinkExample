package com.asiainfo.ocsp.types;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;

import java.util.Properties;

/**
 * @author sulei
 * @date 2020/1/19
 * @e-mail 776531804@qq.com
 */

public class TypesMain {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        EnvironmentSettings es = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment ste = StreamTableEnvironment.create(see, es);

        // Input
        String[] fields = new String[] {"1", "2", "3", "4", "5"};
        DataType[] types = new DataType[] {DataTypes.STRING(), DataTypes.INT(), DataTypes.BIGINT(), DataTypes.FLOAT(), DataTypes.DOUBLE()};
        TableSchema inputSchema = TableSchema.builder().fields(fields, types).build();
        Properties properties = new Properties();
        properties.setProperty("group.id", "sulei");
        properties.setProperty("bootstrap.servers", "10.1.236.139:6667,10.1.236.143:6667,10.1.236.78:6667");
        TypeInformation[] typeInfos = new TypeInformation[] {Types.STRING, Types.INT, Types.LONG, Types.FLOAT, Types.DOUBLE};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInfos, fields);
        CsvRowDeserializationSchema csvDeserializationSchema = new CsvRowDeserializationSchema.Builder(rowTypeInfo).setQuoteCharacter(' ').build();
        KafkaTableSource kafkaSource = new KafkaTableSource(inputSchema, "sulei_in", properties, csvDeserializationSchema);
        Table source = ste.fromTableSource(kafkaSource);

        // Group
        GroupWindowedTable windowTable = source.window(Tumble.over("30").on("").as(""));
        WindowGroupedTable groupedTable = windowTable.groupBy("1");
        Table finalTable = groupedTable.select("1, avg(2) as c2, avg(3) as c3, avg(4) as c4, avg(5) as c5");

        // Sink
        CsvTableSink sink = new CsvTableSink("./output", "--", -1, FileSystem.WriteMode.OVERWRITE);
        TableSink csvTable = sink.configure(finalTable.getSchema().getFieldNames(), finalTable.getSchema().getFieldTypes());
        ste.registerTableSink("output", csvTable);
        finalTable.insertInto("output");

        //Execute
        ste.execute("task");
    }
}
