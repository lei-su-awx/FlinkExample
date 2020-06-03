package com.sulei.test.hdfs;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.RowFormatBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.time.ZoneId;
import java.util.Properties;

/**
 * @author sulei
 * @date 2020/3/23
 * @e-mail 776531804@qq.com
 */

public class OutputToHdfs {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        see.setParallelism(1);
        see.enableCheckpointing(600 * 1000);
        see.setStateBackend(new FsStateBackend("hdfs:///tmp/flink/checkpoint/"));
        EnvironmentSettings es = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment ste = StreamTableEnvironment.create(see, es);

        Properties properties = new Properties();
        properties.setProperty("group.id", "sulei_1995031512312");
        properties.setProperty("zookeeper.connect", "10.1.236.129:2181");
        properties.setProperty("bootstrap.servers", "10.1.236.129:6667");

        Kafka inputKafka = new Kafka().properties(properties).version("0.10").topic("flink_hdfs_in");
        Schema inputSchema = new Schema()
                .field("s_string", DataTypes.STRING())
                .field("s_int", DataTypes.INT())
                .field("s_float", DataTypes.FLOAT())
                .field("s_double", DataTypes.DOUBLE())
                .field("s_long", DataTypes.BIGINT());
        FormatDescriptor csv = new Csv().ignoreParseErrors();

        ste.connect(inputKafka)
                .withFormat(csv)
                .withSchema(inputSchema)
                .createTemporaryTable("input");

        Table input = ste.from("input");

        Table midTable = input.select("s_string, s_int, s_float, s_double, s_long");

        String hdfsFile = "hdfs:///tmp/flink/hdfs_output/tmp1";


        RowFormatBuilder<Row, String, ? extends RowFormatBuilder<Row, String, ?>> builder = StreamingFileSink.forRowFormat(new Path(hdfsFile), new SimpleStringEncoder<>());
        BucketAssigner<Row, String> assigner = new DateTimeBucketAssigner<>("yyyy-MM-dd--HH", ZoneId.of("Asia/Shanghai"));
        BucketAssigner<Row, String> baseAssigner = new BasePathBucketAssigner<>();
        builder.withBucketAssigner(assigner);
        // 每20秒检查一下，是否满足RollingPolicy的条件，满足的话会写文件
        builder.withBucketCheckInterval(300 * 1000);
        builder.withOutputFileConfig(new OutputFileConfig("", ""));
        // 每30秒或者文件打到128M或者间隔了Long.MAX_VALUE毫秒都没有新数据进来，就会新写一个文件
        builder.withRollingPolicy(DefaultRollingPolicy.builder().withInactivityInterval(Long.MAX_VALUE).withMaxPartSize(1024*1024*128).withRolloverInterval(300*1000).build());

        StreamingFileSink<Row> hdfsSink = builder.build();

        DataStream<Row> ds =  ste.toAppendStream(midTable, Row.class);
        ds.addSink(hdfsSink).name("sink to hdfs");
        see.execute("");
    }
}
