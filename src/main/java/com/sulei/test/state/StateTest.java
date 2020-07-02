package com.sulei.test.state;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author sulei
 * @date 2020/4/14
 * @e-mail 776531804@qq.com
 */

public class StateTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings es = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, es);

        Properties properties = new Properties();
        properties.setProperty("group.id", "sulei_1995031512312");
        properties.setProperty("zookeeper.connect", "10.1.236.64:2181");
        properties.setProperty("bootstrap.servers", "10.1.236.64:6667");

        Kafka inputKafka = new Kafka().properties(properties).version("universal").topic("dxcs_flink_1");
        Schema inputSchema = new Schema()
                .field("user_id", Types.STRING())
                .field("area", Types.STRING())
                .field("timestamp", Types.SQL_TIMESTAMP());
        FormatDescriptor csv = new Csv().ignoreParseErrors();

        tableEnv.connect(inputKafka)
                .withFormat(csv)
                .withSchema(inputSchema)
                .createTemporaryTable("input");

        Table input = tableEnv.from("input");

        Table midTable = input.select("user_id, area, timestamp");

        RowTypeInfo rowTypeInfo = new RowTypeInfo(midTable.getSchema().getFieldTypes(), midTable.getSchema().getFieldNames());
        String[] primaryKeys = "user_id".split(",");
        String areaKey = "area";
        String regionDelim = "|";
        String timeKey = "timestamp";
        String timeFieldFormat = "yyyy-MM-hh'T'HH:mm:ss.SSS";
        SimpleDateFormat sdf = new SimpleDateFormat(timeFieldFormat);
        // 五分钟超时
        long allowTimeInterval = Long.parseLong("300") * 1000;

        DataStream<Row> ds = tableEnv.toAppendStream(midTable, rowTypeInfo);

        KeyedStream<Row, Tuple> keyedStream = ds.keyBy(primaryKeys);
        DataStream<Row> stayTimeRow = keyedStream.map(new RichMapFunction<Row, Row>() {
            private MapState<String, Tuple2<Long, Long>> stayAreaTimeMap;

            @Override
            public void open(Configuration parameters) {
                StateTtlConfig stateTtlConfig = StateTtlConfig
                        .newBuilder(Time.seconds(300))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();

                MapStateDescriptor<String, Tuple2<Long, Long>> mapStateDescriptor =
                        new MapStateDescriptor<>(
                                "stayAreaTimeMap",
                                TypeInformation.of(new TypeHint<String>() {
                                }),
                                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                                })
                        );
                mapStateDescriptor.enableTimeToLive(stateTtlConfig);
                stayAreaTimeMap = getRuntimeContext().getMapState(mapStateDescriptor);
            }

            @Override
            public Row map(Row value) throws Exception {
                String[] areaFields;
                Row result;
                int areaFieldIndex = rowTypeInfo.getFieldIndex("area");
                String areaFieldsWithDelim = String.valueOf(value.getField(areaFieldIndex));
                areaFields = StringUtils.split(areaFieldsWithDelim, regionDelim);

                String curTime = String.valueOf(value.getField(rowTypeInfo.getFieldIndex(timeKey)));
                long curTimeLong = sdf.parse(curTime).getTime();
                StringBuilder stayTimeBuilder = new StringBuilder();

                for (String innerAreaField : areaFields) {
                    Tuple2<Long, Long> timeInfo = stayAreaTimeMap.get(innerAreaField);
                    if (timeInfo != null) {
                        long firstArriveTime = timeInfo.f0;
                        long lastArriveTime = timeInfo.f1;
                        if (curTimeLong >= lastArriveTime) {
                            // 数据未乱序
                            if (curTimeLong - lastArriveTime > allowTimeInterval) {
                                // 数据超时，替换f1，输出驻留为0
                                stayTimeBuilder.append("0").append(regionDelim);
                                stayAreaTimeMap.put(innerAreaField, Tuple2.of(curTimeLong, curTimeLong));
                            } else {
                                // 数据正常，输出curTimeLong - firstArriveTime，替换lastArriveTime
                                stayTimeBuilder.append((curTimeLong - firstArriveTime)).append(regionDelim);
                                stayAreaTimeMap.put(innerAreaField, Tuple2.of(firstArriveTime, curTimeLong));
                            }
                        } else {
                            // 数据乱序
                            stayTimeBuilder.append("0").append(regionDelim);
                            if (curTimeLong < firstArriveTime) {
                                // 很乱的乱序，将curTimeLong更新为firstArriveTime，输出驻留为0
                                stayAreaTimeMap.put(innerAreaField, Tuple2.of(curTimeLong, lastArriveTime));
                            }
                        }
                    } else {
                        stayTimeBuilder.append("0").append(regionDelim);
                        stayAreaTimeMap.put(innerAreaField, Tuple2.of(curTimeLong, curTimeLong));
                    }
                }
                result = createRowWithStayTime(value, stayTimeBuilder.substring(0, stayTimeBuilder.length() - 1));
                return result;
            }
        });

        String[] stayTimeRowFieldNames = new String[rowTypeInfo.getArity() + 2];
        TypeInformation<?>[] stayTimeRowFieldTypes = new TypeInformation[rowTypeInfo.getArity() + 2];
        int i = 0;
        for (String fieldName : rowTypeInfo.getFieldNames()) {
            stayTimeRowFieldNames[i] = fieldName;
            stayTimeRowFieldTypes[i] = rowTypeInfo.getTypeAt(i);
            i++;
        }

        final int areaTimeToSplitIndex = i;
        final int areaSplitIndex = rowTypeInfo.getFieldIndex(areaKey);
        DataStream<Row> stayTimeSplit = stayTimeRow.flatMap((FlatMapFunction<Row, Row>) (input1, out) -> {
            String[] areaTimeSplit = StringUtils.split(String.valueOf(input1.getField(areaTimeToSplitIndex)), regionDelim);
            String[] areaSplit = StringUtils.split(String.valueOf(input1.getField(areaSplitIndex)), regionDelim);
            for (int i1 = 0; i1 < areaSplit.length; i1++) {
                Row result = createRowWithStayTime(input1, "");
                String time = areaTimeSplit[i1];
                String area = areaSplit[i1];
                result.setField(areaTimeToSplitIndex, time);
                result.setField(result.getArity() - 1, area);
                out.collect(result);
            }
        });

        stayTimeRowFieldNames[i] = "stayTime";
        stayTimeRowFieldTypes[i] = Types.STRING();
        stayTimeRowFieldNames[i + 1] = "stayArea";
        stayTimeRowFieldTypes[i + 1] = Types.STRING();
        RowTypeInfo newRowTypeInfo = new RowTypeInfo(stayTimeRowFieldTypes, stayTimeRowFieldNames);
        stayTimeSplit.getTransformation().setOutputType(newRowTypeInfo);

        Table stayTimeTable = tableEnv.fromDataStream(stayTimeSplit);

        Table newTable = stayTimeTable.select("user_id, area, timestamp, stayTime, stayArea");

        DataStream<Row> ds1 = tableEnv.toAppendStream(newTable, newRowTypeInfo);
        ds1.print();

        env.execute("");
    }

    private static Row createRowWithStayTime(Row row, String stayTime) {
        Row result = new Row(row.getArity() + 1);
        int i = 0;
        for (; i < row.getArity(); i++) {
            result.setField(i, row.getField(i));
        }

        result.setField(i, stayTime);
        return result;
    }

}
