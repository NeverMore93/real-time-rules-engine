package cn.doitedu.rtmk.engine.main;

import cn.doitedu.rtmk.common.pojo.UserEvent;
import cn.doitedu.rtmk.engine.functions.Json2UserEventMapFunction;
import cn.doitedu.rtmk.engine.functions.Row2RuleMetaBeanMapFunction;
import cn.doitedu.rtmk.engine.functions.RuleMatchProcessFunction;
import cn.doitedu.rtmk.engine.pojo.RuleMetaBean;
import cn.doitedu.rtmk.engine.utils.FlinkStateDescriptors;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

public class RuleEngineCDC {

    public static void main(String[] args) throws Exception {
        // 创建编程环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint");
        //env.setParallelism(2000);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        // 接收用户行为事件
        // 从kafka读入商城用户行为日志
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:59092")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setGroupId("doe-rtmk")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("rtmk-events")
                .build();

        // {"guid":1,"eventId":"e1","properties":{"p1":"v1","p2":"2"},"eventTime":100000}
        DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kfk");
        SingleOutputStreamOperator<UserEvent> userEventStream = sourceStream.map(new Json2UserEventMapFunction());

        MySqlSource<String> stringMySqlSourceBuilder = MySqlSource
                .<String>builder()
                .connectionPoolSize(20)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .hostname("localhost")
                .username("root")
                .password("example")
                .databaseList("rtmk")
                .tableList("rtmk.rule_instance_definition")
                .port(53307)
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> rule_sources = env.fromSource(stringMySqlSourceBuilder, WatermarkStrategy.noWatermarks(), "rule_sources");
        //注意这里的使用逻辑删除还是物理删除
        SingleOutputStreamOperator<RuleMetaBean> ruleMetaBeanDS = rule_sources.map(x -> {
            JSONObject jsonObject1 = JSONObject.parseObject(x);
            //获取操作类型
            String op = jsonObject1.getString("op");
            JSONObject after = jsonObject1.getJSONObject("after");
            after.put("operate_type", op);
            return after.toJavaObject(RuleMetaBean.class);
        });
        ruleMetaBeanDS.print();
        //将规则广播
        BroadcastStream<RuleMetaBean> rulemetabean = ruleMetaBeanDS.broadcast(new MapStateDescriptor<String, RuleMetaBean>("rulemetabean", String.class, RuleMetaBean.class));
        userEventStream.keyBy(UserEvent::getGuid)
                .connect(rulemetabean)
                        .process(new KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject>() {

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                            }

                            @Override
                            public void processElement(UserEvent value, KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

                            }

                            @Override
                            public void processBroadcastElement(RuleMetaBean value, KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                            }

                            @Override
                            public void onTimer(long timestamp, KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);
                            }
                        });

//        // 打印规则匹配结果
//        resultStream.print("main");
//        resultStream.getSideOutput(new OutputTag<JSONObject>("ruleStatInfo", TypeInformation.of(JSONObject.class))).print("stat");


        env.execute();
    }


}
