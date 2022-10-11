package mytest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yuyuyuyu
 */
public class CdcMysql {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setRestartStrategy(RestartStrategies.noRestart());
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

    MySqlSource<String> build =
        MySqlSource.<String>builder()
            .hostname("localhost")
            .port(53307)
            .username("root")
            .password("example")
            .databaseList("datahub")
            .tableList("datahub.*")
            .deserializer(new JsonDebeziumDeserializationSchema())
            .connectionPoolSize(35)
            .startupOptions(StartupOptions.initial())
            .build();
    DataStreamSource<String> streamSource =
        env.fromSource(build, WatermarkStrategy.noWatermarks(), "CdcMysql");
    // 将获取到得到json数据到tableName进行处理
    SingleOutputStreamOperator<JSONObject> map =
        streamSource.map(
            json -> {
              System.out.println(json);
              JSONObject jsonObject = JSON.parseObject(json);
              return jsonObject;
            });

    env.execute("CdcMysql");
  }
}
