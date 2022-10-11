package mytest;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MoreMysqlCDC {
  public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
      env.setRestartStrategy(RestartStrategies.noRestart());

      MySqlSource<String> build2 = MySqlSource.<String>builder()
              .hostname("localhost")
              .port(53307)
              .username("root")
              .password("example")
              .databaseList("datahub")
              .tableList("datahub.metadata_aspect_v2")
              .deserializer(new JsonDebeziumDeserializationSchema())
              .startupOptions(StartupOptions.initial())
              .connectionPoolSize(50)
              .build();
      DataStreamSource<String> moremysql = env.fromSource(build2, WatermarkStrategy.noWatermarks(), "moremysql");
      moremysql.map(x->{
          System.out.println(x);
          return null;
      });
      moremysql.print("==>");
      env.execute("moremysql");
  }
}
