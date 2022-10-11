package mytest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.Set;

/**
 * @author yuyuyuyu
 */
public class Row2StreamCdcMysql {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setRestartStrategy(RestartStrategies.noRestart());
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql(
        "create table flinkrow\n"
            + "(\n"
            + "    id         int   PRIMARY KEY     ,\n"
            + "    name       string    ,\n"
            + "    age        int        ,\n"
            + "    sex        string       ,\n"
            + "    createtime timestamp(3)   ,\n"
            + "    updatetime timestamp(3)   \n"
            + ") WITH (                                   \n"
            + "    'connector' = 'mysql-cdc',               \n"
            + "    'hostname' = 'localhost'   ,             \n"
            + "    'port' = '53307'          ,              \n"
            + "    'username' = 'root'      ,               \n"
            + "    'password' = 'example'      ,            \n"
            + "    'database-name' = 'flinktest',                \n"
            + "    'table-name' = 'flinkrow'\n"
            + ")");
    Table table = tableEnv.sqlQuery("select * from flinkrow");

    DataStream<Row> rowDataStream = tableEnv.toChangelogStream(table);
    rowDataStream.map(
        new MapFunction<Row, JSONObject>() {
          @Override
          public JSONObject map(Row value) throws Exception {
            RowKind kind = value.getKind();
            System.out.println(kind);
            Set<String> fieldNames = value.getFieldNames(true);
            for (String fieldName : fieldNames) {
              System.out.println("==>" + fieldName);
            }
            Object id = value.getField("id");
            System.out.println(id);
            return null;
          }
        });
    rowDataStream.print();
    env.execute("CdcMysql");
  }
}
