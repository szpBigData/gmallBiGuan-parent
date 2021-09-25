package com.atguigu.gmall.realtime.app.dws2;


import com.atguigu.gmall.realtime.app.common.GmallConstant;
import com.atguigu.gmall.realtime.app.func.KeywordUDTF;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author sunzhipeng
 * @create 2021-08-30 23:35
 */
public class KeywordStatspp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop103:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","test");
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv= StreamTableEnvironment.create(env, settings);
        //注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //3.2建表
        //3.1 声明主题以及消费者组
        String pageViewSourceTopic = "dwd_page_log";
        String groupId = "keywordstats_app_group";
        //3.2建表
        tableEnv.executeSql(
                "CREATE TABLE page_view (" +
                        " common MAP<STRING, STRING>," +
                        " page MAP<STRING, STRING>," +
                        " ts BIGINT," +
                        " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                        " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                        " WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")"
        );
        //TODO 4.从动态表中查询数据  --->尚硅谷大数据数仓-> [尚, 硅谷, 大, 数据, 数, 仓]
        Table fullwordTable = tableEnv.sqlQuery(
                "select page['item'] fullword,rowtime " +
                        " from page_view " +
                        " where page['page_id']='good_list' and page['item'] IS NOT NULL"
        );
        //TODO 5.利用自定义函数  对搜索关键词进行拆分
        Table keywordTable = tableEnv.sqlQuery(
                "SELECT keyword, rowtime " +
                        "FROM  " + fullwordTable + "," +
                        "LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)"
        );
        //TODO 6.分组、开窗、聚合
        Table reduceTable = tableEnv.sqlQuery(
                "select keyword,count(*) ct,  '" + GmallConstant.KEYWORD_SEARCH + "' source," +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                        "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt ," +
                        "UNIX_TIMESTAMP()*1000 ts from " + keywordTable +
                        " group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword"
        );
        //转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(reduceTable, KeywordStats.class);
        //写入到CK
        keywordStatsDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)")
        );
        env.execute();
    }
}
