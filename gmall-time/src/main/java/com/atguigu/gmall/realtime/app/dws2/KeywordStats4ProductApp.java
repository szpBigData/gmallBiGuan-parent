package com.atguigu.gmall.realtime.app.dws2;
import com.atguigu.gmall.realtime.app.udf.KeywordProductC2RUDTF;
import com.atguigu.gmall.realtime.app.udf.KeywordUDTF;
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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author sunzhipeng
 * @create 2021-08-31 20:10
 */
public class KeywordStats4ProductApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop103:8020/gmall/flink/checkpoint/ProvinceStatsSqlApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","test");
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        tableEnvironment.createTemporarySystemFunction("ik_analyze",  KeywordUDTF.class);
        tableEnvironment.createTemporarySystemFunction("keywordProductC2R",  KeywordProductC2RUDTF.class);

        //TODO 3.将数据源定义为动态表
        String groupId = "keyword_stats_app";
        String productStatsSourceTopic ="dws_product_stats";

        tableEnvironment.executeSql("create table product_stats(" +
                "spu_name string," +
                "click_ct bigint," +
                "cart_ct bigint," +
                "order_ct bigint," +
                "stt string," +
                "edt string)" +
                "with ("+ MyKafkaUtil.getKafkaDDL(productStatsSourceTopic,groupId)+")");
        //聚合计算
        Table keywordStatsProduct = tableEnvironment.sqlQuery("" +
                "select" +
                "keyword," +
                "ct," +
                "source," +
                "date_format(stt,'yyyy-MM-dd HH:mm:ss') stt," +
                "date_format(edt,'yyyy-MM-dd HH:mm:ss') as edt," +
                "unix_timestamp()*1000 ts " +
                "from product_stats," +
                "lateral table(ik_analyza(spu_name)) as T(keyword)," +
                "lateral table(keywordProductC2R(click_ct,cart_ct,order_ct)) as T2(ct,source)"
        );
        //转换成数据流
        DataStream<KeywordStats> keywordStatsProductDataStream = tableEnvironment.<KeywordStats>toAppendStream(keywordStatsProduct, KeywordStats.class);
        //TODO 8.写入到ClickHouse
        keywordStatsProductDataStream.addSink(
                ClickHouseUtil.<KeywordStats>getJdbcSink(
                        "insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts)  " +
                                "values(?,?,?,?,?,?)"));
        env.execute();
    }
}
