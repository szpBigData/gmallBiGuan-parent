package com.atguigu.gmall.realtime.app.dws2;

import com.atguigu.gmall.realtime.bean.ProvinceStats;
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
 * @create 2021-08-30 22:02
 * 地区主题宽表
 * 主要是反映各个地区的销售情况，逻辑上就是做一次轻度聚合再保存
 * 	定义Table流环境
 * 	把数据源定义为动态表
 * 	通过SQL查询出结果表
 * 	把结果表转换为数据流
 * 	把数据流写入目标数据库
 * 	定义Table流环境
 * 	把数据源定义为动态表
 * 	通过SQL查询出结果表
 * 	把结果表转换为数据流
 * 	把数据流写入目标数据库
 * 如果是Flink官方支持的数据库，也可以直接把目标数据表定义为动态表，用insert into 写入。
 * 由于ClickHouse目前官方没有支持的jdbc连接器（目前支持Mysql、 PostgreSQL、Derby）。也可以制作自定义sink，实现官方不支持的连接器。但是比较繁琐。
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop102:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","test");
        //创建表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env,settings);
        //TODO 2.把数据源定义为动态表
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        tableEnvironment.executeSql("create table order_wide (" +
                "province_id bigint," +
                "province_name string," +
                "province_area_code string," +
                "province_iso_code string," +
                "province_3166_2_code string," +
                "order_id string," +
                "split_total_amount double," +
                "create_time string," +
                "rowtime as to_timestamp(create_time)," +
                //其中WATERMARK FOR rowtime AS rowtime是把某个虚拟字段设定为EVENT_TIME
                "watermark for rowtime as rowtime)" +
                "with ("+ MyKafkaUtil.getKafkaDDL(orderWideTopic,groupId)+")");
        //聚合计算
        Table provinceStateTable = tableEnvironment.sqlQuery("" +
                "select " +
                "date_format(tumble_start(rowtime,interval '10' second),'yyyy-MM-dd HH:mm:ss') stt," +
                "date_format(tumble_end(rowtime,interval '10' second),'yyyy-MM-dd HH:mm:ss') edt," +
                "province_id," +
                "province_name," +
                "province_area_code area_code," +
                "province_iso_code iso_code," +
                "province_3166_2_code iso_3166_2," +
                "count(distinct order_id) order_count," +
                "sum(split_total_amount) order_amount," +
                "unix_timestamp()*1000 ts" +
                "from order_wide " +
                "group by tumble(rowtime,interval '10' second)," +
                "province_id,province_name,province_area_code,province_iso_code," +
                "province_3166_2_code");
        //将动态表转换成流
        DataStream<ProvinceStats> provinceStatsDS = tableEnvironment.toAppendStream(provinceStateTable, ProvinceStats.class);
        //添加到ck
        //如果是Flink官方支持的数据库，也可以直接把目标数据表定义为动态表，用insert into 写入。由于ClickHouse目前官方没有支持的jdbc连接器（目前支持Mysql、 PostgreSQL、Derby）。也可以制作自定义sink，实现官方不支持的连接器。但是比较繁琐。
        provinceStatsDS.addSink(ClickHouseUtil.getJdbcSink("insert into province_stats_0820 values(?,?,?,?,?,?,?,?,?,?)"));
        env.execute();
    }
}
