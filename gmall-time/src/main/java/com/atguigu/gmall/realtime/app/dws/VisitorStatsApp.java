//package com.atguigu.gmall.realtime.app.dws;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.atguigu.gmall.realtime.bean.ClickHouseUtil;
//import com.atguigu.gmall.realtime.bean.VisitorStats;
//import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
//import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.runtime.state.StateBackend;
//import org.apache.flink.runtime.state.filesystem.FsStateBackend;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.*;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.util.Collector;
//
//import java.text.SimpleDateFormat;
//import java.time.Duration;
//import java.util.Date;
//
///**
// * @author sunzhipeng
// * @create 2021-07-20 16:57
// * * Desc:  访客主题统计
// *  * 需要启动的服务
// *  * -logger.sh(Nginx以及日志处理服务)、zk、kafka
// *  * -BaseLogApp、UniqueVisitApp、UserJumpDetailApp、VisitorStatsApp
// *  * 执行流程分析
// *  * -模拟生成日志数据
// *  * -交给Nginx进行反向代理
// *  * -交给日志处理服务 将日志发送到kafka的ods_base_log
// *  * -BaseLogApp从ods层读取数据，进行分流，将分流的数据发送到kakfa的dwd(dwd_page_log)
// *  * -UniqueVisitApp从dwd_page_log读取数据，将独立访客明细发送到dwm_unique_visit
// *  * -UserJumpDetailApp从dwd_page_log读取数据，将页面跳出明细发送到dwm_user_jump_detail
// *  * -VisitorStatsApp
// *  * >从dwd_page_log读取数据，计算pv、持续访问时间、session_count
// *  * >从dwm_unique_visit读取数据，计算uv
// *  * >从dwm_user_jump_detail读取数据，计算页面跳出
// *  * >输出
// *  * >统一格式 合并
// *  * >分组、开窗、聚合
// *  * >将聚合统计的结果保存到ClickHouse OLAP数据库
// */
//public class VisitorStatsApp {
//    public static void main(String[] args) throws Exception{
//        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
//        //1.3 检查点CK相关设置
////        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
////        env.getCheckpointConfig().setCheckpointTimeout(60000);
////        StateBackend fsStateBackend = new FsStateBackend(
////                "hdfs://hadoop103:8020/gmall/flink/checkpoint/VisitorStatsApp");
////        env.setStateBackend(fsStateBackend);
////        System.setProperty("HADOOP_USER_NAME","test");
//
//        //2.1 声明读取的主题名以及消费者组 从Kafka的pv、uv、跳转明细主题中获取数据
//        String pageViewSourceTopic = "dwd_page_log";
//        String uniqueVisitSourceTopic = "dwm_unique_visit";
//        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
//        String groupId = "visitor_stats_app";
//        //从dwd_page_log读取日志数据
//        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
//        DataStreamSource<String> pvJsonStrDS = env.addSource(pageViewSource);
//        //从dwm_unique_visit中读取UV数据
//        FlinkKafkaConsumer<String> uvSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
//        DataStreamSource<String> uvJsonStrDS = env.addSource(uvSource);
//        //从dwm_user_jump_detail读取跳出数据
//        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);
//        DataStreamSource<String> userJumpJsonStrDS = env.addSource(userJumpSource);
//        //把数据流合并在一起，成为一个相同格式对象的数据流
//        //合并数据流的核心算子是union。但是union算子，要求所有的数据流结构必须一致。所以union前要调整数据结构。
//        //对数据流进行结构转换  jsonStr->VisitorStats
//        //2.1 转换pv流
//        SingleOutputStreamOperator<VisitorStats> pageViewStatsDstream = pvJsonStrDS.map(
//                json -> {
//                    //  System.out.println("pv:"+json);
//                    JSONObject jsonObj = JSON.parseObject(json);
//                    return new VisitorStats("", "",
//                            jsonObj.getJSONObject("common").getString("vc"),
//                            jsonObj.getJSONObject("common").getString("ch"),
//                            jsonObj.getJSONObject("common").getString("ar"),
//                            jsonObj.getJSONObject("common").getString("is_new"),
//                            0L, 1L, 0L, 0L, jsonObj.getJSONObject("page").getLong("during_time"), jsonObj.getLong("ts"));
//                });
//
////2.2转换uv流
//        SingleOutputStreamOperator<VisitorStats> uniqueVisitStatsDstream = uvJsonStrDS.map(
//                json -> {
//                    JSONObject jsonObj = JSON.parseObject(json);
//                    return new VisitorStats("", "",
//                            jsonObj.getJSONObject("common").getString("vc"),
//                            jsonObj.getJSONObject("common").getString("ch"),
//                            jsonObj.getJSONObject("common").getString("ar"),
//                            jsonObj.getJSONObject("common").getString("is_new"),
//                            1L, 0L, 0L, 0L, 0L, jsonObj.getLong("ts"));
//                });
//
////2.3 转换sv流
//        SingleOutputStreamOperator<VisitorStats> sessionVisitDstream = pvJsonStrDS.process(
//                new ProcessFunction<String, VisitorStats>() {
//                    @Override
//                    public void processElement(String json, Context ctx, Collector<VisitorStats> out) throws Exception {
//                        JSONObject jsonObj = JSON.parseObject(json);
//                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
//                        if (lastPageId == null || lastPageId.length() == 0) {
//                            //    System.out.println("sc:"+json);
//                            VisitorStats visitorStats = new VisitorStats("", "",
//                                    jsonObj.getJSONObject("common").getString("vc"),
//                                    jsonObj.getJSONObject("common").getString("ch"),
//                                    jsonObj.getJSONObject("common").getString("ar"),
//                                    jsonObj.getJSONObject("common").getString("is_new"),
//                                    0L, 0L, 1L, 0L, 0L, jsonObj.getLong("ts"));
//                            out.collect(visitorStats);
//                        }
//                    }
//                });
//
////2.4 转换跳转流
//        SingleOutputStreamOperator<VisitorStats> userJumpStatDstream = userJumpJsonStrDS.map(json -> {
//            JSONObject jsonObj = JSON.parseObject(json);
//            return new VisitorStats("", "",
//                    jsonObj.getJSONObject("common").getString("vc"),
//                    jsonObj.getJSONObject("common").getString("ch"),
//                    jsonObj.getJSONObject("common").getString("ar"),
//                    jsonObj.getJSONObject("common").getString("is_new"),
//                    0L, 0L, 0L, 1L, 0L, jsonObj.getLong("ts"));
//        });
//
//
//        //将多条流合并到一起   只能合并相同结构的数据流
//        DataStream<VisitorStats> union = pageViewStatsDstream.union(uniqueVisitStatsDstream, sessionVisitDstream, userJumpStatDstream);
//
//        //TODO 4.设置水位线
//        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDstream =
//                union.assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(1)).
//                                withTimestampAssigner( (visitorStats,ts) ->visitorStats.getTs() )
//                ) ;
//
//        //TODO 5.分组 选取四个维度作为key , 使用Tuple4组合
//        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatsTuple4KeyedStream =
//                visitorStatsWithWatermarkDstream
//                        .keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
//                                   @Override
//                                   public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
//                                       return new Tuple4<>(visitorStats.getVc()
//                                               , visitorStats.getCh(),
//                                               visitorStats.getAr(),
//                                               visitorStats.getIs_new());
//
//                                   }
//                               }
//                        );
////TODO 6.开窗
//        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowStream =
//                visitorStatsTuple4KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
////TODO 7.Reduce聚合统计
//        SingleOutputStreamOperator<VisitorStats> visitorStatsDstream =
//                windowStream.reduce(new ReduceFunction<VisitorStats>() {
//                    @Override
//                    public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
//                        //把度量数据两两相加
//                        stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
//                        stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
//                        stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
//                        stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
//                        stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
//                        return stats1;
//                    }
//                }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
//                    @Override
//                    public void process(Tuple4<String, String, String, String> tuple4, Context context,
//                                        Iterable<VisitorStats> visitorStatsIn,
//                                        Collector<VisitorStats> visitorStatsOut) throws Exception {
//                        //补时间字段
//                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                        for (VisitorStats visitorStats : visitorStatsIn) {
//
//                            String startDate = simpleDateFormat.format(new Date(context.window().getStart()));
//                            String endDate = simpleDateFormat.format(new Date(context.window().getEnd()));
//
//                            visitorStats.setStt(startDate);
//                            visitorStats.setEdt(endDate);
//                            visitorStatsOut.collect(visitorStats);
//                        }
//                    }
//                });
//
////TODO 8.向ClickHouse中写入数据
//        visitorStatsDstream.addSink(
//                ClickHouseUtil.getJdbcSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));
//env.execute();
//
//    }
//}
