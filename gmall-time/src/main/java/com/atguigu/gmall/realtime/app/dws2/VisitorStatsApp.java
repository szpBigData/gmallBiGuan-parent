package com.atguigu.gmall.realtime.app.dws2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @author sunzhipeng
 * @create 2021-08-29 20:12
 * 访客主题统计
 * 接收各个明细数据，变为数据流
 * 把数据流合并在一起，成为一个相同格式的对象的数据流
 * 对合并的流进行聚合，聚合的事假窗口决定了时效性
 * 将聚合结果写入数据库
 *  * <p>
 *  * ?要不要把多个明细的同样的维度统计在一起?
 *  * 因为单位时间内mid的操作数据非常有限不能明显的压缩数据量（如果是数据量够大，或者单位时间够长可以）
 *  * 所以用常用统计的四个维度进行聚合 渠道、新老用户、app版本、省市区域
 *  * 度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 *  * 聚合窗口： 10秒
 *  * <p>
 *  * 各个数据在维度聚合前不具备关联性 ，所以 先进行维度聚合
 *  * 进行关联  这是一个fulljoin
 *  * 可以考虑使用flinksql 完成
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception  {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //1.3 检查点CK相关设置
        env.setParallelism(1);

        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop103:8020/gmall/flink/checkpoint/VisitorStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","test");


        //2.1 声明读取的主题名以及消费者组
        String pageViewSourceTopic = "dwd_page_log";
//dwd_page_log
//{"common":{"ar":"310000","uid":"39","os":"Android 10.0","ch":"oppo","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_16","vc":"v2.1.134","ba":"Xiaomi"},"page":{"page_id":"home","during_time":4836},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":4,"order":1},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":4,"order":2},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":4,"order":3},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":3,"order":4},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":2,"order":5},{"display_type":"query","item":"6","item_type":"sku_id","pos_id":2,"order":6},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":1,"order":7},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":5,"order":8},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":4,"order":9}],"ts":1616336963000}
//{"common":{"ar":"310000","uid":"39","os":"Android 10.0","ch":"oppo","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_16","vc":"v2.1.134","ba":"Xiaomi"},"page":{"page_id":"good_detail","item":"6","during_time":4421,"item_type":"sku_id","last_page_id":"home","source_type":"recommend"},"displays":[{"display_type":"query","item":"8","item_type":"sku_id","pos_id":4,"order":1},{"display_type":"query","item":"9","item_type":"sku_id","pos_id":4,"order":2},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":1,"order":3},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":3,"order":4},{"display_type":"query","item":"9","item_type":"sku_id","pos_id":5,"order":5},{"display_type":"query","item":"9","item_type":"sku_id","pos_id":1,"order":6},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":3,"order":7},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":5,"order":8},{"display_type":"query","item":"10","item_type":"sku_id","pos_id":2,"order":9},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":1,"order":10}],"actions":[{"item":"2","action_id":"get_coupon","item_type":"coupon_id","ts":1616336965210}],"ts":1616336963000}
        String uniqueVisitSourceTopic = "dwm_unique_visit";
//{"common":{"ar":"440000","uid":"35","os":"Android 10.0","ch":"xiaomi","is_new":"1","md":"Xiaomi 9","mid":"mid_7","vc":"v2.0.1","ba":"Xiaomi"},"page":{"page_id":"home","during_time":2834},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":2,"order":1},{"display_type":"query","item":"6","item_type":"sku_id","pos_id":3,"order":2},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":1,"order":3},{"display_type":"promotion","item":"7","item_type":"sku_id","pos_id":2,"order":4},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":4,"order":5},{"display_type":"recommend","item":"4","item_type":"sku_id","pos_id":3,"order":6},{"display_type":"query","item":"10","item_type":"sku_id","pos_id":5,"order":7},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":3,"order":8},{"display_type":"recommend","item":"4","item_type":"sku_id","pos_id":1,"order":9},{"display_type":"recommend","item":"4","item_type":"sku_id","pos_id":3,"order":10},{"display_type":"promotion","item":"9","item_type":"sku_id","pos_id":5,"order":11}],"ts":1616334672000}
//{"common":{"ar":"440000","uid":"19","os":"iOS 13.2.9","ch":"Appstore","is_new":"1","md":"iPhone X","mid":"mid_4","vc":"v2.1.132","ba":"iPhone"},"page":{"page_id":"home","during_time":18088},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":3,"order":1},{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":3,"order":2},{"display_type":"promotion","item":"3","item_type":"sku_id","pos_id":5,"order":3},{"display_type":"query","item":"7","item_type":"sku_id","pos_id":3,"order":4},{"display_type":"promotion","item":"6","item_type":"sku_id","pos_id":3,"order":5},{"display_type":"promotion","item":"6","item_type":"sku_id","pos_id":2,"order":6},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":3,"order":7},{"display_type":"query","item":"10","item_type":"sku_id","pos_id":1,"order":8},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":1,"order":9},{"display_type":"promotion","item":"9","item_type":"sku_id","pos_id":5,"order":10}],"ts":1616334673000}
//{"common":{"ar":"110000","uid":"41","os":"iOS 13.2.3","ch":"Appstore","is_new":"0","md":"iPhone Xs Max","mid":"mid_1","vc":"v2.1.134","ba":"iPhone"},"err":{"msg":" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.log.bean.AppError.main(AppError.java:xxxxxx)","error_code":3485},"page":{"page_id":"home","during_time":16527},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":5,"order":1},{"display_type":"recommend","item":"1","item_type":"sku_id","pos_id":4,"order":2},{"display_type":"promotion","item":"3","item_type":"sku_id","pos_id":3,"order":3},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":4,"order":4},{"display_type":"promotion","item":"3","item_type":"sku_id","pos_id":5,"order":5},{"display_type":"promotion","item":"7","item_type":"sku_id","pos_id":5,"order":6},{"display_type":"promotion","item":"5","item_type":"sku_id","pos_id":3,"order":7},{"display_type":"query","item":"6","item_type":"sku_id","pos_id":4,"order":8}],"ts":1616334674000}
//{"common":{"ar":"230000","uid":"15","os":"Android 9.0","ch":"xiaomi","is_new":"0","md":"vivo iqoo3","mid":"mid_5","vc":"v2.1.134","ba":"vivo"},"page":{"page_id":"home","during_time":5363},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":5,"order":1},{"display_type":"recommend","item":"6","item_type":"sku_id","pos_id":4,"order":2},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":2,"order":3},{"display_type":"query","item":"9","item_type":"sku_id","pos_id":4,"order":4},{"display_type":"promotion","item":"9","item_type":"sku_id","pos_id":5,"order":5},{"display_type":"recommend","item":"5","item_type":"sku_id","pos_id":5,"order":6}],"ts":1616334676000}
//{"common":{"ar":"110000","uid":"20","os":"Android 10.0","ch":"xiaomi","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_6","vc":"v2.1.134","ba":"Xiaomi"},"page":{"page_id":"home","during_time":4239},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":1,"order":1},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":1,"order":2},{"display_type":"promotion","item":"9","item_type":"sku_id","pos_id":4,"order":3},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":2,"order":4},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":1,"order":5},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":5,"order":6},{"display_type":"promotion","item":"5","item_type":"sku_id","pos_id":3,"order":7},{"display_type":"query","item":"7","item_type":"sku_id","pos_id":2,"order":8},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":1,"order":9},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":2,"order":10}],"ts":1616334677000}
//{"common":{"ar":"370000","uid":"18","os":"Android 11.0","ch":"oppo","is_new":"1","md":"Huawei Mate 30","mid":"mid_14","vc":"v2.1.134","ba":"Huawei"},"page":{"page_id":"home","during_time":8359},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":4,"order":1},{"display_type":"promotion","item":"6","item_type":"sku_id","pos_id":5,"order":2},{"display_type":"promotion","item":"7","item_type":"sku_id","pos_id":4,"order":3},{"display_type":"promotion","item":"9","item_type":"sku_id","pos_id":1,"order":4},{"display_type":"promotion","item":"10","item_type":"sku_id","pos_id":4,"order":5}],"ts":1616334677000}
//{"common":{"ar":"110000","uid":"22","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 8","mid":"mid_9","vc":"v2.1.134","ba":"iPhone"},"page":{"page_id":"home","during_time":12454},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":4,"order":1},{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":4,"order":2},{"display_type":"recommend","item":"7","item_type":"sku_id","pos_id":5,"order":3},{"display_type":"promotion","item":"3","item_type":"sku_id","pos_id":4,"order":4},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":3,"order":5},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":5,"order":6}],"ts":1616334678000}
//
 String userJumpDetailSourceTopic = "dwm_user_jump_detail";
/**
{"common":{"ar":"440000","uid":"28","os":"Android 11.0","ch":"oppo","is_new":"1","md":"Redmi k30","mid":"mid_4","vc":"v2.0.1","ba":"Redmi"},"page":{"page_id":"home","during_time":16383},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":3,"order":1},{"display_type":"recommend","item":"2","item_type":"sku_id","pos_id":5,"order":2},{"display_type":"query","item":"10","item_type":"sku_id","pos_id":3,"order":3},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":1,"order":4},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":3,"order":5},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":1,"order":6},{"display_type":"query","item":"7","item_type":"sku_id","pos_id":4,"order":7},{"display_type":"promotion","item":"10","item_type":"sku_id","pos_id":2,"order":8}],"ts":1616336169000}
{"common":{"ar":"500000","uid":"10","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone X","mid":"mid_10","vc":"v2.1.132","ba":"iPhone"},"page":{"page_id":"home","during_time":5971},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":1,"order":1},{"display_type":"recommend","item":"4","item_type":"sku_id","pos_id":3,"order":2},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":1,"order":3},{"display_type":"query","item":"6","item_type":"sku_id","pos_id":1,"order":4},{"display_type":"recommend","item":"4","item_type":"sku_id","pos_id":3,"order":5},{"display_type":"promotion","item":"1","item_type":"sku_id","pos_id":1,"order":6},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":1,"order":7},{"display_type":"recommend","item":"6","item_type":"sku_id","pos_id":2,"order":8},{"display_type":"promotion","item":"4","item_type":"sku_id","pos_id":4,"order":9},{"display_type":"promotion","item":"10","item_type":"sku_id","pos_id":2,"order":10},{"display_type":"query","item":"6","item_type":"sku_id","pos_id":3,"order":11}],"ts":1616336171000}
{"common":{"ar":"500000","uid":"27","os":"Android 10.0","ch":"web","is_new":"0","md":"Redmi k30","mid":"mid_13","vc":"v2.1.111","ba":"Redmi"},"page":{"page_id":"home","during_time":11373},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":4,"order":1},{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":4,"order":2},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":5,"order":3},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":5,"order":4},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":4,"order":5},{"display_type":"promotion","item":"9","item_type":"sku_id","pos_id":3,"order":6},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":2,"order":7},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":4,"order":8},{"display_type":"recommend","item":"3","item_type":"sku_id","pos_id":5,"order":9},{"display_type":"query","item":"7","item_type":"sku_id","pos_id":5,"order":10},{"display_type":"recommend","item":"7","item_type":"sku_id","pos_id":4,"order":11}],"ts":1616336173000}
{"common":{"ar":"440000","uid":"10","os":"Android 11.0","ch":"oppo","is_new":"0","md":"vivo iqoo3","mid":"mid_1","vc":"v2.1.134","ba":"vivo"},"page":{"page_id":"home","during_time":18945},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":3,"order":1},{"display_type":"promotion","item":"5","item_type":"sku_id","pos_id":2,"order":2},{"display_type":"promotion","item":"3","item_type":"sku_id","pos_id":4,"order":3},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":1,"order":4},{"display_type":"promotion","item":"2","item_type":"sku_id","pos_id":5,"order":5},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":3,"order":6},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":1,"order":7},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":5,"order":8},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":2,"order":9},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":3,"order":10}],"ts":1616336203000}
{"common":{"ar":"230000","uid":"46","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone Xs","mid":"mid_12","vc":"v2.1.134","ba":"iPhone"},"page":{"page_id":"home","during_time":13665},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":2,"order":1},{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":2,"order":2},{"display_type":"promotion","item":"8","item_type":"sku_id","pos_id":4,"order":3},{"display_type":"promotion","item":"3","item_type":"sku_id","pos_id":3,"order":4},{"display_type":"recommend","item":"3","item_type":"sku_id","pos_id":3,"order":5},{"display_type":"promotion","item":"5","item_type":"sku_id","pos_id":2,"order":6},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":1,"order":7},{"display_type":"query","item":"7","item_type":"sku_id","pos_id":2,"order":8},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":3,"order":9},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":1,"order":10},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":4,"order":11},{"display_type":"promotion","item":"4","item_type":"sku_id","pos_id":5,"order":12}],"ts":1616336211000}
*/
        String groupId = "visitor_stats_app";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpDetailSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);
        DataStreamSource<String> pvJsonStrDS = env.addSource(pageViewSource);
        DataStreamSource<String> uvJsonStrDS = env.addSource(uniqueVisitSource);
        DataStreamSource<String> userJumpJsonStrDS = env.addSource(userJumpDetailSource);
        SingleOutputStreamOperator<VisitorStats> pvStatsDS = pvJsonStrDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0l,
                        1l,
                        0l,
                        0l,
                        jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts")
                );
                return visitorStats;
            }
        });
//转换uv
        SingleOutputStreamOperator<VisitorStats> uvStatDS = uvJsonStrDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        1l,
                        0l,
                        0l,
                        0l,
                        0l,
                        jsonObject.getLong("ts")
                );
                return visitorStats;
            }
        });
        //3.转换sv流，从dwd_page_log获取
        SingleOutputStreamOperator<VisitorStats>  svStatsDS= pvJsonStrDS.process(new ProcessFunction<String, VisitorStats>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<VisitorStats> out) throws Exception {
                //转换成json 格式
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                //获取当前页面的lastpageId
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() > 0) {
                    VisitorStats visitorStats = new VisitorStats(
                            "",
                            "",
                            jsonObject.getJSONObject("common").getString("vc"),
                            jsonObject.getJSONObject("common").getString("ch"),
                            jsonObject.getJSONObject("common").getString("ar"),
                            jsonObject.getJSONObject("common").getString("is_new"),
                            0L,
                            0L,
                            1L,
                            0L,
                            0L,
                            jsonObject.getLong("ts")
                    );
                    out.collect(visitorStats);
                }
            }
        });
        //转换跳出流
        SingleOutputStreamOperator<VisitorStats> userJumpStatsDS = userJumpJsonStrDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String value) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value);
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        jsonObj.getJSONObject("common").getString("vc"),
                        jsonObj.getJSONObject("common").getString("ch"),
                        jsonObj.getJSONObject("common").getString("ar"),
                        jsonObj.getJSONObject("common").getString("is_new"),
                        0L,
                        0L,
                        0L,
                        1L,
                        0L,
                        jsonObj.getLong("ts")
                );
                return visitorStats;
            }
        });
        //将四条流合并到一起
        DataStream<VisitorStats> unionstream = pvStatsDS.union(uvStatDS, svStatsDS, userJumpStatsDS);
        //设置水位线
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDS = unionstream.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );
        //指定按啥维度进行分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = visitorStatsWithWatermarkDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return Tuple4.of(
                        value.getAr(),
                        value.getCh(),
                        value.getVc(),
                        value.getIs_new()
                );
            }
        });
        //开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        //对窗口的数据进行聚合 聚合结束之后，需要补充统计的起止时间
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(new ReduceFunction<VisitorStats>() {
                                                                              @Override
                                                                              public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                                                                                  stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                                                                                  stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                                                                                  stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                                                                                  stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                                                                                  return stats1;
                                                                              }
                                                                          },
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (VisitorStats visitorStats : elements) {
                            //获取窗口的开始时间
                            String startDate = sdf.format(new Date(context.window().getStart()));
                            String endDate = sdf.format(new Date(context.window().getEnd()));
                            visitorStats.setStt(startDate);
                            visitorStats.setEdt(endDate);
                            visitorStats.setTs(new Date().getTime());
                            out.collect(visitorStats);
                        }
                    }
                }
        );
        //向ck中插入数据
        reduceDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)")
        );
        //选用replaceMergeTree引擎来保证数据表的幂等性
//        其中flink-connector-jdbc 是官方通用的jdbcSink包。只要引入对应的jdbc驱动，flink可以用它应对各种支持jdbc的数据库，比如phoenix也可以用它。但是这个jdbc-sink只支持数据流对应一张数据表。如果是一流对多表，就必须通过自定义的方式实现了，比如之前的维度数据。
//        虽然这种jdbc-sink只能一流对一表，但是由于内部使用了预编译器，所以可以实现批量提交以优化写入速度。



        env.execute();
    }
}
