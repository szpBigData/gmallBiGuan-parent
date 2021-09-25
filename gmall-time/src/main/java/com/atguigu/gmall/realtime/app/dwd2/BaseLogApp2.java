package com.atguigu.gmall.realtime.app.dwd2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author sunzhipeng
 * @create 2021-08-13 6:59
 * 前面采集的日志数据已经保存到Kafka中，作为日志数据的ODS层，
 * 从kafka的ODS层读取的日志数据分为3类, 页面日志、启动日志和曝光日志。
 * 这三类数据虽然都是用户行为数据，但是有着完全不一样的数据结构，所以要拆分处理。
 * 将拆分后的不同的日志写回Kafka不同主题中，作为日志DWD层。
 * 页面日志输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光侧输出流
 */
public class BaseLogApp2 {
    //定义用户行为主题信息
    private static final String TOPIC_START ="dwd_start_log";
    private static final String TOPIC_PAGE ="dwd_page_log";
    private static final String TOPIC_DISPLAY ="dwd_display_log";
    public static void main(String[] args) throws Exception{
        //创建基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        //设置CK相关的参数
        //设置精准一次性保证（默认）  每5000ms开始一次checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //Checkpoint必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop103:8020/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME","test");
        //指定消费者配置信息
        String groupId = "ods_dwd_base_log_app";
        String topic = "ods_base_log";
        //TODO 1.从kafka中读取数据
        //调用Kafka工具类，从指定Kafka主题读取数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic,groupId);
        //添加到流环境
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObject =
                kafkaDS.map(new MapFunction<String, JSONObject>() {
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject;
            }
        });
        //这里我的理解就是多学点怎么用，其他的还是看情况使用，这一步不是必须步骤
        //识别新老访客,保存每个mid的首次访问时间
        //按照设备id进行分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObject.keyBy(data -> data.getJSONObject("common").getString("mid"));
        midKeyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {
           //声明第一次访问日期状态
            private ValueState<String> firstVisitDataState;
           //声明日期数据格式化对象
            private SimpleDateFormat simpleDateFormat;
            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化数据
                firstVisitDataState=getRuntimeContext().getState(new ValueStateDescriptor<String>("newMidDateState",String.class));
                simpleDateFormat=new SimpleDateFormat("yyyyMMdd");
            }

            @Override
            public JSONObject map(JSONObject jsonObj) throws Exception {
                //打印数据
                System.out.println(jsonObj);
                //获取访问标记 0表示老访客  1表示新访客
                String isNew = jsonObj.getJSONObject("common").getString("is_new");
                //获取时间戳
                Long ts = jsonObj.getLong("ts");
                //判断标记为1继续进行校验
                if ("1".equals(isNew)){
                    //获取新访客状态
                    String newMidDate = firstVisitDataState.value();
                    //获取当前数据访问日期
                    String tsDate = simpleDateFormat.format(new Date(ts));
                    if (newMidDate!=null && newMidDate.length()!=0){
                        if (!newMidDate.equals(tsDate)){
                            isNew="0";
                            jsonObj.getJSONObject("common").put("is_new",isNew);
                        }
                    }else {
                        //如果复检后，该设备确实没有访问过，那么更新状态为当前日期
                        firstVisitDataState.update(tsDate);
                    }
                }
                //返回确认过新老访客的json数据
                return jsonObj;
            }
        });


   //3.利用侧输出流来进行数据拆分
   //定义启动和曝光数据的侧输出流标签
        OutputTag<String> startTag = new OutputTag<String>("start") {};
        OutputTag<String> displayTag = new OutputTag<String>("display") {};

        //日志页面日志、启动日志、曝光日志
        //将不同的日志输出到不同的流中 页面日志输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光日志侧输出流
        SingleOutputStreamOperator<String> pageDStream = midKeyedDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                //获取数据中的启动字段
                JSONObject startJsonObj = jsonObj.getJSONObject("start");
                //将数据转换为字符串，准备向流中进行输出
                String dataStr = jsonObj.toString();
                //如果是启动字段则输出到启动侧输出流
                if (startJsonObj != null && startJsonObj.size() > 0) {
                    ctx.output(startTag, dataStr);
                } else {
                    //非启动日志，则为页面日志或者曝光日志
                    System.out.println("PageString:" + dataStr);
                    //将页面输出到主流
                    out.collect(dataStr);
                    //获取数据中的曝光数据，如果不为空，则将每条曝光数据取出
                    //输出到曝光日志侧输出流
                    JSONArray displays = jsonObj.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displaysJSONObject = displays.getJSONObject(i);
                            //获取页面id
                            String pageId = jsonObj.getJSONObject("page").getString("page_id");
                            //给每条曝光信息加上pageid
                            displaysJSONObject.put("page_id", pageId);
                            //将曝光数据输出到测输出流
                            ctx.output(displayTag, displaysJSONObject.toString());
                        }
                    }
                }
            }
        });
        //获得侧输出流
        DataStream<String> startDStream = pageDStream.getSideOutput(startTag);
        DataStream<String> displayDStream = pageDStream.getSideOutput(displayTag);

        //TODO 4.将数据输出到kafka不同的主题中
        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        FlinkKafkaProducer<String> displaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);

        startDStream.addSink(startSink);
        pageDStream.addSink(pageSink);
        displayDStream.addSink(displaySink);
        //执行
        env.execute("dwd_base_log Job");
    }
}
//ods_base_log
//{"common":{"ar":"230000","ba":"Honor","ch":"web","is_new":"1","md":"Honor 20s","mid":"mid_15","os":"Android 11.0","uid":"26","vc":"v2.1.132"},"start":{"entry":"icon","loading_time":7681,"open_ad_id":19,"open_ad_ms":9533,"open_ad_skip_ms":2605},"ts":1616332258000}
//{"common":{"ar":"230000","ba":"Honor","ch":"web","is_new":"1","md":"Honor 20s","mid":"mid_15","os":"Android 11.0","uid":"26","vc":"v2.1.132"},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","order":1,"pos_id":3},{"display_type":"promotion","item":"8","item_type":"sku_id","order":2,"pos_id":3},{"display_type":"query","item":"4","item_type":"sku_id","order":3,"pos_id":1},{"display_type":"query","item":"5","item_type":"sku_id","order":4,"pos_id":4},{"display_type":"promotion","item":"4","item_type":"sku_id","order":5,"pos_id":2},{"display_type":"query","item":"7","item_type":"sku_id","order":6,"pos_id":4},{"display_type":"query","item":"4","item_type":"sku_id","order":7,"pos_id":3},{"display_type":"promotion","item":"6","item_type":"sku_id","order":8,"pos_id":5},{"display_type":"query","item":"7","item_type":"sku_id","order":9,"pos_id":3}],"page":{"during_time":14138,"page_id":"home"},"ts":1616332258000}
//{"common":{"ar":"230000","ba":"Honor","ch":"web","is_new":"1","md":"Honor 20s","mid":"mid_15","os":"Android 11.0","uid":"26","vc":"v2.1.132"},"page":{"during_time":16267,"last_page_id":"home","page_id":"search"},"ts":1616332258000}
//{"common":{"ar":"230000","ba":"Honor","ch":"web","is_new":"1","md":"Honor 20s","mid":"mid_15","os":"Android 11.0","uid":"26","vc":"v2.1.132"},"displays":[{"display_type":"promotion","item":"1","item_type":"sku_id","order":1,"pos_id":3},{"display_type":"query","item":"8","item_type":"sku_id","order":2,"pos_id":3},{"display_type":"promotion","item":"9","item_type":"sku_id","order":3,"pos_id":3},{"display_type":"query","item":"9","item_type":"sku_id","order":4,"pos_id":3},{"display_type":"recommend","item":"8","item_type":"sku_id","order":5,"pos_id":2},{"display_type":"query","item":"9","item_type":"sku_id","order":6,"pos_id":3},{"display_type":"query","item":"7","item_type":"sku_id","order":7,"pos_id":1},{"display_type":"query","item":"7","item_type":"sku_id","order":8,"pos_id":5},{"display_type":"query","item":"9","item_type":"sku_id","order":9,"pos_id":3},{"display_type":"promotion","item":"4","item_type":"sku_id","order":10,"pos_id":3}],"page":{"during_time":8269,"item":"图书","item_type":"keyword","last_page_id":"search","page_id":"good_list"},"ts":1616332258000}
//{"actions":[{"action_id":"get_coupon","item":"1","item_type":"coupon_id","ts":1616332266954}],"common":{"ar":"230000","ba":"Honor","ch":"web","is_new":"1","md":"Honor 20s","mid":"mid_15","os":"Android 11.0","uid":"26","vc":"v2.1.132"},"displays":[{"display_type":"query","item":"8","item_type":"sku_id","order":1,"pos_id":1},{"display_type":"query","item":"7","item_type":"sku_id","order":2,"pos_id":3},{"display_type":"recommend","item":"2","item_type":"sku_id","order":3,"pos_id":3},{"display_type":"promotion","item":"5","item_type":"sku_id","order":4,"pos_id":2},{"display_type":"query","item":"1","item_type":"sku_id","order":5,"pos_id":2},{"display_type":"query","item":"7","item_type":"sku_id","order":6,"pos_id":4},{"display_type":"query","item":"10","item_type":"sku_id","order":7,"pos_id":3},{"display_type":"query","item":"6","item_type":"sku_id","order":8,"pos_id":5},{"display_type":"query","item":"2","item_type":"sku_id","order":9,"pos_id":5}],"page":{"during_time":17909,"item":"3","item_type":"sku_id","last_page_id":"good_list","page_id":"good_detail","source_type":"query"},"ts":1616332258000}
//
//dwd_start_log
//{"common":{"ar":"110000","uid":"12","os":"iOS 13.3.1","ch":"Appstore","is_new":"1","md":"iPhone 8","mid":"mid_7","vc":"v2.1.134","ba":"iPhone"},"start":{"entry":"icon","open_ad_skip_ms":2328,"open_ad_ms":5305,"loading_time":7754,"open_ad_id":17},"ts":1616332613000}
//{"common":{"ar":"420000","uid":"47","os":"iOS 13.2.3","ch":"Appstore","is_new":"0","md":"iPhone Xs Max","mid":"mid_14","vc":"v2.1.134","ba":"iPhone"},"err":{"msg":" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.log.bean.AppError.main(AppError.java:xxxxxx)","error_code":2332},"start":{"entry":"icon","open_ad_skip_ms":0,"open_ad_ms":9749,"loading_time":14122,"open_ad_id":19},"ts":1616332614000}
//{"common":{"ar":"110000","uid":"40","os":"iOS 13.3.1","ch":"Appstore","is_new":"1","md":"iPhone 8","mid":"mid_13","vc":"v2.0.1","ba":"iPhone"},"start":{"entry":"icon","open_ad_skip_ms":0,"open_ad_ms":2388,"loading_time":15214,"open_ad_id":4},"ts":1616332615000}
//{"common":{"ar":"110000","uid":"19","os":"Android 11.0","ch":"wandoujia","is_new":"0","md":"Xiaomi Mix2 ","mid":"mid_17","vc":"v2.1.134","ba":"Xiaomi"},"start":{"entry":"icon","open_ad_skip_ms":0,"open_ad_ms":8709,"loading_time":6655,"open_ad_id":7},"ts":1616332616000}
//{"common":{"ar":"310000","uid":"18","os":"iOS 12.4.1","ch":"Appstore","is_new":"1","md":"iPhone X","mid":"mid_16","vc":"v2.1.134","ba":"iPhone"},"start":{"entry":"icon","open_ad_skip_ms":0,"open_ad_ms":2269,"loading_time":1403,"open_ad_id":5},"ts":1616332617000}
//dwd_page_log
//{"common":{"ar":"310000","uid":"39","os":"Android 10.0","ch":"oppo","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_16","vc":"v2.1.134","ba":"Xiaomi"},"page":{"page_id":"home","during_time":4836},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":4,"order":1},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":4,"order":2},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":4,"order":3},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":3,"order":4},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":2,"order":5},{"display_type":"query","item":"6","item_type":"sku_id","pos_id":2,"order":6},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":1,"order":7},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":5,"order":8},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":4,"order":9}],"ts":1616336963000}
//{"common":{"ar":"310000","uid":"39","os":"Android 10.0","ch":"oppo","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_16","vc":"v2.1.134","ba":"Xiaomi"},"page":{"page_id":"good_detail","item":"6","during_time":4421,"item_type":"sku_id","last_page_id":"home","source_type":"recommend"},"displays":[{"display_type":"query","item":"8","item_type":"sku_id","pos_id":4,"order":1},{"display_type":"query","item":"9","item_type":"sku_id","pos_id":4,"order":2},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":1,"order":3},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":3,"order":4},{"display_type":"query","item":"9","item_type":"sku_id","pos_id":5,"order":5},{"display_type":"query","item":"9","item_type":"sku_id","pos_id":1,"order":6},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":3,"order":7},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":5,"order":8},{"display_type":"query","item":"10","item_type":"sku_id","pos_id":2,"order":9},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":1,"order":10}],"actions":[{"item":"2","action_id":"get_coupon","item_type":"coupon_id","ts":1616336965210}],"ts":1616336963000}
//dwd_display_log
//{"display_type":"activity","page_id":"home","item":"2","item_type":"activity_id","pos_id":4,"order":1}
//        {"display_type":"query","page_id":"home","item":"10","item_type":"sku_id","pos_id":4,"order":2}
//        {"display_type":"promotion","page_id":"home","item":"9","item_type":"sku_id","pos_id":1,"order":3}
