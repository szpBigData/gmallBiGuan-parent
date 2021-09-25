//package com.atguigu.gmall.realtime.app;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.runtime.state.filesystem.FsStateBackend;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.util.OutputTag;
//
//import java.text.SimpleDateFormat;
//import java.util.Date;
//
///**
// * @author sunzhipeng
// * @create 2021-07-16 13:14
// */
//public class BaseLogApp {
//    //定义用户行为主题信息
//    private static final String TOPIC_START="dwd_start_topic";
//    private static final String TOPIC_PAGE="dwd_page_log";
//    private static final String TOPIC_DISPLLAY="dwd_display_log";
//
//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
//        //设置CK相关的参数
//        //设置精准一次性保证（默认）  每5000ms开始一次checkpoint
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        //Checkpoint必须在一分钟内完成，否则就会被抛弃
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint"));
//        System.setProperty("HADOOP_USER_NAME","test");
//
//        String groupId="ods_dwd_base_log_app";
//        String topic="ods_base_log";
//
//        //1.从kafka拿数据
//        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
//        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
//        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
//            @Override
//            public JSONObject map(String value) throws Exception {
//                JSONObject jsonObject = JSON.parseObject(value);
//
//                return jsonObject;
//            }
//        });
//        //识别新老用户
//        //保存每个mid的首次访问日期，每条进入该算子的访问记录，都会把mid对应的首次访问时间和当前日期进行比较，只有首次访问时间不为空
//        //且首次访问时间早于的，则认为该访客是老访客，否则是新访客，如果没有访问记录的话，会写入首次访问时间。
//        KeyedStream<JSONObject, String> midKeyedDS = jsonObjectDS.keyBy(data -> data.getJSONObject("common").getString("mid"));
//        SingleOutputStreamOperator<JSONObject> midWithNewFlagDS = midKeyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {
//            //声明第一次访问日期的状态
//            private ValueState<String> firstVisitDataState;
//            //声明日期格式化对象
//            private SimpleDateFormat simpleDateFormat;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                firstVisitDataState=getRuntimeContext().getState(
//                        new ValueStateDescriptor<String>("newMidDateState",String.class)
//                );
//                simpleDateFormat=new SimpleDateFormat("yyyyMMdd");
//            }
//
//            @Override
//            public JSONObject map(JSONObject jsonObj) throws Exception {
//                //打印数据
//                System.out.println(jsonObj);
//                //获取访问标记  0表示老访客  1表示新访客
//                String isNew=jsonObj.getJSONObject("common").getString("is_new");
//                //获取数据中的时间戳
//                long ts=jsonObj.getLong("ts");
//
//                //判断标记如果为"1",则继续校验数据
//                if ("1".equals(isNew)){
//                    //获取新访客状态
//                    String newMidDate=firstVisitDataState.value();
//                    //获取当前数据访问日期
//                    String tsDate=simpleDateFormat.format(new Date(ts));
//                    //如果新访客状态不为空，说明该设备已访问过，则将访问标记为0
//                    if (newMidDate!=null &&newMidDate.length()!=0){
//                        if (!newMidDate.equals(tsDate)){
//                            isNew="0";
//                            jsonObj.getJSONObject("common").put("is_new",isNew);
//                        }
//                    }else {
//                        //如果复检后，该设备的确没有访问过，那么更新的状态为当前日期
//                        firstVisitDataState.update(tsDate);
//                    }
//                }
//                //返回确认过新老访客的接送数据
//                return null;
//            }
//        });
//
//        //定义侧输出标签实现数据拆分
//        OutputTag<String> startTag=new OutputTag<String>("start");
//        OutputTag<String> displayTag=new OutputTag<String>("display");
//
//        env.execute("dwd_baselog");
//    }
//}
