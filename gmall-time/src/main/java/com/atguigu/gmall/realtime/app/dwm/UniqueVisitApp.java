package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author sunzhipeng
 * @create 2021-07-17 21:09
 * 统计日活
 * 那么如何从用户行为日志中识别出当日的访客，那么有两点：
 * 	其一，是识别出该访客打开的第一个页面，表示这个访客开始进入我们的应用
 * 	其二，由于访客可以在一天中多次进入应用，所以我们要在一天的范围内进行去重
 * 前期准备：
 *  *   -启动ZK、Kafka、Logger.sh、BaseLogApp、UniqueVisitApp
 *  * 执行流程:
 *  *   模式生成日志的jar->nginx->日志采集服务->kafka(ods)
 *  *   ->BaseLogApp(分流)->kafka(dwd) dwd_page_log
 *  *   ->UniqueVisitApp(独立访客)->kafka(dwm_unique_visit)
 *  	重写filter方法进行过滤
 * 	可以直接筛掉last_page_id不为空的字段，因为只要有上一页，说明这条不是这个用户进入的首个页面。
 * 	 状态用来记录用户的进入时间，只要这个lastVisitDate是今天，就说明用户今天已经访问过了所以筛除掉。如果为空或者不是今天，说明今天还没访问过，则保留。
 * 	因为状态值主要用于筛选是否今天来过，所以这个记录过了今天基本上没有用了，这里enableTimeToLive 设定了1天的过期时间，避免状态过大。
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //设置CK相关的参数
        //设置精准一次性保证（默认）  每5000ms开始一次checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //Checkpoint必须在一分钟内完成，否则就会被抛弃
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop103:8020/gmall/flink/checkpoint"));
//        System.setProperty("HADOOP_USER_NAME","test");
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app_group";
        String sinkTopic = "dwm_unique_visit";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic,groupId);
        DataStreamSource<String> source = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> jsonObjectStream = source.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            return jsonObject;
        });
        //按照设备id进行分组
        KeyedStream<JSONObject, String> keybyWithMidDS= jsonObjectStream.keyBy(data -> data.getJSONObject("common").getString("mid"));
        //过滤得到uv
        SingleOutputStreamOperator<JSONObject> filteredDS = keybyWithMidDS.filter(new RichFilterFunction<JSONObject>() {
            //定义状态和日期工具类
            ValueState<String> lastVisitDateState = null;
            SimpleDateFormat sdf = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化日期
                sdf = new SimpleDateFormat("yyyy-MM-dd");
                ValueStateDescriptor<String> lastVisitDateStateDes = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                //因为我们统计的是日活DAU，所以状态只能在当天有效，过了一天就可以失效掉
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                lastVisitDateStateDes.enableTimeToLive(stateTtlConfig);
                this.lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDes);
            }

            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                //首先判断当前页面是否从别的页面跳转过来
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                //获取当前访问状态
                long ts = jsonObj.getLong("ts");
                //将当前访问时间戳转换为日期字符串
                String logDate = sdf.format(new Date(ts));
                //获取状态日期
                String lastVisitDate = lastVisitDateState.value();
                //用当前页面的访问时间和状态时间进行对比
                if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                    System.out.println("已访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                    return false;
                } else {
                    System.out.println("未访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                    lastVisitDateState.update(logDate);
                    return true;
                }
            }
        }).uid("uvFilter");
        //TODO 6. 向kafka中写回，需要将json转换为String
        //6.1 json->string
        SingleOutputStreamOperator<String> kafkaDS = filteredDS.map(jsonObj -> jsonObj.toJSONString());

        //6.2 写回到kafka的dwm层
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
