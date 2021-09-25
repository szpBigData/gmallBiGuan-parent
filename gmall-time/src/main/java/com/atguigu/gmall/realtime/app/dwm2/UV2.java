package com.atguigu.gmall.realtime.app.dwm2;

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

/**
 * @author sunzhipeng
 * @create 2021-08-19 22:18
 * 访客UV的计算
 * 思路：
 * 从用户行为日志中识别出当日的访客，那么有两点：
 * 其一，是识别出该访客打开的第一个页面，表示这个访客开始进入我们的应用
 * 其二，由于访客可以在一天中多次进入应用，所以我们要在一天的范围内进行去重
 */
public class UV2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //设置CK相关的参数
        //设置精准一次性保证（默认）  每5000ms开始一次checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //Checkpoint必须在一分钟内完成，否则就会被抛弃
      env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop103:8020/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME","test");
        //dwd_page_log
//{"common":{"ar":"310000","uid":"39","os":"Android 10.0","ch":"oppo","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_16","vc":"v2.1.134","ba":"Xiaomi"},"page":{"page_id":"home","during_time":4836},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":4,"order":1},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":4,"order":2},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":4,"order":3},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":3,"order":4},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":2,"order":5},{"display_type":"query","item":"6","item_type":"sku_id","pos_id":2,"order":6},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":1,"order":7},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":5,"order":8},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":4,"order":9}],"ts":1616336963000}
//{"common":{"ar":"310000","uid":"39","os":"Android 10.0","ch":"oppo","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_16","vc":"v2.1.134","ba":"Xiaomi"},"page":{"page_id":"good_detail","item":"6","during_time":4421,"item_type":"sku_id","last_page_id":"home","source_type":"recommend"},"displays":[{"display_type":"query","item":"8","item_type":"sku_id","pos_id":4,"order":1},{"display_type":"query","item":"9","item_type":"sku_id","pos_id":4,"order":2},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":1,"order":3},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":3,"order":4},{"display_type":"query","item":"9","item_type":"sku_id","pos_id":5,"order":5},{"display_type":"query","item":"9","item_type":"sku_id","pos_id":1,"order":6},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":3,"order":7},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":5,"order":8},{"display_type":"query","item":"10","item_type":"sku_id","pos_id":2,"order":9},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":1,"order":10}],
// "actions":[{"item":"2","action_id":"get_coupon","item_type":"coupon_id","ts":1616336965210}],"ts":1616336963000}
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app_group";
        String sinkTopic = "dwm_unique_visit";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> jsonStream = source.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            return jsonObject;
        });
        //我需要知道这里传进来的的数据格式
        //想象一下按key进行分组，就像group by id
        //对表进行过滤,过滤条件   初始化日期和状态  就像where条件   如果不满足返回false过滤掉
        //.uid的作用
        //写回kafka的格式
        KeyedStream<JSONObject, String> keybyWithMidDS = jsonStream.keyBy(data -> data.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filteredJsonObjDstream = keybyWithMidDS.filter(new RichFilterFunction<JSONObject>() {
            ValueState<String> lastVisitDateState = null;
            private SimpleDateFormat sdf = null;

            @Override
            public void open(Configuration parameters) throws Exception {

                sdf = new SimpleDateFormat("yyyy-MM-dd");
                ValueStateDescriptor<String> lastVisitDateStateDes = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                //因为我们统计的是日活DAU，所以状态只能在当天有效，过了一天就可以失效掉
                //默认值 表明当状态创建或每次写入时都会更新时间戳
                //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                //默认值  一旦这个状态过期了，那么永远不会被返回给调用方，只会返回空状态

                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                lastVisitDateStateDes.enableTimeToLive(stateTtlConfig);
                this.lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDes);
            }
//* 从用户行为日志中识别出当日的访客，那么有两点：
// 其一，是识别出该访客打开的第一个页面，表示这个访客开始进入我们的应用
// * 其二，由于访客可以在一天中多次进入应用，所以我们要在一天的范围内进行去重
//	首先用keyby按照mid进行分组，每组表示当前设备的访问情况
//	分组后使用keystate状态，记录用户进入时间，实现RichFilterFunction完成过滤
//	重写open 方法用来初始化状态
//	重写filter方法进行过滤
//	可以直接筛掉last_page_id不为空的字段，因为只要有上一页，说明这条不是这个用户进入的首个页面。
//状态用来记录用户的进入时间，只要这个lastVisitDate是今天，就说明用户今天已经访问过了所以筛除掉。如果为空或者不是今天，说明今天还没访问过，则保留。
//因为状态值主要用于筛选是否今天来过，所以这个记录过了今天基本上没有用了，这里enableTimeToLive 设定了1天的过期时间，避免状态过大。

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                long ts = value.getLong("ts");
                String logDate = sdf.format(ts);
                String lastViewDate = lastVisitDateState.value();
                if (lastViewDate != null && lastViewDate.length() > 0 && logDate.equals(lastViewDate)) {
                    System.out.println("已访问：lastVisit:" + lastViewDate + "|| logDate：" + logDate);
                    return false;
                } else {
                    System.out.println("未访问：lastVisit:" + lastViewDate + "|| logDate：" + logDate);
                    lastVisitDateState.update(logDate);
                    return true;

                }

            }
        }).uid("uvFilter");

        SingleOutputStreamOperator<String> dataJsonStringDstream = filteredJsonObjDstream.map(data -> data.toJSONString());
        dataJsonStringDstream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        /**
         * {"common":{"ar":"440000","uid":"35","os":"Android 10.0","ch":"xiaomi","is_new":"1","md":"Xiaomi 9","mid":"mid_7","vc":"v2.0.1","ba":"Xiaomi"},"page":{"page_id":"home","during_time":2834},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":2,"order":1},{"display_type":"query","item":"6","item_type":"sku_id","pos_id":3,"order":2},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":1,"order":3},{"display_type":"promotion","item":"7","item_type":"sku_id","pos_id":2,"order":4},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":4,"order":5},{"display_type":"recommend","item":"4","item_type":"sku_id","pos_id":3,"order":6},{"display_type":"query","item":"10","item_type":"sku_id","pos_id":5,"order":7},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":3,"order":8},{"display_type":"recommend","item":"4","item_type":"sku_id","pos_id":1,"order":9},{"display_type":"recommend","item":"4","item_type":"sku_id","pos_id":3,"order":10},{"display_type":"promotion","item":"9","item_type":"sku_id","pos_id":5,"order":11}],"ts":1616334672000}
         * {"common":{"ar":"440000","uid":"19","os":"iOS 13.2.9","ch":"Appstore","is_new":"1","md":"iPhone X","mid":"mid_4","vc":"v2.1.132","ba":"iPhone"},"page":{"page_id":"home","during_time":18088},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":3,"order":1},{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":3,"order":2},{"display_type":"promotion","item":"3","item_type":"sku_id","pos_id":5,"order":3},{"display_type":"query","item":"7","item_type":"sku_id","pos_id":3,"order":4},{"display_type":"promotion","item":"6","item_type":"sku_id","pos_id":3,"order":5},{"display_type":"promotion","item":"6","item_type":"sku_id","pos_id":2,"order":6},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":3,"order":7},{"display_type":"query","item":"10","item_type":"sku_id","pos_id":1,"order":8},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":1,"order":9},{"display_type":"promotion","item":"9","item_type":"sku_id","pos_id":5,"order":10}],"ts":1616334673000}
         * {"common":{"ar":"110000","uid":"41","os":"iOS 13.2.3","ch":"Appstore","is_new":"0","md":"iPhone Xs Max","mid":"mid_1","vc":"v2.1.134","ba":"iPhone"},"err":{"msg":" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.log.bean.AppError.main(AppError.java:xxxxxx)","error_code":3485},"page":{"page_id":"home","during_time":16527},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":5,"order":1},{"display_type":"recommend","item":"1","item_type":"sku_id","pos_id":4,"order":2},{"display_type":"promotion","item":"3","item_type":"sku_id","pos_id":3,"order":3},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":4,"order":4},{"display_type":"promotion","item":"3","item_type":"sku_id","pos_id":5,"order":5},{"display_type":"promotion","item":"7","item_type":"sku_id","pos_id":5,"order":6},{"display_type":"promotion","item":"5","item_type":"sku_id","pos_id":3,"order":7},{"display_type":"query","item":"6","item_type":"sku_id","pos_id":4,"order":8}],"ts":1616334674000}
         * {"common":{"ar":"230000","uid":"15","os":"Android 9.0","ch":"xiaomi","is_new":"0","md":"vivo iqoo3","mid":"mid_5","vc":"v2.1.134","ba":"vivo"},"page":{"page_id":"home","during_time":5363},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":5,"order":1},{"display_type":"recommend","item":"6","item_type":"sku_id","pos_id":4,"order":2},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":2,"order":3},{"display_type":"query","item":"9","item_type":"sku_id","pos_id":4,"order":4},{"display_type":"promotion","item":"9","item_type":"sku_id","pos_id":5,"order":5},{"display_type":"recommend","item":"5","item_type":"sku_id","pos_id":5,"order":6}],"ts":1616334676000}
         * {"common":{"ar":"110000","uid":"20","os":"Android 10.0","ch":"xiaomi","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_6","vc":"v2.1.134","ba":"Xiaomi"},"page":{"page_id":"home","during_time":4239},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":1,"order":1},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":1,"order":2},{"display_type":"promotion","item":"9","item_type":"sku_id","pos_id":4,"order":3},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":2,"order":4},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":1,"order":5},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":5,"order":6},{"display_type":"promotion","item":"5","item_type":"sku_id","pos_id":3,"order":7},{"display_type":"query","item":"7","item_type":"sku_id","pos_id":2,"order":8},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":1,"order":9},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":2,"order":10}],"ts":1616334677000}
         * {"common":{"ar":"370000","uid":"18","os":"Android 11.0","ch":"oppo","is_new":"1","md":"Huawei Mate 30","mid":"mid_14","vc":"v2.1.134","ba":"Huawei"},"page":{"page_id":"home","during_time":8359},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":4,"order":1},{"display_type":"promotion","item":"6","item_type":"sku_id","pos_id":5,"order":2},{"display_type":"promotion","item":"7","item_type":"sku_id","pos_id":4,"order":3},{"display_type":"promotion","item":"9","item_type":"sku_id","pos_id":1,"order":4},{"display_type":"promotion","item":"10","item_type":"sku_id","pos_id":4,"order":5}],"ts":1616334677000}
         * {"common":{"ar":"110000","uid":"22","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 8","mid":"mid_9","vc":"v2.1.134","ba":"iPhone"},"page":{"page_id":"home","during_time":12454},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":4,"order":1},{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":4,"order":2},{"display_type":"recommend","item":"7","item_type":"sku_id","pos_id":5,"order":3},{"display_type":"promotion","item":"3","item_type":"sku_id","pos_id":4,"order":4},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":3,"order":5},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":5,"order":6}],"ts":1616334678000}
         */
        env.execute();

    }
}
/**  部分数据
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 2021-09-21 21:12:49,561 WARN [org.apache.hadoop.hdfs.DataStreamer] - Abandoning BP-218572329-172.26.62.251-1615639213147:blk_1073777659_36858
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 2021-09-21 21:12:49,624 WARN [org.apache.hadoop.hdfs.DataStreamer] - Excluding datanode DatanodeInfoWithStorage[172.26.62.250:9866,DS-2fd1e2d2-4405-4753-becb-007dc8b7744e,DISK]
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 未访问：lastVisit:null|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 2021-09-21 21:13:10,708 WARN [org.apache.hadoop.hdfs.DataStreamer] - Abandoning BP-218572329-172.26.62.251-1615639213147:blk_1073777661_36860
 * 2021-09-21 21:13:10,810 WARN [org.apache.hadoop.hdfs.DataStreamer] - Excluding datanode DatanodeInfoWithStorage[172.26.62.249:9866,DS-3325ac86-21b2-4776-9ae9-c02a9bb9d5e0,DISK]
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 * 已访问：lastVisit:2021-03-21|| logDate：2021-03-21
 */