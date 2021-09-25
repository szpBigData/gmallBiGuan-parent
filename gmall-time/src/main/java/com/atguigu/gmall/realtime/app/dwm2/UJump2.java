package com.atguigu.gmall.realtime.app.dwm2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.ibm.icu.util.Output;
import org.apache.calcite.plan.RelRule;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;


/**
 * @author sunzhipeng
 * @create 2021-08-24 22:53
 * 用户跳出行为过滤
 * 跳出就是用户成功访问了网站的一个页面后就退出，不在继续访问网站的其它页面。而跳出率就是用跳出次数除以访问次数。
 * 关注跳出率，可以看出引流过来的访客是否能很快的被吸引，渠道引流过来的用户之间的质量对比，对于应用优化前后跳出率的对比也能看出优化改进的成果。
 *	该页面是用户近期访问的第一个页面
 * 这个可以通过该页面是否有上一个页面（last_page_id）来判断，如果这个表示为空，就说明这是这个访客这次访问的第一个页面。
 * 首次访问之后很长一段时间（自己设定），用户没继续再有其他页面的访问。
 * 这第一个特征的识别很简单，保留last_page_id为空的就可以了。
 * 最简单的办法就是Flink自带的CEP技术。这个CEP非常适合通过多条数据组合来识别某个事件。
 * 用户跳出事件，本质上就是一个条件事件加一个超时事件的组合。
 */
public class UJump2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //设置CK相关的参数
        //设置精准一次性保证（默认）  每5000ms开始一次checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //Checkpoint必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop103:8020/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME","test");
        //定义topic
        String sourceTopic="dwd_page_log";
        String groupId="user_jump_detail_group";
        //这个最好也要知道
        String sinkTopic="dwm_user_jump_detail";
        FlinkKafkaConsumer<String> source = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> streamSource = env.addSource(source);
        //对读取到的数据进行结构转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = streamSource.map(data -> JSON.parseObject(data));
        //由于这里涉及到时间的判断，
        // 所以必须设定数据流的EventTime和水位线。
        // 这里没有设置延迟时间，实际生产情况可以视乱序情况增加一些延迟。
        //增加延迟把forMonotonousTimestamps换为forBoundedOutOfOrderness即可。

        //注意：从Flink1.12开始，默认的时间语义就是事件时间，不需要额外指定；如果是之前的版本，需要通过如下语句指定事件时间语义
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //指定事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }
        ));
        //按照mid进行分组
        KeyedStream<JSONObject, String> keyByMidDss = jsonObjWithTSDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //            计算页面跳出明细，需要满足两个条件
        //                1.不是从其它页面跳转过来的页面，是一个首次访问页面
        //                        last_page_id == null
        //                2.距离首次访问结束后10秒内，没有对其它的页面再进行访问
        //
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //获取last_page_id
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        //判断是都为null将为空的保留，非空的过滤掉
                        if (lastPageId == null || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                }).next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //获取当前页面的id
                        String pageId = value.getJSONObject("page").getString("page_id");
                        //判断当前 页面id是否为null
                        if (pageId == null && pageId.length() > 0) {
                            return true;
                        }
                        return false;
                    }
                })
                //时间限制
                .within(Time.milliseconds(10000));
        //根据CEP进行筛选
        PatternStream<JSONObject> patternStream = CEP.pattern(keyByMidDss, pattern);
        OutputTag<String> timeoutputTag = new OutputTag<String>("timeout") {};
        //从筛选出来的流中提取数据，将超时数据放入侧输出流中
        SingleOutputStreamOperator<String> filterDS = patternStream.flatSelect(timeoutputTag, new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        //获取所有符合first的接送对象
                        List<JSONObject> jsonObjectList = pattern.get("first");
                        //注意在timeout方法中的数据都会被参数1中的标签标记
                        for (JSONObject jsonObject : jsonObjectList) {
                            out.collect(jsonObject.toJSONString());
                        }
                    }
                },
                //处理没有超时的数据
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                        //没有超时的数据，不在我们的统计范围之内 ，所以这里不需要写什么代码
                    }
                }
        );
        //从侧输出流获取超时数据
        DataStream<String> jumpDS = filterDS.getSideOutput(timeoutputTag);
        jumpDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        env.execute();
/**
 * {"common":{"ar":"440000","uid":"28","os":"Android 11.0","ch":"oppo","is_new":"1","md":"Redmi k30","mid":"mid_4","vc":"v2.0.1","ba":"Redmi"},"page":{"page_id":"home","during_time":16383},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":3,"order":1},{"display_type":"recommend","item":"2","item_type":"sku_id","pos_id":5,"order":2},{"display_type":"query","item":"10","item_type":"sku_id","pos_id":3,"order":3},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":1,"order":4},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":3,"order":5},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":1,"order":6},{"display_type":"query","item":"7","item_type":"sku_id","pos_id":4,"order":7},{"display_type":"promotion","item":"10","item_type":"sku_id","pos_id":2,"order":8}],"ts":1616336169000}
 * {"common":{"ar":"500000","uid":"10","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone X","mid":"mid_10","vc":"v2.1.132","ba":"iPhone"},"page":{"page_id":"home","during_time":5971},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":1,"order":1},{"display_type":"recommend","item":"4","item_type":"sku_id","pos_id":3,"order":2},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":1,"order":3},{"display_type":"query","item":"6","item_type":"sku_id","pos_id":1,"order":4},{"display_type":"recommend","item":"4","item_type":"sku_id","pos_id":3,"order":5},{"display_type":"promotion","item":"1","item_type":"sku_id","pos_id":1,"order":6},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":1,"order":7},{"display_type":"recommend","item":"6","item_type":"sku_id","pos_id":2,"order":8},{"display_type":"promotion","item":"4","item_type":"sku_id","pos_id":4,"order":9},{"display_type":"promotion","item":"10","item_type":"sku_id","pos_id":2,"order":10},{"display_type":"query","item":"6","item_type":"sku_id","pos_id":3,"order":11}],"ts":1616336171000}
 * {"common":{"ar":"500000","uid":"27","os":"Android 10.0","ch":"web","is_new":"0","md":"Redmi k30","mid":"mid_13","vc":"v2.1.111","ba":"Redmi"},"page":{"page_id":"home","during_time":11373},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":4,"order":1},{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":4,"order":2},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":5,"order":3},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":5,"order":4},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":4,"order":5},{"display_type":"promotion","item":"9","item_type":"sku_id","pos_id":3,"order":6},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":2,"order":7},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":4,"order":8},{"display_type":"recommend","item":"3","item_type":"sku_id","pos_id":5,"order":9},{"display_type":"query","item":"7","item_type":"sku_id","pos_id":5,"order":10},{"display_type":"recommend","item":"7","item_type":"sku_id","pos_id":4,"order":11}],"ts":1616336173000}
 * {"common":{"ar":"440000","uid":"10","os":"Android 11.0","ch":"oppo","is_new":"0","md":"vivo iqoo3","mid":"mid_1","vc":"v2.1.134","ba":"vivo"},"page":{"page_id":"home","during_time":18945},"displays":[{"display_type":"activity","item":"2","item_type":"activity_id","pos_id":3,"order":1},{"display_type":"promotion","item":"5","item_type":"sku_id","pos_id":2,"order":2},{"display_type":"promotion","item":"3","item_type":"sku_id","pos_id":4,"order":3},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":1,"order":4},{"display_type":"promotion","item":"2","item_type":"sku_id","pos_id":5,"order":5},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":3,"order":6},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":1,"order":7},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":5,"order":8},{"display_type":"query","item":"2","item_type":"sku_id","pos_id":2,"order":9},{"display_type":"query","item":"5","item_type":"sku_id","pos_id":3,"order":10}],"ts":1616336203000}
 * {"common":{"ar":"230000","uid":"46","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone Xs","mid":"mid_12","vc":"v2.1.134","ba":"iPhone"},"page":{"page_id":"home","during_time":13665},"displays":[{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":2,"order":1},{"display_type":"activity","item":"1","item_type":"activity_id","pos_id":2,"order":2},{"display_type":"promotion","item":"8","item_type":"sku_id","pos_id":4,"order":3},{"display_type":"promotion","item":"3","item_type":"sku_id","pos_id":3,"order":4},{"display_type":"recommend","item":"3","item_type":"sku_id","pos_id":3,"order":5},{"display_type":"promotion","item":"5","item_type":"sku_id","pos_id":2,"order":6},{"display_type":"query","item":"8","item_type":"sku_id","pos_id":1,"order":7},{"display_type":"query","item":"7","item_type":"sku_id","pos_id":2,"order":8},{"display_type":"query","item":"4","item_type":"sku_id","pos_id":3,"order":9},{"display_type":"query","item":"1","item_type":"sku_id","pos_id":1,"order":10},{"display_type":"query","item":"3","item_type":"sku_id","pos_id":4,"order":11},{"display_type":"promotion","item":"4","item_type":"sku_id","pos_id":5,"order":12}],"ts":1616336211000}
 */

    }
}
