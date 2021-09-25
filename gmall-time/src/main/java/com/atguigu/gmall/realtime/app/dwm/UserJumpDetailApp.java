package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
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
 * @create 2021-07-18 11:25
 * 用户跳出行为过滤
 * 跳出就是用户成功访问了网站的一个页面后就退出，不在继续访问网站的其它页面。而跳出率就是用跳出次数除以访问次数。
 * 首先要识别哪些是跳出行为，要把这些跳出的访客最后一个访问的页面识别出来。那么要抓住几个特征：
 * 	该页面是用户近期访问的第一个页面
 * 这个可以通过该页面是否有上一个页面（last_page_id）来判断，如果这个表示为空，就说明这是这个访客这次访问的第一个页面。
 * 	首次访问之后很长一段时间（自己设定），用户没继续再有其他页面的访问。
 * 这第一个特征的识别很简单，保留last_page_id为空的就可以了。但是第二个访问的判断，其实有点麻烦，首先这不是用一条数据就能得出结论的，需要组合判断，要用一条存在的数据和不存在的数据进行组合判断。而且要通过一个不存在的数据求得一条存在的数据。更麻烦的他并不是永远不存在，而是在一定时间范围内不存在。那么如何识别有一定失效的组合行为呢？
 * 最简单的办法就是Flink自带的CEP技术。这个CEP非常适合通过多条数据组合来识别某个事件。
 * 用户跳出事件，本质上就是一个条件事件加一个超时事件的组合。
 *
 *
 * 何时指定事件时间字段？
 */
public class UserJumpDetailApp {
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

        //我需要知道这个的格式
        String sourceTopic="dwd_page_log";
        String groupId="user_jump_detail_group";
        //这个最好也要知道
        String sinkTopic="dwm_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic,groupId);
        DataStreamSource<String> dataStream = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStream.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            return jsonObject;
        });
        //注意：从Flink1.12开始，默认的时间语义就是事件时间，不需要额外指定；如果是之前的版本，需要通过如下语句指定事件时间语义
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //指定事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjectSingleOutputStreamOperator = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //分组按照mid
        KeyedStream<JSONObject, String> keyedStream = jsonObjectSingleOutputStreamOperator.keyBy(data -> data.getJSONObject("common").getString("mid"));
        //定义CEP规则
        /*
            计算页面跳出明细，需要满足两个条件
                1.不是从其它页面跳转过来的页面，是一个首次访问页面
                        last_page_id == null
                2.距离首次访问结束后10秒内，没有对其它的页面再进行访问
        */
        //TODO 6.配置CEP表达式
        Pattern<JSONObject,JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                           @Override
                           public boolean filter(JSONObject jsonObject) throws Exception {
                               //获取last_page_id
                               String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                               //判断是否为null  将为空的保留    非空的过滤掉
                               if (lastPageId == null || lastPageId.length() == 0) {
                                   return true;
                               }
                               return false;
                           }
                       }
                )
                .next("next")
                .where(
                        //模式2 ：判读是否对页面做了访问
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject value) throws Exception {
                                //获取当前页面的当前id
                                String pageId = value.getJSONObject("page").getString("page_id");
                                //判断当前访问的页面id是否为null
                                if (pageId != null && pageId.length() > 0) {
                                    return true;
                                }
                                return false;
                            }
                        })
                //时间限制模式
                .within(Time.milliseconds(10000));

        //根据CEP表达式筛选流
        PatternStream<JSONObject> patternStream= CEP.pattern(keyedStream,pattern);

        //定义一条侧输出流
        OutputTag<String> timeoutputTag = new OutputTag<String>("timeout") {};

        SingleOutputStreamOperator<String> filterDS = patternStream.flatSelect(timeoutputTag,
                //处理超时数据
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        //获取所有符合first的Json对象
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
                        //没有超时的数据，不在我们的统计范围之内，所以这里不需要写什么代码

                    }
                }
        );
        //从侧输出流中获取超时数据
        DataStream<String> jumpDS = filterDS.getSideOutput(timeoutputTag);

        //将跳出的数据写回到kafka 的DWM层
        jumpDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        env.execute();


    }
}
/**
 * 6)	提取命中的数据
 * 	设定超时时间标识 timeoutTag
 * 	flatSelect方法中，实现PatternFlatTimeoutFunction中的timeout方法。
 * 	所有out.collect的数据都被打上了超时标记
 * 	本身的flatSelect方法因为不需要未超时的数据所以不接受数据。
 * 	通过SideOutput侧输出流输出超时数据
 */