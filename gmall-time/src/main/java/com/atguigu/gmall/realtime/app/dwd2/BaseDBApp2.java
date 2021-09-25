package com.atguigu.gmall.realtime.app.dwd2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author sunzhipeng
 * @create 2021-08-14 8:00
 * 业务数据的变化，我们可以通过Maxwell采集到，但是MaxWell是把全部数据统一写入一个Topic中,
 * 这些数据包括业务数据，也包含维度数据，这样显然不利于日后的数据处理，所以这个功能是从Kafka的业务数据ODS层读取数据，
 * 经过处理后，将维度数据保存到Hbase，将事实数据写回Kafka作为业务数据的DWD层。
 */
public class BaseDBApp2 {
    public static void main(String[] args) throws Exception{
        //TODO 0.基本环境准备
        //Flink流式处理环境
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
        //接收kafka数据，过滤空值
        //定义消费者组以及指定消费主题
        String topic = "ods_base_db_m";
        String groupId = "ods_base_group";
        //从kafka读取数据，消费，添加到环境中
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStream<String> jsonDstream = env.addSource(kafkaSource);
        //进行结构转换
        DataStream<JSONObject> jsonStream = jsonDstream.map(data -> JSON.parseObject(data));
        //过滤为空或者 长度不足的数据
        SingleOutputStreamOperator<JSONObject> filteredDstream = jsonStream.filter(
                jsonObject -> {
                    boolean flag = jsonObject.getString("table") != null
                            && jsonObject.getJSONObject("data") != null
                            && jsonObject.getString("data").length() > 3;
                    return flag;
                }) ;
        //TODO 2.动态分流  事实表放入主流，作为DWD层；维度表放入侧输出流
//定义输出到Hbase的侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {};

//使用自定义ProcessFunction进行分流处理
        SingleOutputStreamOperator<JSONObject> kafkaDStream  = filteredDstream.process(new TableProcessFunction(hbaseTag));

//获取侧输出流，即将通过Phoenix写到Hbase的数据
        DataStream<JSONObject> hbaseDStream = kafkaDStream.getSideOutput(hbaseTag);
        hbaseDStream.addSink(new DimSink());
        //3.将侧输出流数据写入hbase
        hbaseDStream.addSink(new DimSink());
        //4.将主流数据写入kafka
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("启动kafka sink");
            }

            //从每条数据得到该条数据应送往的主题名
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                String topic = jsonObject.getString("sink_table");
                JSONObject dataJSONObj = jsonObject.getJSONObject("data");
                return new ProducerRecord(topic, dataJSONObj.toJSONString().getBytes());
            }
        });

        env.execute();


    }
}
