package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author sunzhipeng
 * @create 2021-07-16 11:22
 * 操作kafka工具类
 */
public class MyKafkaUtil {
    private static String kafkaServer="hadoop101:9092,hadoop102:9092,hadoop103:9092";
    private static String DEFAULT_TOPIC="DEFAULT_DATA";
    //封装kafka消费者
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        Properties prop=new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop);
    }

    /**
     * 前者给定确定的Topic
     * 而后者除了缺省情况下会采用DEFAULT_TOPIC，一般情况下可以根据不同的业务数据在KafkaSerializationSchema中通过方法实现。
     * 可以查看一下FlinkKafkaProducer中的invoke方法源码
     */
    //封装kafka生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<>(kafkaServer,topic,new SimpleStringSchema());
    }
  //封装kafka   动态指定多个不同的主题
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        //设置生产数据超时时间  如果15分钟没有更新状态，则超时默认1分钟
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,kafkaSerializationSchema,properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
    //拼接kafka相关属性到DDL
    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka', " +
                " 'topic' = '"+topic+"',"   +
                " 'properties.bootstrap.servers' = '"+ kafkaServer +"', " +
                " 'properties.group.id' = '"+groupId+ "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'  ";
        return  ddl;
    }
}
