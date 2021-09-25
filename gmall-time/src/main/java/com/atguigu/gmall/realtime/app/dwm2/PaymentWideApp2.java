package com.atguigu.gmall.realtime.app.dwm2;

import com.alibaba.fastjson.JSON;

import com.atguigu.gmall.realtime.app.bean2.OrderWide;
import com.atguigu.gmall.realtime.app.bean2.PaymentInfo;
import com.atguigu.gmall.realtime.app.bean2.PaymentWide;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author sunzhipeng
 * @create 2021-08-22 20:13
 */
public class PaymentWideApp2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //1.3 检查点相关的配置
        //设置CK相关配置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend("hdfs://hadoop103:8020/gmall/flink/checkpoint/OrderWideApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME", "test");
        //TODO 2. 从kafka的主题中读取数据
        //2.1 声明相关的主题以及消费者组
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        String groupId = "paymentwide_app_group";
        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> paymentInfojsonDstream = env.addSource(paymentInfoSource);
        DataStreamSource<String> orderWidejsonDstream = env.addSource(orderWideSource);

        DataStream<PaymentInfo> paymentInfoDStream = paymentInfojsonDstream.map(data -> JSON.parseObject(data, PaymentInfo.class));
        DataStream<OrderWide> orderWideDstream = orderWidejsonDstream.map(data -> JSON.parseObject(data, OrderWide.class));
        //设置水位线
        SingleOutputStreamOperator<PaymentInfo> paymentInfoEventTimeDstream =
                paymentInfoDStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        (paymentInfo, ts) -> DateTimeUtil.toTs(paymentInfo.getCallback_time())
                                ));
        //设置水位线
        SingleOutputStreamOperator<OrderWide> orderInfoWithEventTimeDstream =
                orderWideDstream.assignTimestampsAndWatermarks(WatermarkStrategy.
                        <OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                (orderWide, ts) -> DateTimeUtil.toTs(orderWide.getCreate_time())
                        )
                );

        //设置分区键
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedStream= paymentInfoEventTimeDstream.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideKeyedStream = orderInfoWithEventTimeDstream.keyBy(OrderWide::getOrder_id);

        //对数据进行关联
        SingleOutputStreamOperator<PaymentWide> payment_wide_join = paymentInfoKeyedStream.intervalJoin(orderWideKeyedStream)
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                }).uid("payment_wide_join");
        //写入到kafka
        MyKafkaUtil.getKafkaSink(paymentWideSinkTopic);
        env.execute();


    }
}
