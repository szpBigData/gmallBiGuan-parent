package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @create 2021-07-19 22:30
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //1.3 检查点相关的配置
        //设置CK相关配置
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        StateBackend fsStateBackend = new FsStateBackend("hdfs://hadoop103:8020/gmall/flink/checkpoint/OrderWideApp");
//        env.setStateBackend(fsStateBackend);
//        System.setProperty("HADOOP_USER_NAME", "test");


        //TODO 2. 从kafka的主题中读取数据
        //2.1 声明相关的主题以及消费者组
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        String groupId = "paymentwide_app_group";
        //封装kafka消费者  读取支付流数据
        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);
        DataStreamSource<String> paymentInfojsonDstream = env.addSource(paymentInfoSource);
        //对读取的支付数据进行转换
        DataStream<PaymentInfo> paymentInfoDStream=paymentInfojsonDstream.map(jsonstring -> JSON.parseObject(jsonstring, PaymentInfo.class));
        //封装kafka消费者，读取订单宽表流数据
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStream<String> orderWidejsonDstream = env.addSource(orderWideSource);
        //对读取的订单宽表数据进行转换
        DataStream<OrderWide> orderWideDstream =
                orderWidejsonDstream.map(jsonString -> JSON.parseObject(jsonString, OrderWide.class));
        //设置水位线
        SingleOutputStreamOperator<PaymentInfo> paymentInfoEventTimeDstream =
                paymentInfoDStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        (paymentInfo, ts) -> DateTimeUtil.toTs(paymentInfo.getCallback_time())
                                ));

        SingleOutputStreamOperator<OrderWide> orderInfoWithEventTimeDstream =
                orderWideDstream.assignTimestampsAndWatermarks(WatermarkStrategy.
                        <OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                (orderWide, ts) -> DateTimeUtil.toTs(orderWide.getCreate_time())
                        )
                );
        //设置分区键
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedStream =
                paymentInfoEventTimeDstream.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideKeyedStream =
                orderInfoWithEventTimeDstream.keyBy(OrderWide::getOrder_id);
       //关联数据
        //关联数据
        SingleOutputStreamOperator<PaymentWide> paymentWideDstream =
                paymentInfoKeyedStream.intervalJoin(orderWideKeyedStream).
                        between(Time.seconds(-1800), Time.seconds(0)).
                        process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo,
                                                       OrderWide orderWide,
                                                       Context ctx, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }).uid("payment_wide_join");
        SingleOutputStreamOperator<String> paymentWideStringDstream = paymentWideDstream.map(paymentWide -> JSON.toJSONString(paymentWide));
        paymentWideStringDstream.print("pay:");
        paymentWideStringDstream.addSink(
                MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        env.execute();


    }
}
