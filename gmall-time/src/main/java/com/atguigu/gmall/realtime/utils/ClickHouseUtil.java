package com.atguigu.gmall.realtime.utils;
import com.atguigu.gmall.realtime.app.common.GmallConfig;
import com.atguigu.gmall.realtime.bean.TransientSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Author: Felix
 * Date: 2021/2/23
 * Desc: 操作ClickHouse的工具类
 * 之所以选用ReplacingMergeTree引擎主要是靠它来保证数据表的幂等性。
 * paritition by 把日期变为数字类型（如：20201126），用于分区。所以尽量保证查询条件尽量包含stt字段。
 * order by 后面字段数据在同一分区下，出现重复会被去重，重复数据保留ts最大的数据。
 * flink-connector-jdbc 是官方通用的jdbcSink包。只要引入对应的jdbc驱动，flink可以用它应对各种支持jdbc的数据库，比如phoenix也可以用它。但是这个jdbc-sink只支持数据流对应一张数据表。如果是一流对多表，就必须通过自定义的方式实现了，比如之前的维度数据。
 * 虽然这种jdbc-sink只能一流对一表，但是由于内部使用了预编译器，所以可以实现批量提交以优化写入速度。
 */
public class ClickHouseUtil {
    /**
     * 获取向Clickhouse中写入数据的SinkFunction
     *
     * @param sql
     * @param <T>
     * @return
     */
    /**
     *
     A.	JdbcSink.<T>sink( )的四个参数说明
     	参数1： 传入Sql，格式如：insert into xxx values(?,?,?,?)
     	参数2:  可以用lambda表达实现(jdbcPreparedStatement, t) -> t为数据对象，要装配到语句预编译器的参数中。
     	参数3：设定一些执行参数，比如重试次数，批次大小。
     	参数4：设定连接参数，比如地址，端口，驱动名。

     */
    public static <T> SinkFunction getJdbcSink(String sql) {
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(
                //要执行的SQL语句
                sql,
                //执行写入操作   就是将当前流中的对象属性赋值给SQL的占位符 insert into visitor_stats_0820 values(?,?,?,?,?,?,?,?,?,?,?,?)
                new JdbcStatementBuilder<T>() {
                    //obj  就是流中的一条数据对象
                    @Override
                    public void accept(PreparedStatement ps, T obj) throws SQLException {
                        //获取当前类中  所有的属性
                        Field[] fields = obj.getClass().getDeclaredFields();
                        //跳过的属性计数
                        int skipOffset = 0;

                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            //通过属性对象获取属性上是否有@TransientSink注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            //如果transientSink不为空，说明属性上有@TransientSink标记，那么在给?占位符赋值的时候，应该跳过当前属性
                            if (transientSink != null) {
                                skipOffset++;
                                continue;
                            }

                            //设置私有属性可访问
                            field.setAccessible(true);
                            try {
                                //获取属性值
                                Object o = field.get(obj);
                                ps.setObject(i + 1 - skipOffset, o);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                //构建者设计模式，创建JdbcExecutionOptions对象，给batchSize属性赋值，执行执行批次大小
                new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
                //构建者设计模式，JdbcConnectionOptions，给连接相关的属性进行赋值
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
        );
        return sinkFunction;
    }
}
