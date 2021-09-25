package com.atguigu.gmall.realtime.app.func;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.TableName;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Desc:  自定义维度异步查询的函数
 *  模板方法设计模式
 *      在父类中只定义方法的声明，让整个流程跑通
 *      具体的实现延迟到子类中实现
 *      该类继承异步方法类RichAsyncFunction，实现自定义维度查询接口
 * 其中RichAsyncFunction<IN,OUT>是Flink提供的异步方法类，此处因为是查询操作输入类和返回类一致，所以是<T,T>。
 *    RichAsyncFunction这个类要实现两个方法:
 *    open用于初始化异步连接池。
 *    asyncInvoke方法是核心方法，里面的操作必须是异步的，如果你查询的数据库有异步api也可以用线程的异步方法，如果没有异步方法，就要自己利用线程池等方式实现异步查询。
 *    核心的类是AsyncDataStream，这个类有两个方法一个是有序等待（orderedWait），一个是无序等待（unorderedWait）。
 * 	无序等待（unorderedWait）
 * 后来的数据，如果异步查询速度快可以超过先来的数据，这样性能会更好一些，但是会有乱序出现。
 * 	有序等待（orderedWait）
 * 严格保留先来后到的顺序，所以后来的数据即使先完成也要等前面的数据。所以性能会差一些。
 * 	注意
 * 	这里实现了用户维表的查询，那么必须重写装配结果join方法和获取查询rowkey的getKey方法。
 * 	方法的最后两个参数10, TimeUnit.SECONDS ，标识次异步查询最多执行10秒，否则会报超时异常。
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{

    //线程池对象的父接口生命（多态）
    private ExecutorService executorService;

    //维度的表名
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池对象
        System.out.println("初始化线程池对象");
        executorService = ThreadPoolUtil.getInstance();
    }

    /**
     * 发送异步请求的方法
     * @param obj     流中的事实数据
     * @param resultFuture      异步处理结束之后，返回结果
     * @throws Exception
     */
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        //发送异步请求
                        long start = System.currentTimeMillis();
                        //从流中事实数据获取key
                        String key = getKey(obj);

                        //根据维度的主键到维度表中进行查询
                        JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                        //System.out.println("维度数据Json格式：" + dimInfoJsonObj);

                        if(dimInfoJsonObj != null){
                            //维度关联  流中的事实数据和查询出来的维度数据进行关联
                            join(obj,dimInfoJsonObj);
                        }
                        //System.out.println("维度关联后的对象:" + obj);
                        long end = System.currentTimeMillis();
                        System.out.println("异步维度查询耗时" +(end -start)+"毫秒");
                        //将关联后的数据数据继续向下传递
                        resultFuture.complete(Arrays.asList(obj));
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(tableName + "维度异步查询失败");
                    }
                }
            }
        );
    }
}
