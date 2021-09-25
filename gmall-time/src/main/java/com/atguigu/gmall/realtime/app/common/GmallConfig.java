package com.atguigu.gmall.realtime.app.common;

/**
 * @author sunzhipeng
 * @create 2021-07-18 19:48
 * 项目配置的常量类
 */
public class GmallConfig {
    //hbase的命名空间
    public static final String HBASE_SCHEMA="GMALL2021_REALTIME";
    //Phonenix连接的服务器地址
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181";
    //ClickHouse的URL链接地址
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop102:8123/default";
}
