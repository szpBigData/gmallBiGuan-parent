package com.atguigu.gmall.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Author: Felix
 * Date: 2021/2/23
 * Desc: 用该注解标记的属性，不需要插入到ClickHouse
 * 由于之前的ClickhouseUtil工具类的写入机制就是把该实体类的所有字段按次序一次写入数据表。但是实体类有时会用到一些临时字段，计算中有用但是并不需要最终保存在临时表中。我们可以把这些字段做一些标识，然后再写入的时候判断标识来过滤掉这些字段。
 * 为字段打标识通常的办法就是给字段加个注解，这里我们就增加一个自定义注解@TransientSink来标识该字段不需要保存到数据表中。
 */
@Target(FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
