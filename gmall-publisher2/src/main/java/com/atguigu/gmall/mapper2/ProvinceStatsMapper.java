package com.atguigu.gmall.mapper2;

import com.atguigu.gmall.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author sunzhipeng
 * @create 2021-09-25 14:39
 */
public interface ProvinceStatsMapper {
    @Select("select province_name,sum(order_amount) order_amount from province_stats_2021 where toYYYYMMDD(stt)=#{date} group by province_id,province_name")
    public List<ProvinceStats> selectProvinceStats(int date);
}
