package com.atguigu.gmall.service2;

import com.atguigu.gmall.bean.ProvinceStats;

import java.util.List;

/**
 * @author sunzhipeng
 * @create 2021-09-25 14:58
 */
public interface ProvinceStatsService {
    public List<ProvinceStats> selectProvinceStats(int date);
}
