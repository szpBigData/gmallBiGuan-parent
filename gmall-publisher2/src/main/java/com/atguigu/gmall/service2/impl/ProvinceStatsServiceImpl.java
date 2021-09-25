package com.atguigu.gmall.service2.impl;

import com.atguigu.gmall.bean.ProvinceStats;
import com.atguigu.gmall.mapper2.ProvinceStatsMapper;
import com.atguigu.gmall.service2.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author sunzhipeng
 * @create 2021-09-25 15:28
 */
public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    @Autowired
    ProvinceStatsMapper statsMapper;
    @Override
    public List<ProvinceStats> selectProvinceStats(int date) {
        return statsMapper.selectProvinceStats(date);
    }
}
