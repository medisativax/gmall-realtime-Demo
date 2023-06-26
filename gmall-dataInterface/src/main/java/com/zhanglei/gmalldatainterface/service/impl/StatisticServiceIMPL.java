package com.zhanglei.gmalldatainterface.service.impl;

import com.zhanglei.gmalldatainterface.bean.Statistic;
import com.zhanglei.gmalldatainterface.mapper.GMVMapper;
import com.zhanglei.gmalldatainterface.mapper.StatisticsMapper;
import com.zhanglei.gmalldatainterface.service.StatisticsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class StatisticServiceIMPL implements StatisticsService {
    @Autowired
    private StatisticsMapper statisticsMapper;

    @Override
    public List<Statistic> getStatistics(int date) {
        return statisticsMapper.selectStatistics(date);
    }
}
