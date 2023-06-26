package com.zhanglei.gmalldatainterface.service;

import com.zhanglei.gmalldatainterface.bean.Statistic;

import java.util.List;

public interface StatisticsService {
    List<Statistic> getStatistics(int date);
}
