package com.zhanglei.gmalldatainterface.service;

import com.zhanglei.gmalldatainterface.bean.Statistic;

import java.util.List;

public interface WordCloudService {
    List<Statistic> selectWord(int date);
}
