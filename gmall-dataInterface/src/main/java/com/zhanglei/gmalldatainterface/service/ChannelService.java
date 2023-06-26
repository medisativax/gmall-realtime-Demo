package com.zhanglei.gmalldatainterface.service;

import com.zhanglei.gmalldatainterface.bean.Statistic;

import java.util.List;

public interface ChannelService {
    List<Statistic> selectChannel(int date);
}
