package com.zhanglei.gmalldatainterface.service.impl;

import com.zhanglei.gmalldatainterface.bean.Statistic;
import com.zhanglei.gmalldatainterface.mapper.ChannelMapper;
import com.zhanglei.gmalldatainterface.mapper.StatisticsMapper;
import com.zhanglei.gmalldatainterface.service.ChannelService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class ChannelServerIMPL implements ChannelService {
    @Autowired
    private ChannelMapper channelMapper;

    @Override
    public List<Statistic> selectChannel(int date) {
        return channelMapper.selectChannel(date);
    }

}
