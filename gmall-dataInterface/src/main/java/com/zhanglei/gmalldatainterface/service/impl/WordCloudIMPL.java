package com.zhanglei.gmalldatainterface.service.impl;

import com.zhanglei.gmalldatainterface.bean.Statistic;
import com.zhanglei.gmalldatainterface.mapper.ChannelMapper;
import com.zhanglei.gmalldatainterface.mapper.WordCloudMapper;
import com.zhanglei.gmalldatainterface.service.WordCloudService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class WordCloudIMPL implements WordCloudService {
    @Autowired
    private WordCloudMapper wordCloudMapper;

    @Override
    public List<Statistic> selectWord(int date) {
        return wordCloudMapper.selectWord(date);
    }
}
