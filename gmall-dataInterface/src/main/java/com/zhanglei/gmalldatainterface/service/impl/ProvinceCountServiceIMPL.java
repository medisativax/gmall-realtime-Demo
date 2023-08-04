package com.zhanglei.gmalldatainterface.service.impl;

import com.zhanglei.gmalldatainterface.bean.Statistic;
import com.zhanglei.gmalldatainterface.mapper.ProvinceCountMapper;
import com.zhanglei.gmalldatainterface.service.ProvinceCountService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class ProvinceCountServiceIMPL implements ProvinceCountService {
    @Autowired
    private ProvinceCountMapper provinceCountMapper;
    @Override
    public List<Statistic> selectProvinceCount(int date) {
        return provinceCountMapper.selectProvinceCount(date);
    }
}
