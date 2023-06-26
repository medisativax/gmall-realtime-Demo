package com.zhanglei.gmalldatainterface.service.impl;

import com.zhanglei.gmalldatainterface.bean.SpuBean;
import com.zhanglei.gmalldatainterface.mapper.SpuMapper;
import com.zhanglei.gmalldatainterface.service.SpuService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class SpuServiceIMPL implements SpuService {
    @Autowired
    protected SpuMapper spuMapper;

    @Override
    public List<SpuBean> selectSpu(int date) {
        return spuMapper.selectSpu(date);
    }
}
