package com.zhanglei.gmalldatainterface.mapper;

import com.zhanglei.gmalldatainterface.bean.SpuBean;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface SpuMapper {
    @Select("select spu_name, sum(order_count) order_count,count(distinct user_id) uu_count,sum(order_amount) order_amount from dws_trade_user_spu_order_window where toYYYYMMDD(stt) =#{date} group by spu_id, spu_name order by order_amount desc;")
    List<SpuBean> selectSpu(@Param("date")int date);
}
