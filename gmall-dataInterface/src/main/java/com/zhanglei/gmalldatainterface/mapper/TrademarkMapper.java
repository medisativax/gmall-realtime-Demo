package com.zhanglei.gmalldatainterface.mapper;

import com.zhanglei.gmalldatainterface.bean.Statistic;
import com.zhanglei.gmalldatainterface.bean.Trademark;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TrademarkMapper {
    @Select("select trademark_name, order_count, uu_count, order_amount, refund_count, refund_uu_count\n" +
            "from (select trademark_id,\n" +
            "             trademark_name,\n" +
            "             sum(order_count)        order_count,\n" +
            "             count(distinct user_id) uu_count,\n" +
            "             sum(order_amount)\n" +
            "                               order_amount\n" +
            "      from dws_trade_user_spu_order_window where toYYYYMMDD(stt) = #{date}\n" +
            "      group by trademark_id, trademark_name) oct full outer join (select trademark_id,trademark_name,sum(refund_count)\n" +
            "            refund_count,count(distinct user_id) refund_uu_count from dws_trade_trademark_category_user_refund_window where toYYYYMMDD(stt) = #{date}\n" +
            "            group by trademark_id, trademark_name) rct\n" +
            "on oct.trademark_id = rct.trademark_id;")
    List<Trademark> selectTrademark(@Param("date")int date);
}
