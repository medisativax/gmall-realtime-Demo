package com.zhanglei.gmalldatainterface.mapper;


import com.zhanglei.gmalldatainterface.bean.Statistic;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface ChannelMapper {

    @Select(" select ch,sum(uv_ct) uv from  dws_traffic_vc_ch_ar_is_new_page_view_window where toYYYYMMDD(stt)=#{date} group by ch order by uv desc limit 5;")
    List<Statistic> selectChannel(@Param("date")int date);
}
