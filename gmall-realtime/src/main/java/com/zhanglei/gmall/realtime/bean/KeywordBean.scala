package com.zhanglei.gmall.realtime.bean

import lombok.{AllArgsConstructor, Data, NoArgsConstructor}

@Data
@AllArgsConstructor
@NoArgsConstructor
case class KeywordBean(
                        // 窗口起始时间
                        var stt: String,
                        // 窗口结束时间
                        var edt: String,
                        // 关键词来源
//                        @TransientSink
                        var source: String,
                        // 关键词
                        var keyword: String,
                        // 关键词出现频次
                        var keyword_count: Long,
                        // 时间戳
                        var ts: Long,
                      )


