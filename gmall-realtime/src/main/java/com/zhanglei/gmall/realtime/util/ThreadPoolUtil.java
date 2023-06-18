package com.zhanglei.gmall.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("AlibabaCommentsMustBeJavadocFormat")
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor;

    public ThreadPoolUtil() {
    }

    // 懒汉式
    public static ThreadPoolExecutor getThreadPoolExecutor(){
        if (threadPoolExecutor == null){
            synchronized (ThreadPoolUtil.class){
                if (threadPoolExecutor == null){
                    //noinspection AlibabaThreadShouldSetName
                    threadPoolExecutor = new ThreadPoolExecutor(4,
                            20,
                            100,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }
}
