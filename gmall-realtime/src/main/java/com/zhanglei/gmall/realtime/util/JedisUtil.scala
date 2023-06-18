package com.zhanglei.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisUtil {
  var jedisPool: JedisPool = _
  def initJedisPoll(): Unit ={
    val pollConfig: JedisPoolConfig = new JedisPoolConfig
    pollConfig.setMaxTotal(100)
    pollConfig.setMaxIdle(5)
    pollConfig.setMinIdle(5)
    pollConfig.setBlockWhenExhausted(true)
    pollConfig.setMaxWaitMillis(2000)
    pollConfig.setTestOnBorrow(true)
    jedisPool = new JedisPool(pollConfig,"hadoop01",6379,10000)
  }
  def getJedis(): Jedis ={
    if (jedisPool == null){
      initJedisPoll()
    }
    jedisPool.getResource
  }
}
