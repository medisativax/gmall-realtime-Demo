package com.zhanglei.gmall.realtime.util

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhanglei.gmall.realtime.common.GmallConfig
import redis.clients.jedis.Jedis

import java.sql.Connection

object DimUtil {
  def getDimInfo(connection: Connection, tableName: String, key: String): JSONObject = {
    //先查询Redis
    val jedis: Jedis = JedisUtil.getJedis()
    val redisKey = "DIM:" + tableName + ":" + key
    val dimJsonStr: String = jedis.get(redisKey)
    if (dimJsonStr != null){
      // 重置过期时间
      jedis.expire(redisKey, 24 * 60 *60)
      //归还连接
      jedis.close()
      //返回数据
      return JSON.parseObject(dimJsonStr)
    }

    //拼接sql语句
    val querySql: String = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + key + "'"

    //查询数据
    val queryList: List[JSONObject] = JDBCUtil.queryList(connection, querySql, classOf[JSONObject], upderScoreToCamel = false)
    //将从Phoenix查询到数据写入Redis
    val dimInfo: JSONObject = queryList.head
    jedis.set(redisKey,dimInfo.toJSONString)
    jedis.expire(redisKey, 24 * 60 *60)
    jedis.close()
    //返回结果
    dimInfo
  }
  def delDimInfo(tableName: String, key: String): Unit ={
    //获取连接
    val jedis: Jedis = JedisUtil.getJedis()
    //删除数据
    jedis.del("DIM:" + tableName + ":" + key)
    //归还连接
    jedis.close()
  }

  def main(args: Array[String]): Unit = {
    val source: DruidDataSource = DruidDSUtil.createDataSource()
    val connection: DruidPooledConnection = source.getConnection()
    val start: Long = System.currentTimeMillis()
    val jSONObject: JSONObject = getDimInfo(connection, "DIM_USER_INFO", "829")
    val end: Long = System.currentTimeMillis()
    val start1: Long = System.currentTimeMillis()
    val jSONObject1: JSONObject = getDimInfo(connection, "DIM_USER_INFO", "839")
    val end1: Long = System.currentTimeMillis()
    println(jSONObject)
    print(jSONObject1)
    println(end - start) //350 334 328 313
    print(end1-start1)

    connection.close()
  }
}
