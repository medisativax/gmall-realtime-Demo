package com.zhanglei.gmall.realtime.util

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.alibaba.fastjson.JSONObject
import com.google.common.base.CaseFormat
import org.apache.commons.beanutils.BeanUtils

import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}
import scala.collection.mutable.ListBuffer

/** *
 * 当前工具类可以适用任何JDBC方式访问数据库中的任何查询语句
 *
 * @return 单行单列，单行多列，多行单列，多行多列
 */
object JDBCUtil {
  def queryList[T](connection: Connection, sql: String, clz: Class[T], upderScoreToCamel:Boolean): List[T] = {
    //创建集合用于存储结果数据
    val result = new ListBuffer[T]

    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      //编译sql语句
      preparedStatement = connection.prepareStatement(sql)
      //执行查询
      resultSet = preparedStatement.executeQuery()
      //获取元信息
      val metaData: ResultSetMetaData = resultSet.getMetaData
      //变量结果集，将每行数据转换成为T对象加入集合
      while (resultSet.next()){
        val t: T = clz.newInstance()
        for(i <- 0 until metaData.getColumnCount){
          //获取列名和列值
          var columnName: String = metaData.getColumnName(i+1)
          val value: AnyRef = resultSet.getObject(columnName)
          //是否需要下划线与驼峰命名转换
          if (upderScoreToCamel){
           columnName =  CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase())
          }
          //赋值
          BeanUtils.setProperty(t,columnName,value)
        }
        //将T对象放入集合
        result.append(t)
      }
    } catch {
      case e:Exception => throw new SQLException
    } finally {
      resultSet.close()
      preparedStatement.close()
    }
    // 返回结果数据
    result.toList
  }
}
