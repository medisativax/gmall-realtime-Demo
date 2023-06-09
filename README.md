



# 练习·尚硅谷大数据项目 （电商平台）



## 第一章、项目框架设计

### 1.1 框架选取

```
 数据采集：Flume，Kafka，sqoop
 数据存储：Mysql，HDFS，HBase，Redis，MongoBD
 数据计算：Hive，Tez，Sprak，Flink
 数据查询：Presto，Kylin
 数据可视化：Echarts，Superset
 任务调度：Azkaban
 集群监控：Zabbix，Prometheus
 元数据管理：Atlas
 权限管理：Ranger
```

### 1.2 框架示意图

![](/img/Snipaste_2023-03-09_19-54-30.png)

### 1.3 集群规划

|  hadoop01   |    Hadoop02     |     hadoop03      |
| :---------: | :-------------: | :---------------: |
|  namenode   |                 |                   |
|             |                 | SecondaryNameNode |
|  datanode   |    datanode     |     datanode      |
| nodemanager |   nodemanager   |    nodemanager    |
|             | resourcemanager |                   |
|     zk      |       zk        |        zk         |
|    flume    |      flume      |       flume       |
|    kafka    |      kafka      |       kafka       |
|    hive     |                 |                   |
|    mysql    |                 |                   |
|    sqoop    |                 |                   |



## 第二章、日志数据采集集群搭建

### 2.0 工作流程图

![image-20230314144721711](/img/image-20230314144721711.png)

### 2.1 SSH免密登录

```
ssh-keygen -t rsa								--生成公钥与私钥
ssh-copy-id hadoop01
ssh-copy-id hadoop02
ssh-copy-id hadoop03							--在三台机器上，分别执行一次
```

### 2.2 编写分发脚本

```shell
rsync -av /路径 /路径								--同步文件命令
```

```shell
vim /root/bin/xsync

#!/bin/bash

#1. 判断参数个数
if [ $# -lt 1 ]
then
    echo Not Enough Arguement!
    exit;
fi

#2. 遍历集群所有机器
for host in hadoop01 hadoop02 hadoop03
do
    echo ====================  $host  ====================
    #3. 遍历所有目录，挨个发送

    for file in $@
    do
        #4. 判断文件是否存在
        if [ -e $file ]
            then
                #5. 获取父目录
                pdir=$(cd -P $(dirname $file); pwd)

                #6. 获取当前文件的名称
                fname=$(basename $file)
                ssh $host "mkdir -p $pdir"
                rsync -av $pdir/$fname $host:$pdir
            else
                echo $file does not exists!
        fi
    done
done
```

```shell
chmod +x /root/bin/xsync
xsync /root/bin/xsync
```

### 2.3 安装JDK

**卸载centos自带JDK**

```shell
rpm -qa | grep i java | xargs -n1 rpm -e --nodeps						--三台机器上都做一遍
```

**用XShell工具将JDK安装包导入到hadoop01的/opt/software文件夹下面，解压**

```shell
tar -zxvf /opt/software/jdk-8u212-linux-x64.tar.gz -C /opt/module/					--解压JDK到指定目录里
```

**配置JDK环境变量**

```shell
vim /etc/profile.d/my_env.sh

#JAVA_HOME
export JAVA_HOME=/opt/module/jdk1.8.0_212
export PATH=$PATH:$JAVA_HOME/bin

export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root
export HDFS_JOURNALNODE_USER=root
export HDFS_ZKFC_USER=root
```

**使环境变量生效**

```shell
source /etc/profile  										-- 三台机器上都要使用
```

**验证JDK是否安装成功**

```shell
java -verison
```

**分发环境变量配置，JDK**

```shell
xsync /etc/profile.d/my_env.sh
xsync /opt/module/jdk1.8.0_212
```

### 2.4 编写查看所有进程脚本

```shell
vim /root/bin/jpsall

#! /bin/bash
 
for i in hadoop01 hadoop02 hadoop03
do
    echo --------- $i ----------
    ssh $i "jps"
done
```

**修改脚本执行权限**

```shell
chmod +x /root/bin/jpsall
```

**执行且测试脚本**

```shell
jpsall
```

### 2.5 安装Hadoop

**节点规划**

|      |      服务器hadoop01      |           服务器hadoop02           |          服务器hadoop03           |
| ---- | :----------------------: | :--------------------------------: | :-------------------------------: |
| HDFS | NameNode  <br />DataNode |              DataNode              | DataNode<br />  SecondaryNameNode |
| Yarn |       NodeManager        | Resourcemanager<br />  NodeManager |            NodeManager            |

**用XShell工具将Hadoop安装包导入到hadoop01的/opt/software文件夹下面，解压**

```shell
tar -zxvf /opt/software/hadoop-3.1.3.tar.gz -C /opt/module/				--解压Hadoop到指定目录
```

**将Hadoop添加到环境变量**

```shell
vim /etc/profile.d/my_env.sh

#HADOOP_HOME
HADOOP_HOME=/opt/module/hadoop-3.1.3
PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
```

**分发环境变量**

```shell
xsync /etc/profile.d/my_env.sh 
```

**使环境变量生效**

```shell
source /etc/profile.d/my_env.sh  										-- 三台机器上都要使用
```

**验证Hadoop是否安装成功**

```shell
hadoop version
```

#### 2.5.1 更改Hadoop配置文件

**修改配置文件core-site.xml**

```xml
vim /opt/module/hadoop-3.1.3/etc/hadoop/core-site.xml

<configuration>
 	<!-- 指定NameNode的地址 -->
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop01:8020</value>
    </property>

    <!-- 指定hadoop数据的存储目录 -->
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/module/hadoop-3.1.3/data</value>
    </property>

    <!-- 配置HDFS网页登录使用的静态用户为root -->
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>root</value>
    </property>
    <!-- 整合hive 用户代理设置 -->
    <property>
        <name>hadoop.proxyuser.root.hosts</name>
        <value>*</value>
    </property>
    <property>
        <name>hadoop.proxyuser.root.groups</name>
        <value>*</value>
    </property>
</configuration>
```

**修改配置文件hdfs-site.xml**

```xml
vim /opt/module/hadoop-3.1.3/etc/hadoop/hdfs-site.xml

<configuration>
    <!-- nn web端访问地址-->
    <property>
    	<name>dfs.namenode.http-address</name>
        <value>hadoop01:9870</value>
    </property>
    <!-- 2nn web端访问地址-->
    <property>
        <name>dfs.namenode.secondary.http-address</name>
		<value>hadoop03:9868</value>
    </property>
</configuration>
```

**修改配置文件yarn-site.xml**

```xml
vim /opt/module/hadoop-3.1.3/etc/hadoop/yarn-site.xml

<configuration>
    <!-- 指定MR走shuffle -->
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <!-- 环境变量的继承 -->
    <property>
        <name>yarn.nodemanager.env-whitelist</name>	<value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>
    <!-- 指定ResourceManager的地址-->
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>hadoop02</value>
    </property>
	<!--是否启动一个线程检查每个任务正使用的物理内存量，如果任务超出分配值，则直接将其杀掉，默认是 true -->
    <property>
    	<name>yarn.nodemanager.pmem-check-enabled</name>
    	<value>false</value>
    </property>
	<!--是否启动一个线程检查每个任务正使用的虚拟内存量，如果任务超出分配值，则直接将其杀掉，默认是 true -->
    <property>
    	<name>yarn.nodemanager.vmem-check-enabled</name>
    	<value>false</value>
    </property>
</configuration>
```

**修改配置文件mapred-site.xml**

```xml
vim /opt/module/hadoop-3.1.3/etc/hadoop/mapred-site.xml

<configuration>
    <!-- 指定MapReduce程序运行在Yarn上 -->
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <!-- 历史服务器端地址 -->
    <property>
        <name>mapreduce.jobhistory.address</name>
        <value>hadoop01:10020</value>
    </property>
    <!-- 历史服务器web端地址 -->
    <property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>hadoop01:19888</value>
    </property>
</configuration>
```

**修改配置文件workers(slaves)  文件编写中不许存在空格**

```shell
vim /opt/module/hadoop-3.1.3/etc/hadoop/workers

hadoop01
hadoop02
hadoop03
```

**分发文件Hadoop给其他机器**

```
xsync /opt/module/hadoop-3.1.3
```

**启动集群**

```shell
hdfs namenode -format			--如果集群是第一次启动，格式化集群
```

```shell
start-dfs.sh	(myhadoo.sh start)				--启动HDFS
start-yarn.sh		
stop-dfs.sh		(myhadoo.sh stop)				--启动Yarn
stop-yarn.sh						
```

#### **2.5.2 编写Hadoop集群启动停止脚本**

```shell
vim /root/bin/myhadoop.sh

#!/bin/bash

if [ $# -lt 1 ]
then
    echo "No Args Input..."
    exit ;
fi

case $1 in
"start")
        echo " =================== 启动 hadoop集群 ==================="

        echo " --------------- 启动 hdfs ---------------"
        ssh hadoop01 "/opt/module/hadoop-3.1.3/sbin/start-dfs.sh"
        echo " --------------- 启动 yarn ---------------"
        ssh hadoop02 "/opt/module/hadoop-3.1.3/sbin/start-yarn.sh"
        echo " --------------- 启动 historyserver ---------------"
        ssh hadoop01 "/opt/module/hadoop-3.1.3/bin/mapred --daemon start historyserver"
;;
"stop")
        echo " =================== 关闭 hadoop集群 ==================="

        echo " --------------- 关闭 historyserver ---------------"
        ssh hadoop01 "/opt/module/hadoop-3.1.3/bin/mapred --daemon stop historyserver"
        echo " --------------- 关闭 yarn ---------------"
        ssh hadoop02 "/opt/module/hadoop-3.1.3/sbin/stop-yarn.sh"
        echo " --------------- 关闭 hdfs ---------------"
        ssh hadoop01 "/opt/module/hadoop-3.1.3/sbin/stop-dfs.sh"
;;
*)
    echo "Input Args Error..."
;;
esac

chmod +x /root/bin/myhadoop.sh
```

**验证集群是否启动成功**

```
jpsall
```

![](/img/image-20230309211008552.png)

![image-20230309211034040](/img/image-20230309211034040.png)

![image-20230309211112560](/img/image-20230309211112560.png)

### 2.6 安装ZK（Zookeeper）

**节点规划**

| 服务器hadoop01 | 服务器hadoop02 | 服务器hadoop03 |
| -------------- | -------------- | -------------- |
| Zookeeper      | Zookeeper      | Zookeeper      |

**用XShell工具将ZooKeeper安装包导入到hadoop01的/opt/software文件夹下面，解压**

```shell
tar -zxvf /opt/software/apache-zookeeper-3.5.7-bin.tar.gz -C /opt/module/  		--解压ZK到指定目录
mv /opt/module/apache-zookeeper-3.5.7-bin /opt/module/zookeeper-3.5.7 	  --更改文件夹名字
```

**配置**

```shell
mkdir /opt/module/zookeeper-3.5.7/ZKdata
echo '1'>/opt/module/zookeeper-3.5.7/ZKdata/myid
```

#### **2.6.1 配置zoo.cfg文件**

```shell
mv /opt/module/zookeeper-3.5.7/conf/zoo_sample.cfg /opt/module/zookeeper-3.5.7/conf/zoo.cfg
vim /opt/module/zookeeper-3.5.7/conf/zoo.cfg

dataDir=/opt/module/zookeeper-3.5.7/ZKdata

server.1=hadoop01:2888:3888
server.2=hadoop02:2888:3888
server.3=hadoop03:2888:3888
```

**同步ZK文件到其他集群**

```shell
xsync /opt/module/zookeeper-3.5.7
```

**修改其他机器服务器编号**

```shell
ssh hadoop02 "echo '2'>/opt/module/zookeeper-3.5.7/ZKdata/myid"
ssh hadoop03 "echo '3'>/opt/module/zookeeper-3.5.7/ZKdata/myid"
```

####  **2.6.2 编写ZK集群启动停止脚本**

```shell
vim /root/bin/zk.sh

#!/bin/bash

case $1 in
"start"){
	for i in hadoop01 hadoop02 hadoop03
	do
        echo ---------- zookeeper $i 启动 ------------
		ssh $i "/opt/module/zookeeper-3.5.7/bin/zkServer.sh start"
	done
};;
"stop"){
	for i in hadoop01 hadoop02 hadoop03
	do
        echo ---------- zookeeper $i 停止 ------------    
		ssh $i "/opt/module/zookeeper-3.5.7/bin/zkServer.sh stop"
	done
};;
"status"){
	for i in hadoop01 hadoop02 hadoop03
	do
        echo ---------- zookeeper $i 状态 ------------    
		ssh $i "/opt/module/zookeeper-3.5.7/bin/zkServer.sh status"
	done
};;
esac

chmod +x /root/bin/zk.sh
```

**测试脚本和服务器是否正常启动**

```xml
zk.sh start
zk.sh status
zk.sh stop
```

![image-20230309214430225](/img/image-20230309214430225.png)

### 2.7 安装Kafka

**节点规划**

| hadoop01 | hadoop02 | hadoop03 |
| :------: | :------: | :------: |
|    zk    |    zk    |    zk    |
|  kafka   |  kafka   |  kafka   |

**用XShell工具将Kafka安装包导入到hadoop01的/opt/software文件夹下面，解压**

```shell
tar -zxvf /opt/software/kafka_2.11-2.4.1.tgz -C /opt/module/		--解压Kafka到指定目录
mv /opt/module/kafka_2.11-2.4.1/ /opt/module/kafka					--更改文件夹名字
```

**在/opt/module/kafka 目录下创建logs文件夹**

```shell
mkdir /opt/module/kafka/logs
```

#### **2.7.1 修改Kafka中的配置文件**

```shell
>/opt/module/kafka/config/server.properties
vim /opt/module/kafka/config/server.properties

#broker 的全局唯一编号，不能重复，只能是数字。
broker.id=0
#处理网络请求的线程数量
num.network.threads=3
#用来处理磁盘 IO 的线程数量
num.io.threads=8
#发送套接字的缓冲区大小
socket.send.buffer.bytes=102400
#接收套接字的缓冲区大小
socket.receive.buffer.bytes=102400
#请求套接字的缓冲区大小
socket.request.max.bytes=104857600
#kafka 运行日志(数据)存放的路径，路径不需要提前创建，kafka 自动帮你创建，可以配置多个磁盘路径，路径与路径之间可以用"，"分隔
log.dirs=/opt/module/kafka/datas
#topic 在当前 broker 上的分区个数
num.partitions=1
#用来恢复和清理 data 下数据的线程数量
num.recovery.threads.per.data.dir=1
# 每个 topic 创建时的副本数，默认时 1 个副本
offsets.topic.replication.factor=1
#segment 文件保留的最长时间，超时将被删除
log.retention.hours=168
#每个 segment 文件的大小，默认最大 1G
log.segment.bytes=1073741824
# 检查过期数据的时间，默认 5 分钟检查一次是否数据过期
log.retention.check.interval.ms=300000
#配置连接 Zookeeper 集群地址（在 zk 根目录下创建/kafka，方便管理）
zookeeper.connect=hadoop01:2181,hadoop02:2181,hadoop03:2181/kafka
```

**配置环境变量**

```shell
vim /etc/profile.d/my_env.sh

#KAFKA_HOME
export KAFKA_HOME=/opt/module/kafka
export PATH=$KAFKA_HOME/bin:$PATH
```

**分发文件Kafka和环境变量给其他机器**

```shell
xsync /opt/module/kafka
xsync /etc/profile.d/my_env.sh
```

**更改hadoop03和hadoop02中kafka的broker.id编号**

```shell
ssh hadoop02
vim /opt/module/kafka/config/server.properties
#broker 的全局唯一编号，不能重复，只能是数字。
broker.id=1
exit

ssh hadoop03
vim /opt/module/kafka/config/server.properties
#broker 的全局唯一编号，不能重复，只能是数字。
broker.id=2
exit
```

#### **2.7.2 编写Kafka集群启动停止脚本**

```shell
vim /root/bin/kf.sh

#! /bin/bash
case $1 in
"start"){
 for i in hadoop01 hadoop02 hadoop03
 do
 echo " --------启动 $i Kafka-------"
 ssh $i "/opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties"
 done
};;
"stop"){
 for i in hadoop01 hadoop02 hadoop03
 do
 echo " --------停止 $i Kafka-------"
 ssh $i "/opt/module/kafka/bin/kafka-server-stop.sh "
 done
};;
esac

chmod +x /root/bin/kf.sh
```

**验证Kafka和启动脚本是否成功**

```shell
zk.sh start						--Kafka依赖ZooKeepker，必须优先启动Zk
kf.sh start

kf.sh stop						--kafka关闭，可选择是否关闭zk
zk.sh stop

jpsall							-- 查看Kafka是否启动成功
```

![](/img/image-20230310134324320.png)

### 2.8 安装flume

**用XShell工具将flume安装包导入到hadoop01的/opt/software文件夹下面，解压**

```shell
tar -zxvf /opt/software/apache-flume-1.9.0-bin.tar.gz -C /opt/module/ 		--解压flume到指定目录
mv /opt/module/apache-flume-1.9.0-bin /opt/module/flume						--更改文件夹名字
```

**将lib文件夹下的guava-11.0.2.jar删除以兼容Hadoop 3.1.3**

```shell
rm /opt/module/flume/lib/guava-11.0.2.jar
```

**将flume/conf下的flume-env.sh.template文件修改为flume-env.sh**

```shell
mv /opt/module/conf/flume-env.sh.template  /opt/module/conf/flume-env.sh
vim /opt/module/conf/flume-env.sh

export JAVA_HOME=/opt/module/jdk1.8.0_212
```

**Flume安装示意图**

![未命名文件](/img/未命名文件.png)

#### 2.8.1 配置flume采集日志配置

|                 | 服务器hadoop01 | 服务器hadoop02 | 服务器hadoop03 |
| :-------------: | :------------: | :------------: | :------------: |
| Flume(采集日志) |     Flume      |     Flume      |                |

**编写flume日志采集系统配置文件**

```shell
vim /opt/module/flume-1.9.0/conf/file-flume-kafka.conf

a1.sources = r1
a1.channels = c1

#配置source(tailDir source)
a1.sources.r1.type = TAILDIR
a1.sources.r1.filegroups = f1
a1.sources.r1.filegroups.f1 = /opt/module/applog/log/app.*
a1.sources.r1.positionFile = /opt/module/flume-1.9.0/taildir_position.json

#配置拦截器(ETL数据清洗,json数据完整)
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = com.zhanglei.flume.interceptor.ELTintercetor$Builder

#配置channel
a1.channels.c1.type = org.apache.flume.channel.kafka.KafkaChannel
a1.channels.c1.kafka.bootstrap.servers = hadoop01:9092,hadoop02:9092
a1.channels.c1.kafka.topic = topic_log
a1.channels.c1.parseAsFlumeEvent = false

#拼接组件
a1.sources.r1.channels = c1
```

**编写自定义拦截器**

**创建Maven工程flume-interceptor**

**创建包名：com.zhanglei.flume.interceptor**

**在pom.xml文件中添加如下配置**

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.flume</groupId>
        <artifactId>flume-ng-core</artifactId>
        <version>1.9.0</version>
        <scope>provided</scope>
    </dependency>

    <dependency>
        <groupId>com.alibaba</groupId>
        <artifactId>fastjson</artifactId>
        <version>1.2.62</version>
    </dependency>
</dependencies>

<build>
    <plugins>
        <plugin>
            <artifactId>maven-compiler-plugin</artifactId>
            <version>2.3.2</version>
            <configuration>
                <source>1.8</source>
                <target>1.8</target>
            </configuration>
        </plugin>
        <plugin>
            <artifactId>maven-assembly-plugin</artifactId>
            <configuration>
                <descriptorRefs>
                    <descriptorRef>jar-with-dependencies</descriptorRef>
                </descriptorRefs>
            </configuration>
            <executions>
                <execution>
                    <id>make-assembly</id>
                    <phase>package</phase>
                    <goals>
                        <goal>single</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

**在com.zhanglei.flume.interceptor包下创建ELTintercetor类**

```java
package com.zhanglei.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class ELTintercetor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        // TODO 取出数据，进行校验

        // 1.取出数据
        byte[] body = event.getBody();

        String log = new String(body, StandardCharsets.UTF_8);

        // 2.校验
        if (JSONUtils.isValiddate(log)){
            return event;
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        list.removeIf(next -> intercept(next) == null);

        return list;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ELTintercetor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
```

**在com.zhanglei.flume.interceptor包下创建JSONUtils类**

```java
package com.zhanglei.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.google.gson.JsonElement;

public class JSONUtils {
//    // 验证方法是否正确
//    public static void main(String[] args) {
//        System.out.println(isValiddate("{/"age/":/"18/"}"));
//    }

    // 验证数据是否 JSON
    public static boolean isValiddate(String log) {
        try {
            JSON.parse(log);
            return true;
        }catch (JSONException e){
            return false;
        }

    }
}
```

**打包**

![image-20230311195257362](/img/image-20230311195257362.png)

**将打好的包放入到hadoop01的/opt/module/flume/lib文件夹下面**

```shell
cd /opt/module/flume/lib
ls | grep interceptor
untitled-interceptor-1.0-SNAPSHOT-jar-with-dependencies.jar
```

**分发flume到其他集群上**

```shell
xsync /opt/module/flume
```

#### 2.8.2 flume日志采集启动脚本

```shell
vim /root/bin/f1.sh

#! /bin/bash

case $1 in
"start"){
        for i in hadoop01 hadoop02
        do
                echo " --------启动 $i 采集flume-------"
                ssh $i "nohup /opt/module/flume-1.9.0/bin/flume-ng agent --conf-file /opt/module/flume-1.9.0/conf/file-flume-kafka.conf --name a1 -Dflume.root.logger=INFO,LOGFILE  >/opt/module/flume-1.9.0/log1.txt 2>&1  &"
        done
};;	
"stop"){
        for i in hadoop01 hadoop02
        do
                echo " --------停止 $i 采集flume-------"
                ssh $i "ps -ef | grep file-flume-kafka | grep -v grep |awk  '{print /$2}' | xargs -n1 kill -9 "
        done

};;
esac

chmod 777 /root/bin/f1.sh
```

**验证flume配置和脚本是否成功**

```
f1.sh start
jpsall
f1.sh stop
```

![image-20230311203619354](/img/image-20230311203619354.png)

#### 2.8.3 配置flume采集日志配置

**编写flume消费者采集系统配置文件（采集kafka中的topic读取数据）**

```shell
vim /opt/module/flume-1.9.0/conf/kafka-flume-hdfs.conf

#定义组件
a1.sources = r1
a1.channels = c1
a1.sinks = k1

#配置source
a1.sources.r1.type = org.apache.flume.source.kafka.KafkaSource
a1.sources.r1.kafka.bootstrap.servers = hadoop01:9092,hadoop02:9092,hadoop03:9092
a1.sources.r1.kafka.topics = topic_log
a1.sources.r1.batchSize = 5000
a1.sources.r1.batchDurationMillis = 2000

#配置拦截器 (时间戳拦截器)
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = com.zhanglei.flume.interceptor.TimeStampInterceptor$Builder

#配置channel
a1.channels.c1.type = file
a1.channels.c1.dataDirs = /opt/module/flume-1.9.0/data/behavior1/
a1.channels.c1.checkpointDir = /opt/module/flume-1.9.0/checkpoint/behavior1

#配置sink
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /origin_data/gmall/log/topic_log/%Y-%m-%d
a1.sinks.k1.hdfs.filePrefix = log-
a1.sinks.k1.hdfs.round = false

## 控制文件大小
a1.sinks.k1.hdfs.rollInterval = 10
a1.sinks.k1.hdfs.rollSize = 134217728
a1.sinks.k1.hdfs.rollCount = 0

## 控制输入文件是原生文件
a1.sinks.k1.hdfs.fileType = CompressedStream 
a1.sinks.k1.hdfs.codeC = gzip

#连接组件
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```

**在com.zhanglei.flume.interceptor包下创建TimeStampInterceptor类**

```java
package com.zhanglei.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class TimeStampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        // TODO 拦截日志，取出header里面的key，取出booy中的日志时间 ： 将 ts 的值 赋值给 header 的 key  timestamp

        // 1. 获取头部信息
        Map<String, String> headers = event.getHeaders();

        // 2.获取 body 中 ts
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);

        JSONObject jsonObject = JSONObject.parseObject(log);

        String ts = jsonObject.getString("ts");

        // 3. 将ts赋值给timestamp
        headers.put("timestamp",ts);

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        for (Event event : list) {
            intercept(event);
        }

        return list;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
```

**打包**

![](/img/image-20230311195257362.png)

**将打好的包放入到hadoop03的/opt/module/flume/lib文件夹下面**

```
cd /opt/module/flume/lib
ls | grep interceptor
untitled-interceptor-1.0-SNAPSHOT-jar-with-dependencies.jar
```

**分发flume到其他集群上**

```
xsync /opt/module/flume
```

#### 2.8.2 flume消费者采集启动脚本

```shell
vim /root/bin/f2.sh

#! /bin/bash

case $1 in
"start"){
        for i in hadoop03
        do
                echo " --------启动 $i 消费flume-------"
                ssh $i "nohup /opt/module/flume-1.9.0/bin/flume-ng agent --conf-file /opt/module/flume-1.9.0/conf/kafka-flume-hdfs.conf --name a1 -Dflume.root.logger=INFO,LOGFILE >/opt/module/flume-1.9.0/log2.txt   2>&1 &"
        done
};;
"stop"){
        for i in hadoop03
        do
                echo " --------停止 $i 消费flume-------"
                ssh $i "ps -ef | grep kafka-flume-hdfs | grep -v grep |awk '{print /$2}' | xargs -n1 kill"
        done

};;
esac

chmod 777 /root/bin/f2.sh
```

**验证flume配置和脚本是否成功**

```shell
f2.sh start
jps
f2.sh stop
```

![image-20230311204932696](/img/image-20230311204932696.png)



### 2.9 编写一键集群启动脚本

```shell
vim /root/bin/cluster.sh

#！ /bin/bash

case $1 in
"start")
                echo "-----------启动 集群-------------"
                zk.sh start

                myhadoop.sh start

                kf.sh start

                f1.sh start

                f2.sh start

;;
"stop")
                echo "-----------停止 集群-------------"
                f2.sh stop

                f1.sh stop

                kf.sh stop

                myhadoop.sh stop

                zk.sh stop
;;
esac

chmod 777 /root/bin/cluster.sh
```

**测试脚本是否成功**

```shell
cluster.sh start
jpsall
cluster.sh stop
```

![image-20230311210345653](/img/image-20230311210345653.png)

![image-20230311210408914](/img/image-20230311210408914.png)

![image-20230311210453630](/img/image-20230311210453630.png)



### 2.10 搭建日志生成系统

**将文件上传到hadoop01的/opt/module/applog目录下**

![image-20230316133352068](/img/image-20230316133352068.png)

```shell
mkdir /opt/module/applog/
```

#### 2.10.1 修改配置文件

**application.yml可以根据需求生成对应日期的用户行为日志**

```yaml
vim /opt/module/applog/application.yml

# 外部配置打开
logging.config: "./logback.xml"
#业务日期
mock.date: "2020-04-14"

#模拟数据发送模式
#mock.type: "http"
#mock.type: "kafka"
mock.type: "log"

#http模式下，发送的地址
mock.url: "http://hdp1/applog"

#kafka模式下，发送的地址
mock:
  kafka-server: "hdp1:9092,hdp2:9092,hdp3:9092"
  kafka-topic: "ODS_BASE_LOG"

#启动次数
mock.startup.count: 200
#设备最大值
mock.max.mid: 500000
#会员最大值
mock.max.uid: 100
#商品最大值
mock.max.sku-id: 35
#页面平均访问时间
mock.page.during-time-ms: 20000
#错误概率 百分比
mock.error.rate: 3
#每条日志发送延迟 ms
mock.log.sleep: 10
#商品详情来源  用户查询，商品推广，智能推荐, 促销活动
mock.detail.source-type-rate: "40:25:15:20"
#领取购物券概率
mock.if_get_coupon_rate: 75
#购物券最大id
mock.max.coupon-id: 3
#搜索关键词  
mock.search.keyword: "图书,小米,iphone11,电视,口红,ps5,苹果手机,小米盒子"
```

**path.json，该文件用来配置访问路径**

```json
[
  {"path":["home","good_list","good_detail","cart","trade","payment"],"rate":20 },
  {"path":["home","search","good_list","good_detail","login","good_detail","cart","trade","payment"],"rate":40 },
  {"path":["home","mine","orders_unpaid","trade","payment"],"rate":10 },
  {"path":["home","mine","orders_unpaid","good_detail","good_spec","comment","trade","payment"],"rate":5 },
  {"path":["home","mine","orders_unpaid","good_detail","good_spec","comment","home"],"rate":5 },
  {"path":["home","good_detail"],"rate":10 },
  {"path":["home"  ],"rate":10 }
]
```

**logback配置文件可配置日志生成路径**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property name="LOG_HOME" value="/opt/module/applog/log" />
    <appender name="console" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%msg%n</pattern>
        </encoder>
    </appender>

    <appender name="rollingFile" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>${LOG_HOME}/app.%d{yyyy-MM-dd}.log</fileNamePattern>
        </rollingPolicy>
        <encoder>
            <pattern>%msg%n</pattern>
        </encoder>
    </appender>

    <!-- 将某一个包下日志单独打印日志 -->
    <logger name="com.atgugu.gmall2020.mock.log.util.LogUtil"
            level="INFO" additivity="false">
        <appender-ref ref="rollingFile" />
        <appender-ref ref="console" />
    </logger>

    <root level="error"  >
        <appender-ref ref="console" />
    </root>
</configuration>
```

#### 2.10.2 集群日志生成脚本

```shell
vim /root/bin/lg.shg

#! /bin/bash

for i in hadoop01 hadoop02
do 
	echo "-----------------生成$i-的数据--------------------"
	ssh $i "cd /opt/module/applog; java -jar gmall2020-mock-log-2021-01-22.jar >/dev/null 2>&1 &"
done

chmod +x /root/bin/lg.shg
```



## 第三章、业务数据采集集群搭建

### 3.0 工作流程图

![image-20230314144931927](/img/image-20230314144931927.png)

### 3.1 安装mysql

**将安装包和JDBC驱动上传到/opt/software，共计6个**

![image-20230314145136508](/img/image-20230314145136508.png)

**将虚拟机自带的Mysql-libs卸载**

```shell
rpm -qa | grep -i -E mysql/|mariadb | xargs -n1 rpm -e --nodeps
```

**安装Mysql依赖，根据依赖包的序号依次按照**

```shell
rpm -ivh 01_mysql-community-common-5.7.16-1.el7.x86_64.rpm
rpm -ivh 02_mysql-community-libs-5.7.16-1.el7.x86_64.rpm
rpm -ivh 03_mysql-community-libs-compat-5.7.16-1.el7.x86_64.rpm
rpm -ivh 04_mysql-community-client-5.7.16-1.el7.x86_64.rpm
rpm -ivh 05_mysql-community-server-5.7.16-1.el7.x86_64.rpm
```

**如若出现版本过久，提示安装失败时**

```shell
rpm -ivh 05_mysql-community-server-5.7.16-1.el7.x86_64.rpm --force --nodeps
```

**启动Mysql**

```shell
systemctl start mysqld
```

**查看Mysql初始化时的随机密码**

```shell
cat /var/log/mysqld.log | grep password
```

![image-20230314150155089](/img/image-20230314150155089.png)

### 3.2 配置Mysql

**用刚刚查到的密码进入MySQL（如果报错，给密码加单引号）**

```shell
mysql -uroot -p'mypassword'
```

**设置复杂密码（由于MySQL密码策略，此密码必须足够复杂）**

```sql
set password=password("Qs23=zs32");
```

**更改MySQL密码策略**

```sql
set global validate_password_length=4;      --密码长度为4
set global validate_password_policy=0;		--密码复杂度为0
```

**设置简单好记的密码**

```sql
set password=password("root");
```

**进入MySQL库**

```sql
use mysql
```

**查询user表**

```sql
update user set host="%" where user="root";
```

**刷新**

```sql
flush privileges;
```

**退出**

```sql
quit;
```

**连接数据库，执行sql脚本**

![image-20230314151935479](/img/image-20230314151935479.png)

![image-20230314152322477](/img/image-20230314152322477.png)

**导入数据库结构脚本（gmall.sql）**

![image-20230314152607206](img/image-20230314152607206.png)

![image-20230314152656005](/img/image-20230314152656005.png)

### 3.3 生成业务数据

**在hadoop01的/opt/module/目录下创建db_log文件夹**

```shell
mkdir /opt/module/db_log
```

**把gmall2020-mock-db-2021-01-22.jar和application.properties上传到hadoop01的/opt/module/db_log路径上**

![image-20230314153828366](/img/image-20230314153828366.png)

**根据需求修改application.properties相关配置**

```properties
vim /opt/module/db_log/application.properties

logging.level.root=info

spring.datasource.driver-class-name=com.mysql.jdbc.Driver
spring.datasource.url=jdbc:mysql://hadoop01:3306/gmall?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8
spring.datasource.username=root
spring.datasource.password=root

logging.pattern.console=%m%n

mybatis-plus.global-config.db-config.field-strategy=not_null

#业务日期
mock.date=2020-06-14
#是否重置     注意：第一次执行必须设置为1，后续不需要重置不用设置为1
mock.clear=0
#是否重置用户 注意：第一次执行必须设置为1，后续不需要重置不用设置为1
mock.clear.user=0

#生成新用户数量
mock.user.count=100
#男性比例
mock.user.male-rate=20
#用户数据变化概率
mock.user.update-rate:20

#收藏取消比例
mock.favor.cancel-rate=10
#收藏数量
mock.favor.count=100

#每个用户添加购物车的概率
mock.cart.user-rate=50
#每次每个用户最多添加多少种商品进购物车
mock.cart.max-sku-count=8 
#每个商品最多买几个
mock.cart.max-sku-num=3 

#购物车来源  用户查询，商品推广，智能推荐, 促销活动
mock.cart.source-type-rate=60:20:10:10

#用户下单比例
mock.order.user-rate=50
#用户从购物中购买商品比例
mock.order.sku-rate=50
#是否参加活动
mock.order.join-activity=1
#是否使用购物券
mock.order.use-coupon=1
#购物券领取人数
mock.coupon.user-count=100

#支付比例
mock.payment.rate=70
#支付方式 支付宝：微信 ：银联
mock.payment.payment-type=30:60:10


#评价比例 好：中：差：自动
mock.comment.appraise-rate=30:10:10:50

#退款原因比例：质量问题 商品描述与实际描述不一致 缺货 号码不合适 拍错 不想买了 其他
mock.refund.reason-rate=30:10:20:5:15:5:5
```

**生成业务数据**

```bash
java -jar /opt/module/db_log/gmall2020-mock-db-2021-01-22.jar
```

### **3.4 Sqoop安装**

**上传sqoop的安装包到hadoop01的/opt/software**

![image-20230316180641503](/\img\image-20230316180641503.png)

```bash
tar -zxf /opt/software/sqoop-1.4.6.bin__hadoop-2.0.4-alpha.tar.gz -C /opt/module/
```

#### 3.4.1 修改配置文件

**重命名配置文件**

```shell
mv /opt/module/sqoop/conf/sqoop-env-template.sh /opt/module/sqoop/conf/sqoop-env.sh 
```

**修改配置文件**

```shell
vim /opt/module/sqoop/conf/sqoop-env.sh 

export HADOOP_COMMON_HOME=/opt/module/hadoop-3.1.3
export HADOOP_MAPRED_HOME=/opt/module/hadoop-3.1.3
export HIVE_HOME=/opt/module/hive
export ZOOKEEPER_HOME=/opt/module/zookeeper-3.5.7
export ZOOCFGDIR=/opt/module/zookeeper-3.5.7/conf
```

**拷贝JDBC驱动**

```shell
cp /opt/software/mysql-connector-java-5.1.27-bin.jar /opt/module/sqoop/lib/
```

**测试Sqoop是否能够成功连接数据库**

```bash
/opt/module/sqoop/bin/sqoop list-databases --connect jdbc:mysql://hadoop01:3306/ --username root --password root
```



### 3.5 同步策略

![image-20230317100403155](/img/image-20230317100403155.png)

#### 3.5.1首日同步脚本（业务数据导入HDFS）

```bash
vim /root/bin/mysql_to_hdfs_init.sh
```

```shell
#! /bin/bash

APP=gmall
sqoop=/opt/module/sqoop/bin/sqoop

if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi 

import_data(){
$sqoop import /
--connect jdbc:mysql://hadoop01:3306/$APP /
--username root /
--password root /
--target-dir /origin_data/$APP/db/$1/$do_date /
--delete-target-dir /
--query "$2 where /$CONDITIONS" /
--num-mappers 1 /
--fields-terminated-by '/t' /
--compress /
--compression-codec gzip /
--null-string '//N' /
--null-non-string '//N'
}

import_order_info(){
  import_data order_info "select
                            id, 
                            total_amount, 
                            order_status, 
                            user_id, 
                            payment_way,
                            delivery_address,
                            out_trade_no, 
                            create_time, 
                            operate_time,
                            expire_time,
                            tracking_no,
                            province_id,
                            activity_reduce_amount,
                            coupon_reduce_amount,                            
                            original_total_amount,
                            feight_fee,
                            feight_fee_reduce      
                        from order_info"
}


import_coupon_use(){
  import_data coupon_use "select
                          id,
                          coupon_id,
                          user_id,
                          order_id,
                          coupon_status,
                          get_time,
                          using_time,
                          used_time,
                          expire_time
                        from coupon_use"
}

import_order_status_log(){
  import_data order_status_log "select
                                  id,
                                  order_id,
                                  order_status,
                                  operate_time
                                from order_status_log"
}

import_user_info(){
  import_data "user_info" "select 
                            id,
                            login_name,
                            nick_name,
                            name,
                            phone_num,
                            email,
                            user_level, 
                            birthday,
                            gender,
                            create_time,
                            operate_time
                          from user_info"
}

import_order_detail(){
  import_data order_detail "select 
                              id,
                              order_id, 
                              sku_id,
                              sku_name,
                              order_price,
                              sku_num, 
                              create_time,
                              source_type,
                              source_id,
                              split_total_amount,
                              split_activity_amount,
                              split_coupon_amount
                            from order_detail"
}

import_payment_info(){
  import_data "payment_info"  "select 
                                id,  
                                out_trade_no, 
                                order_id, 
                                user_id, 
                                payment_type, 
                                trade_no, 
                                total_amount,  
                                subject, 
                                payment_status,
                                create_time,
                                callback_time 
                              from payment_info"
}

import_comment_info(){
  import_data comment_info "select
                              id,
                              user_id,
                              sku_id,
                              spu_id,
                              order_id,
                              appraise,
                              create_time
                            from comment_info"
}

import_order_refund_info(){
  import_data order_refund_info "select
                                id,
                                user_id,
                                order_id,
                                sku_id,
                                refund_type,
                                refund_num,
                                refund_amount,
                                refund_reason_type,
                                refund_status,
                                create_time
                              from order_refund_info"
}

import_sku_info(){
  import_data sku_info "select 
                          id,
                          spu_id,
                          price,
                          sku_name,
                          sku_desc,
                          weight,
                          tm_id,
                          category3_id,
                          is_sale,
                          create_time
                        from sku_info"
}

import_base_category1(){
  import_data "base_category1" "select 
                                  id,
                                  name 
                                from base_category1"
}

import_base_category2(){
  import_data "base_category2" "select
                                  id,
                                  name,
                                  category1_id 
                                from base_category2"
}

import_base_category3(){
  import_data "base_category3" "select
                                  id,
                                  name,
                                  category2_id
                                from base_category3"
}

import_base_province(){
  import_data base_province "select
                              id,
                              name,
                              region_id,
                              area_code,
                              iso_code,
                              iso_3166_2
                            from base_province"
}

import_base_region(){
  import_data base_region "select
                              id,
                              region_name
                            from base_region"
}

import_base_trademark(){
  import_data base_trademark "select
                                id,
                                tm_name
                              from base_trademark"
}

import_spu_info(){
  import_data spu_info "select
                            id,
                            spu_name,
                            category3_id,
                            tm_id
                          from spu_info"
}

import_favor_info(){
  import_data favor_info "select
                          id,
                          user_id,
                          sku_id,
                          spu_id,
                          is_cancel,
                          create_time,
                          cancel_time
                        from favor_info"
}

import_cart_info(){
  import_data cart_info "select
                        id,
                        user_id,
                        sku_id,
                        cart_price,
                        sku_num,
                        sku_name,
                        create_time,
                        operate_time,
                        is_ordered,
                        order_time,
                        source_type,
                        source_id
                      from cart_info"
}

import_coupon_info(){
  import_data coupon_info "select
                          id,
                          coupon_name,
                          coupon_type,
                          condition_amount,
                          condition_num,
                          activity_id,
                          benefit_amount,
                          benefit_discount,
                          create_time,
                          range_type,
                          limit_num,
                          taken_count,
                          start_time,
                          end_time,
                          operate_time,
                          expire_time
                        from coupon_info"
}

import_activity_info(){
  import_data activity_info "select
                              id,
                              activity_name,
                              activity_type,
                              start_time,
                              end_time,
                              create_time
                            from activity_info"
}

import_activity_rule(){
    import_data activity_rule "select
                                    id,
                                    activity_id,
                                    activity_type,
                                    condition_amount,
                                    condition_num,
                                    benefit_amount,
                                    benefit_discount,
                                    benefit_level
                                from activity_rule"
}

import_base_dic(){
    import_data base_dic "select
                            dic_code,
                            dic_name,
                            parent_code,
                            create_time,
                            operate_time
                          from base_dic"
}


import_order_detail_activity(){
    import_data order_detail_activity "select
                                                                id,
                                                                order_id,
                                                                order_detail_id,
                                                                activity_id,
                                                                activity_rule_id,
                                                                sku_id,
                                                                create_time
                                                            from order_detail_activity"
}


import_order_detail_coupon(){
    import_data order_detail_coupon "select
                                                                id,
                                                                        order_id,
                                                                order_detail_id,
                                                                coupon_id,
                                                                coupon_use_id,
                                                                sku_id,
                                                                create_time
                                                            from order_detail_coupon"
}


import_refund_payment(){
    import_data refund_payment "select
                                                        id,
                                                        out_trade_no,
                                                        order_id,
                                                        sku_id,
                                                        payment_type,
                                                        trade_no,
                                                        total_amount,
                                                        subject,
                                                        refund_status,
                                                        create_time,
                                                        callback_time
                                                    from refund_payment"                                                    

}

import_sku_attr_value(){
    import_data sku_attr_value "select
                                                    id,
                                                    attr_id,
                                                    value_id,
                                                    sku_id,
                                                    attr_name,
                                                    value_name
                                                from sku_attr_value"
}


import_sku_sale_attr_value(){
    import_data sku_sale_attr_value "select
                                                            id,
                                                            sku_id,
                                                            spu_id,
                                                            sale_attr_value_id,
                                                            sale_attr_id,
                                                            sale_attr_name,
                                                            sale_attr_value_name
                                                        from sku_sale_attr_value"
}

case $1 in
  "order_info")
     import_order_info
;;
  "base_category1")
     import_base_category1
;;
  "base_category2")
     import_base_category2
;;
  "base_category3")
     import_base_category3
;;
  "order_detail")
     import_order_detail
;;
  "sku_info")
     import_sku_info
;;
  "user_info")
     import_user_info
;;
  "payment_info")
     import_payment_info
;;
  "base_province")
     import_base_province
;;
  "base_region")
     import_base_region
;;
  "base_trademark")
     import_base_trademark
;;
  "activity_info")
      import_activity_info
;;
  "cart_info")
      import_cart_info
;;
  "comment_info")
      import_comment_info
;;
  "coupon_info")
      import_coupon_info
;;
  "coupon_use")
      import_coupon_use
;;
  "favor_info")
      import_favor_info
;;
  "order_refund_info")
      import_order_refund_info
;;
  "order_status_log")
      import_order_status_log
;;
  "spu_info")
      import_spu_info
;;
  "activity_rule")
      import_activity_rule
;;
  "base_dic")
      import_base_dic
;;
  "order_detail_activity")
      import_order_detail_activity
;;
  "order_detail_coupon")
      import_order_detail_coupon
;;
  "refund_payment")
      import_refund_payment
;;
  "sku_attr_value")
      import_sku_attr_value
;;
  "sku_sale_attr_value")
      import_sku_sale_attr_value
;;
  "all")
   import_base_category1
   import_base_category2
   import_base_category3
   import_order_info
   import_order_detail
   import_sku_info
   import_user_info
   import_payment_info
   import_base_region
   import_base_province
   import_base_trademark
   import_activity_info
   import_cart_info
   import_comment_info
   import_coupon_use
   import_coupon_info
   import_favor_info
   import_order_refund_info
   import_order_status_log
   import_spu_info
   import_activity_rule
   import_base_dic
   import_order_detail_activity
   import_order_detail_coupon
   import_refund_payment
   import_sku_attr_value
   import_sku_sale_attr_value
;;
esac
```

#### **3.5.2 每日同步脚本（业务数据导入HDFS）**

```bass
vim /root/bin/mysql_to_hdf.sh
```

```shell
#! /bin/bash

APP=gmall
sqoop=/opt/module/sqoop/bin/sqoop

if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d '-1 day' +%F`
fi

import_data(){
$sqoop import /
--connect jdbc:mysql://hadoop01:3306/$APP /
--username root /
--password root /
--target-dir /origin_data/$APP/db/$1/$do_date /
--delete-target-dir /
--query "$2 and  /$CONDITIONS" /
--num-mappers 1 /
--fields-terminated-by '/t' /
--compress /
--compression-codec gzip /
--null-string '//N' /
--null-non-string '//N'
}


import_order_info(){
  import_data order_info "select
                            id, 
                            total_amount, 
                            order_status, 
                            user_id, 
                            payment_way,
                            delivery_address,
                            out_trade_no, 
                            create_time, 
                            operate_time,
                            expire_time,
                            tracking_no,
                            province_id,
                            activity_reduce_amount,
                            coupon_reduce_amount,                            
                            original_total_amount,
                            feight_fee,
                            feight_fee_reduce      
                        from order_info
                        where (date_format(create_time,'%Y-%m-%d')='$do_date' 
                        or date_format(operate_time,'%Y-%m-%d')='$do_date')"
}

import_coupon_use(){
  import_data coupon_use "select
                          id,
                          coupon_id,
                          user_id,
                          order_id,
                          coupon_status,
                          get_time,
                          using_time,
                          used_time,
                          expire_time
                        from coupon_use
                        where (date_format(get_time,'%Y-%m-%d')='$do_date'
                        or date_format(using_time,'%Y-%m-%d')='$do_date'
                        or date_format(used_time,'%Y-%m-%d')='$do_date'
                        or date_format(expire_time,'%Y-%m-%d')='$do_date')"
}

import_order_status_log(){
  import_data order_status_log "select
                                  id,
                                  order_id,
                                  order_status,
                                  operate_time
                                from order_status_log
                                where date_format(operate_time,'%Y-%m-%d')='$do_date'"
}

import_user_info(){
  import_data "user_info" "select 
                            id,
                            login_name,
                            nick_name,
                            name,
                            phone_num,
                            email,
                            user_level, 
                            birthday,
                            gender,
                            create_time,
                            operate_time
                          from user_info 
                          where (DATE_FORMAT(create_time,'%Y-%m-%d')='$do_date' 
                          or DATE_FORMAT(operate_time,'%Y-%m-%d')='$do_date')"
}

import_order_detail(){
  import_data order_detail "select 
                              id,
                              order_id, 
                              sku_id,
                              sku_name,
                              order_price,
                              sku_num, 
                              create_time,
                              source_type,
                              source_id,
                              split_total_amount,
                              split_activity_amount,
                              split_coupon_amount
                            from order_detail 
                            where DATE_FORMAT(create_time,'%Y-%m-%d')='$do_date'"
}

import_payment_info(){
  import_data "payment_info"  "select 
                                id,  
                                out_trade_no, 
                                order_id, 
                                user_id, 
                                payment_type, 
                                trade_no, 
                                total_amount,  
                                subject, 
                                payment_status,
                                create_time,
                                callback_time 
                              from payment_info 
                              where (DATE_FORMAT(create_time,'%Y-%m-%d')='$do_date' 
                              or DATE_FORMAT(callback_time,'%Y-%m-%d')='$do_date')"
}

import_comment_info(){
  import_data comment_info "select
                              id,
                              user_id,
                              sku_id,
                              spu_id,
                              order_id,
                              appraise,
                              create_time
                            from comment_info
                            where date_format(create_time,'%Y-%m-%d')='$do_date'"
}

import_order_refund_info(){
  import_data order_refund_info "select
                                id,
                                user_id,
                                order_id,
                                sku_id,
                                refund_type,
                                refund_num,
                                refund_amount,
                                refund_reason_type,
                                refund_status,
                                create_time
                              from order_refund_info
                              where date_format(create_time,'%Y-%m-%d')='$do_date'"
}

import_sku_info(){
  import_data sku_info "select 
                          id,
                          spu_id,
                          price,
                          sku_name,
                          sku_desc,
                          weight,
                          tm_id,
                          category3_id,
                          is_sale,
                          create_time
                        from sku_info where 1=1"
}

import_base_category1(){
  import_data "base_category1" "select 
                                  id,
                                  name 
                                from base_category1 where 1=1"
}

import_base_category2(){
  import_data "base_category2" "select
                                  id,
                                  name,
                                  category1_id 
                                from base_category2 where 1=1"
}

import_base_category3(){
  import_data "base_category3" "select
                                  id,
                                  name,
                                  category2_id
                                from base_category3 where 1=1"
}

import_base_province(){
  import_data base_province "select
                              id,
                              name,
                              region_id,
                              area_code,
                              iso_code,
                              iso_3166_2
                            from base_province
                            where 1=1"
}

import_base_region(){
  import_data base_region "select
                              id,
                              region_name
                            from base_region
                            where 1=1"
}

import_base_trademark(){
  import_data base_trademark "select
                                id,
                                tm_name
                              from base_trademark
                              where 1=1"
}

import_spu_info(){
  import_data spu_info "select
                            id,
                            spu_name,
                            category3_id,
                            tm_id
                          from spu_info
                          where 1=1"
}

import_favor_info(){
  import_data favor_info "select
                          id,
                          user_id,
                          sku_id,
                          spu_id,
                          is_cancel,
                          create_time,
                          cancel_time
                        from favor_info
                        where 1=1"
}

import_cart_info(){
  import_data cart_info "select
                        id,
                        user_id,
                        sku_id,
                        cart_price,
                        sku_num,
                        sku_name,
                        create_time,
                        operate_time,
                        is_ordered,
                        order_time,
                        source_type,
                        source_id
                      from cart_info
                      where 1=1"
}

import_coupon_info(){
  import_data coupon_info "select
                          id,
                          coupon_name,
                          coupon_type,
                          condition_amount,
                          condition_num,
                          activity_id,
                          benefit_amount,
                          benefit_discount,
                          create_time,
                          range_type,
                          limit_num,
                          taken_count,
                          start_time,
                          end_time,
                          operate_time,
                          expire_time
                        from coupon_info
                        where 1=1"
}

import_activity_info(){
  import_data activity_info "select
                              id,
                              activity_name,
                              activity_type,
                              start_time,
                              end_time,
                              create_time
                            from activity_info
                            where 1=1"
}

import_activity_rule(){
    import_data activity_rule "select
                                    id,
                                    activity_id,
                                    activity_type,
                                    condition_amount,
                                    condition_num,
                                    benefit_amount,
                                    benefit_discount,
                                    benefit_level
                                from activity_rule
                                where 1=1"
}

import_base_dic(){
    import_data base_dic "select
                            dic_code,
                            dic_name,
                            parent_code,
                            create_time,
                            operate_time
                          from base_dic
                          where 1=1"
}


import_order_detail_activity(){
    import_data order_detail_activity "select
                                                                id,
                                                                order_id,
                                                                order_detail_id,
                                                                activity_id,
                                                                activity_rule_id,
                                                                sku_id,
                                                                create_time
                                                            from order_detail_activity
                                                            where date_format(create_time,'%Y-%m-%d')='$do_date'"
}


import_order_detail_coupon(){
    import_data order_detail_coupon "select
                                                                id,
								                                                order_id,
                                                                order_detail_id,
                                                                coupon_id,
                                                                coupon_use_id,
                                                                sku_id,
                                                                create_time
                                                            from order_detail_coupon
                                                            where date_format(create_time,'%Y-%m-%d')='$do_date'"
}


import_refund_payment(){
    import_data refund_payment "select
                                                        id,
                                                        out_trade_no,
                                                        order_id,
                                                        sku_id,
                                                        payment_type,
                                                        trade_no,
                                                        total_amount,
                                                        subject,
                                                        refund_status,
                                                        create_time,
                                                        callback_time
                                                    from refund_payment
                                                    where (DATE_FORMAT(create_time,'%Y-%m-%d')='$do_date' 
                                                    or DATE_FORMAT(callback_time,'%Y-%m-%d')='$do_date')"                                                    

}

import_sku_attr_value(){
    import_data sku_attr_value "select
                                                    id,
                                                    attr_id,
                                                    value_id,
                                                    sku_id,
                                                    attr_name,
                                                    value_name
                                                from sku_attr_value
                                                where 1=1"
}


import_sku_sale_attr_value(){
    import_data sku_sale_attr_value "select
                                                            id,
                                                            sku_id,
                                                            spu_id,
                                                            sale_attr_value_id,
                                                            sale_attr_id,
                                                            sale_attr_name,
                                                            sale_attr_value_name
                                                        from sku_sale_attr_value
                                                        where 1=1"
}

case $1 in
  "order_info")
     import_order_info
;;
  "base_category1")
     import_base_category1
;;
  "base_category2")
     import_base_category2
;;
  "base_category3")
     import_base_category3
;;
  "order_detail")
     import_order_detail
;;
  "sku_info")
     import_sku_info
;;
  "user_info")
     import_user_info
;;
  "payment_info")
     import_payment_info
;;
  "base_province")
     import_base_province
;;
  "activity_info")
      import_activity_info
;;
  "cart_info")
      import_cart_info
;;
  "comment_info")
      import_comment_info
;;
  "coupon_info")
      import_coupon_info
;;
  "coupon_use")
      import_coupon_use
;;
  "favor_info")
      import_favor_info
;;
  "order_refund_info")
      import_order_refund_info
;;
  "order_status_log")
      import_order_status_log
;;
  "spu_info")
      import_spu_info
;;
  "activity_rule")
      import_activity_rule
;;
  "base_dic")
      import_base_dic
;;
  "order_detail_activity")
      import_order_detail_activity
;;
  "order_detail_coupon")
      import_order_detail_coupon
;;
  "refund_payment")
      import_refund_payment
;;
  "sku_attr_value")
      import_sku_attr_value
;;
  "sku_sale_attr_value")
      import_sku_sale_attr_value
;;
"all")
   import_base_category1
   import_base_category2
   import_base_category3
   import_order_info
   import_order_detail
   import_sku_info
   import_user_info
   import_payment_info
   import_base_trademark
   import_activity_info
   import_cart_info
   import_comment_info
   import_coupon_use
   import_coupon_info
   import_favor_info
   import_order_refund_info
   import_order_status_log
   import_spu_info
   import_activity_rule
   import_base_dic
   import_order_detail_activity
   import_order_detail_coupon
   import_refund_payment
   import_sku_attr_value
   import_sku_sale_attr_value
;;
esac
```

### 3.6 Hive安装部署

**把apache-hive-3.1.2-bin.tar.gz上传到hadoop01的/opt/software目录下**

![image-20230317105713170](/img/image-20230317105713170.png)



**解压apache-hive-3.1.2-bin.tar.gz到/opt/module/目录下面**

```bash
    tar -zxvf /opt/software/apache-hive-3.1.2-bin.tar.gz -C /opt/module/
```

**修改apache-hive-3.1.2-bin.tar.gz的名称为hive**

```bash
mv /opt/module/apache-hive-3.1.2-bin/ /opt/module/hive
```

**修改/etc/profile.d/my_env.sh，添加环境变量**

```bash
vim /etc/profile.d/my_env.sh
```

```shell
#HIVE_HOME
export HIVE_HOME=/opt/module/hive
export PATH=$HIVE_HOME/bin:$PATH
```

**source一下 /etc/profile.d/my_env.sh文件，使环境变量生效**

```bash
source /etc/profile.d/my_env.sh
```

**解决日志Jar包冲突，进入/opt/module/hive/lib目录**

```
mv /opt/module/hive/lib/log4j-slf4j-impl-2.10.0.jar /opt/module/hive/lib/log4j-slf4j-impl-2.10.0.jar.bak
```

**Hive元数据配置到MySQL**

```bash
cp /opt/software/mysql-connector-java-5.1.27-bin.jar /opt/module/hive/lib/
```

**配置Metastore到MySQL**

```bash
vim /opt/module/hive/conf/hive-site.xml
```

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:mysql://hadoop01:3306/metastore?useSSL=false&amp;useUnicode=true&amp;characterEncoding=UTF-8</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>com.mysql.jdbc.Driver</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>root</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>root</value>
    </property>

    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>/user/hive/warehouse</value>
    </property>

    <property>
        <name>hive.metastore.schema.verification</name>
        <value>false</value>
    </property>

    <property>
    <name>hive.server2.thrift.port</name>
    <value>10000</value>
    </property>

    <property>
        <name>hive.server2.thrift.bind.host</name>
        <value>hadoop01</value>
    </property>

    <property>
        <name>hive.metastore.event.db.notification.api.auth</name>
        <value>false</value>
    </property>
    
    <property>
        <name>hive.cli.print.header</name>
        <value>true</value>
    </property>

    <property>
        <name>hive.cli.print.current.db</name>
        <value>true</value>
    </property>

    <property>
    	<name>hive.server2.active.passive.ha.enable</name>
    	<value>true</value>
    </property>

    <property>
        <name>hive.server2.logging.operation.enabled</name>
        <value>false</value>
    </property>

</configuration>
```

**初始化元数据库**

```bash
mysql -uroot -proot
```

```mysql
create database metastore;
quit;
```

```bash
schematool -initSchema -dbType mysql -verbose
```

**启动Hive客户端**

```bash
hive
```

```hive
hive (default)> show databases;
OK
database_name
default
```



## 第四章、搭建整体数据仓库

### 4.1 数据仓库理论

#### 4.1.1 数据仓库的命名规范

**表命名规范**

- **Ø ODS层命名为ods_表名**
- **Ø DIM层命名为dim_表名**
- **Ø DWD层命名为dwd_表名**
- **Ø DWS层命名为dws_表名**
- **Ø DWT层命名为dwt_表名**
- **Ø ADS层命名为ads_表名**
- **Ø 临时表命名为tmp_表名**

**脚本命名规范**

- **数据源__to__目标_db/log.sh**
- **用户行为脚本以log为后缀；业务数据脚本以db为后缀**

**表字段类型**

- **数量类型为bigint**
- **金额类型为decimal(16,2),表示：16位有效数，其中小数部分2位**
- **字符型（名字，描述信息等 ）类型为string**
- **主键外键类型为string**
- **时间戳类型为bigint**

#### 4.1.2 维度表和事实表简介

- 维度表：一般是对事物的描述信息。每一张维度表对应现实世界的一个对象或者概念。例如：用户、商品、日期、地区等

- 事实表：事实表中的每一行数据代表一个业务事件（下单、支付、退款、评价等）。“事实”这个术语表示的是业务事件的度量值 （可统计次数、个数、金额等）




### 4.2 数仓环境搭建

#### 4.2.1 **Hive on Spark配置**

**Hive引擎介绍**

- **Hive引擎包括：默认MR、tez、spark**
- Hive on Spark：Hive即作为存储元数据又负责SQL的解析优化，语法是HQL语法，执行引擎变成了Spark，Spark负责采用RDD执行

**上传并解压，解压spark-3.0.0-bin-hadoop3.2.tgz**

![image-20230317160042013](/img/image-20230317160042013.png)

```bash
tar -zxf /opt/software/spark-3.0.0-bin-hadoop3.2.tgz -C /opt/module
mv /opt/module/spark-3.0.0-bin-hadoop3.2 /opt/module/spark
```

**配置SPARK_HOME环境变量**

```bash
vim /etc/profile.d/my_env.sh 
```

```shell
#SPARK_HOME
export SPARK_HOME=/opt/module/spark
export PATH=$PATH:$SPARK_HOME/bin
```

**source 使其生效**

```bash
source /etc/profile
```

**更改spark文件配置**

```bash
cp /opt/module/spark/conf/spark-env.sh.template /opt/module/spark/conf/spark-env.sh
vim /opt/module/spark/conf/spark-env.sh
```

```sh
HADOOP_CONF_DIR=/opt/module/hadoop-3.1.3/etc/hadoop
YARN_CONF_DIR=/opt/module/hadoop-3.1.3/etc/hadoop
```

```bash
cp /opt/module/spark/conf/spark-defaults.conf.template /opt/module/spark/conf/spark-defaults.conf
vim /opt/module/spark/conf/spark-defaults.conf
```

```conf
spark.master					yarn
spark.eventLog.enabled			true
spark.eventLog.dir				hdfs://hadoop01:8020/directory
spark.driver.memory   			1g
spark.executor.memory			1g
```

**在HDFS创建如下路径，用于存储历史日志**

```bash
hadoop fs -mkdir /spark-history
hadoop fs -mkdir /directory
```

**上传并解压spark-3.0.0-bin-without-hadoop.tgz**

```bash
tar -zxvf /opt/software/spark-3.0.0-bin-without-hadoop.tgz
```

**上传Spark纯净版jar包到HDFS**

```bash
hadoop fs -mkdir /spark-jars
hadoop fs -put /opt/software/spark-3.0.0-bin-without-hadoop/jars/* /spark-jars
```

**修改hive-site.xml文件**

```bash
vim /opt/module/hive/conf/hive-site.xml
```

```xml
<!--Spark依赖位置（注意：端口号8020必须和namenode的端口号一致）-->
<property>
    <name>spark.yarn.jars</name>
    <value>hdfs://hadoop01:8020/spark-jars/*</value>
</property>
  
<!--Hive执行引擎-->
<property>
    <name>hive.execution.engine</name>
    <value>spark</value>
</property>
```

**启动hive客户端**

```bash
hive
```

```hive
hive (default)> create table student(id int, name string);
hive (default)> insert into table student values(1,"zhangsan");
```

![image-20230318150022045](/img/image-20230318150022045.png)

#### 4.2.2 Yarn配置

**增加ApplicationMaster资源比例**

```bash
vim /opt/module/hadoop-3.1.3/etc/hadoop/capacity-scheduler.xml
```

```xml
<property>
    <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
    <value>0.7</value>
    <description>
      Maximum percent of resources in the cluster which can be used to run 
      application masters i.e. controls number of concurrent running
      applications.
    </description>
</property>
```

**分发capacity-scheduler.xml配置文件**

```
xsync /opt/module/hadoop-3.1.3/etc/hadoop/capacity-scheduler.xml
```

**关闭正在运行的任务，重新启动yarn集群**

```bash
stop-yarn.sh
start-yarn.sh
```



## 第五章、数据仓库（离线）

### 5.1、数仓分层

![image-20230609143636509](/img/image-20230609143636509.png)

### 5.2、ODS脚本

#### **5.2.1、ODS层日志表加载数据脚本**

```shell
#!/bin/bash

# 定义变量方便修改
APP=gmall

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
   do_date=$1
else 
   do_date=`date -d "-1 day" +%F`
fi 

echo ================== 日志日期为 $do_date ==================
sql="
load data inpath '/origin_data/$APP/log/topic_log/$do_date' into table ${APP}.ods_log partition(dt='$do_date');
"

hive -e "$sql"
```

#### **5.2.2、ODS层业务表首日数据装载脚本**

```shell
#!/bin/bash

APP=gmall

if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi 

ods_order_info=" 
load data inpath '/origin_data/$APP/db/order_info/$do_date' OVERWRITE into table ${APP}.ods_order_info partition(dt='$do_date');"

ods_order_detail="
load data inpath '/origin_data/$APP/db/order_detail/$do_date' OVERWRITE into table ${APP}.ods_order_detail partition(dt='$do_date');"

ods_sku_info="
load data inpath '/origin_data/$APP/db/sku_info/$do_date' OVERWRITE into table ${APP}.ods_sku_info partition(dt='$do_date');"

ods_user_info="
load data inpath '/origin_data/$APP/db/user_info/$do_date' OVERWRITE into table ${APP}.ods_user_info partition(dt='$do_date');"

ods_payment_info="
load data inpath '/origin_data/$APP/db/payment_info/$do_date' OVERWRITE into table ${APP}.ods_payment_info partition(dt='$do_date');"

ods_base_category1="
load data inpath '/origin_data/$APP/db/base_category1/$do_date' OVERWRITE into table ${APP}.ods_base_category1 partition(dt='$do_date');"

ods_base_category2="
load data inpath '/origin_data/$APP/db/base_category2/$do_date' OVERWRITE into table ${APP}.ods_base_category2 partition(dt='$do_date');"

ods_base_category3="
load data inpath '/origin_data/$APP/db/base_category3/$do_date' OVERWRITE into table ${APP}.ods_base_category3 partition(dt='$do_date'); "

ods_base_trademark="
load data inpath '/origin_data/$APP/db/base_trademark/$do_date' OVERWRITE into table ${APP}.ods_base_trademark partition(dt='$do_date'); "

ods_activity_info="
load data inpath '/origin_data/$APP/db/activity_info/$do_date' OVERWRITE into table ${APP}.ods_activity_info partition(dt='$do_date'); "

ods_cart_info="
load data inpath '/origin_data/$APP/db/cart_info/$do_date' OVERWRITE into table ${APP}.ods_cart_info partition(dt='$do_date'); "

ods_comment_info="
load data inpath '/origin_data/$APP/db/comment_info/$do_date' OVERWRITE into table ${APP}.ods_comment_info partition(dt='$do_date'); "

ods_coupon_info="
load data inpath '/origin_data/$APP/db/coupon_info/$do_date' OVERWRITE into table ${APP}.ods_coupon_info partition(dt='$do_date'); "

ods_coupon_use="
load data inpath '/origin_data/$APP/db/coupon_use/$do_date' OVERWRITE into table ${APP}.ods_coupon_use partition(dt='$do_date'); "

ods_favor_info="
load data inpath '/origin_data/$APP/db/favor_info/$do_date' OVERWRITE into table ${APP}.ods_favor_info partition(dt='$do_date'); "

ods_order_refund_info="
load data inpath '/origin_data/$APP/db/order_refund_info/$do_date' OVERWRITE into table ${APP}.ods_order_refund_info partition(dt='$do_date'); "

ods_order_status_log="
load data inpath '/origin_data/$APP/db/order_status_log/$do_date' OVERWRITE into table ${APP}.ods_order_status_log partition(dt='$do_date'); "

ods_spu_info="
load data inpath '/origin_data/$APP/db/spu_info/$do_date' OVERWRITE into table ${APP}.ods_spu_info partition(dt='$do_date'); "

ods_activity_rule="
load data inpath '/origin_data/$APP/db/activity_rule/$do_date' OVERWRITE into table ${APP}.ods_activity_rule partition(dt='$do_date');" 

ods_base_dic="
load data inpath '/origin_data/$APP/db/base_dic/$do_date' OVERWRITE into table ${APP}.ods_base_dic partition(dt='$do_date'); "

ods_order_detail_activity="
load data inpath '/origin_data/$APP/db/order_detail_activity/$do_date' OVERWRITE into table ${APP}.ods_order_detail_activity partition(dt='$do_date'); "

ods_order_detail_coupon="
load data inpath '/origin_data/$APP/db/order_detail_coupon/$do_date' OVERWRITE into table ${APP}.ods_order_detail_coupon partition(dt='$do_date'); "

ods_refund_payment="
load data inpath '/origin_data/$APP/db/refund_payment/$do_date' OVERWRITE into table ${APP}.ods_refund_payment partition(dt='$do_date'); "

ods_sku_attr_value="
load data inpath '/origin_data/$APP/db/sku_attr_value/$do_date' OVERWRITE into table ${APP}.ods_sku_attr_value partition(dt='$do_date'); "

ods_sku_sale_attr_value="
load data inpath '/origin_data/$APP/db/sku_sale_attr_value/$do_date' OVERWRITE into table ${APP}.ods_sku_sale_attr_value partition(dt='$do_date'); "

ods_base_province=" 
load data inpath '/origin_data/$APP/db/base_province/$do_date' OVERWRITE into table ${APP}.ods_base_province;"

ods_base_region="
load data inpath '/origin_data/$APP/db/base_region/$do_date' OVERWRITE into table ${APP}.ods_base_region;"

case $1 in
    "ods_order_info"){
        hive -e "$ods_order_info"
    };;
    "ods_order_detail"){
        hive -e "$ods_order_detail"
    };;
    "ods_sku_info"){
        hive -e "$ods_sku_info"
    };;
    "ods_user_info"){
        hive -e "$ods_user_info"
    };;
    "ods_payment_info"){
        hive -e "$ods_payment_info"
    };;
    "ods_base_category1"){
        hive -e "$ods_base_category1"
    };;
    "ods_base_category2"){
        hive -e "$ods_base_category2"
    };;
    "ods_base_category3"){
        hive -e "$ods_base_category3"
    };;
    "ods_base_trademark"){
        hive -e "$ods_base_trademark"
    };;
    "ods_activity_info"){
        hive -e "$ods_activity_info"
    };;
    "ods_cart_info"){
        hive -e "$ods_cart_info"
    };;
    "ods_comment_info"){
        hive -e "$ods_comment_info"
    };;
    "ods_coupon_info"){
        hive -e "$ods_coupon_info"
    };;
    "ods_coupon_use"){
        hive -e "$ods_coupon_use"
    };;
    "ods_favor_info"){
        hive -e "$ods_favor_info"
    };;
    "ods_order_refund_info"){
        hive -e "$ods_order_refund_info"
    };;
    "ods_order_status_log"){
        hive -e "$ods_order_status_log"
    };;
    "ods_spu_info"){
        hive -e "$ods_spu_info"
    };;
    "ods_activity_rule"){
        hive -e "$ods_activity_rule"
    };;
    "ods_base_dic"){
        hive -e "$ods_base_dic"
    };;
    "ods_order_detail_activity"){
        hive -e "$ods_order_detail_activity"
    };;
    "ods_order_detail_coupon"){
        hive -e "$ods_order_detail_coupon"
    };;
    "ods_refund_payment"){
        hive -e "$ods_refund_payment"
    };;
    "ods_sku_attr_value"){
        hive -e "$ods_sku_attr_value"
    };;
    "ods_sku_sale_attr_value"){
        hive -e "$ods_sku_sale_attr_value"
    };;
    "ods_base_province"){
        hive -e "$ods_base_province"
    };;
    "ods_base_region"){
        hive -e "$ods_base_region"
    };;
    "all"){
        hive -e "$ods_order_info$ods_order_detail$ods_sku_info$ods_user_info$ods_payment_info$ods_base_category1$ods_base_category2$ods_base_category3$ods_base_trademark$ods_activity_info$ods_cart_info$ods_comment_info$ods_coupon_info$ods_coupon_use$ods_favor_info$ods_order_refund_info$ods_order_status_log$ods_spu_info$ods_activity_rule$ods_base_dic$ods_order_detail_activity$ods_order_detail_coupon$ods_refund_payment$ods_sku_attr_value$ods_sku_sale_attr_value$ods_base_province$ods_base_region"
    };;
esac
```

#### 5.2.3、ODS层业务表每日数据装载脚本

```shell
#!/bin/bash

APP=gmall

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

ods_order_info=" 
load data inpath '/origin_data/$APP/db/order_info/$do_date' OVERWRITE into table ${APP}.ods_order_info partition(dt='$do_date');"

ods_order_detail="
load data inpath '/origin_data/$APP/db/order_detail/$do_date' OVERWRITE into table ${APP}.ods_order_detail partition(dt='$do_date');"

ods_sku_info="
load data inpath '/origin_data/$APP/db/sku_info/$do_date' OVERWRITE into table ${APP}.ods_sku_info partition(dt='$do_date');"

ods_user_info="
load data inpath '/origin_data/$APP/db/user_info/$do_date' OVERWRITE into table ${APP}.ods_user_info partition(dt='$do_date');"

ods_payment_info="
load data inpath '/origin_data/$APP/db/payment_info/$do_date' OVERWRITE into table ${APP}.ods_payment_info partition(dt='$do_date');"

ods_base_category1="
load data inpath '/origin_data/$APP/db/base_category1/$do_date' OVERWRITE into table ${APP}.ods_base_category1 partition(dt='$do_date');"

ods_base_category2="
load data inpath '/origin_data/$APP/db/base_category2/$do_date' OVERWRITE into table ${APP}.ods_base_category2 partition(dt='$do_date');"

ods_base_category3="
load data inpath '/origin_data/$APP/db/base_category3/$do_date' OVERWRITE into table ${APP}.ods_base_category3 partition(dt='$do_date'); "

ods_base_trademark="
load data inpath '/origin_data/$APP/db/base_trademark/$do_date' OVERWRITE into table ${APP}.ods_base_trademark partition(dt='$do_date'); "

ods_activity_info="
load data inpath '/origin_data/$APP/db/activity_info/$do_date' OVERWRITE into table ${APP}.ods_activity_info partition(dt='$do_date'); "

ods_cart_info="
load data inpath '/origin_data/$APP/db/cart_info/$do_date' OVERWRITE into table ${APP}.ods_cart_info partition(dt='$do_date'); "

ods_comment_info="
load data inpath '/origin_data/$APP/db/comment_info/$do_date' OVERWRITE into table ${APP}.ods_comment_info partition(dt='$do_date'); "

ods_coupon_info="
load data inpath '/origin_data/$APP/db/coupon_info/$do_date' OVERWRITE into table ${APP}.ods_coupon_info partition(dt='$do_date'); "

ods_coupon_use="
load data inpath '/origin_data/$APP/db/coupon_use/$do_date' OVERWRITE into table ${APP}.ods_coupon_use partition(dt='$do_date'); "

ods_favor_info="
load data inpath '/origin_data/$APP/db/favor_info/$do_date' OVERWRITE into table ${APP}.ods_favor_info partition(dt='$do_date'); "

ods_order_refund_info="
load data inpath '/origin_data/$APP/db/order_refund_info/$do_date' OVERWRITE into table ${APP}.ods_order_refund_info partition(dt='$do_date'); "

ods_order_status_log="
load data inpath '/origin_data/$APP/db/order_status_log/$do_date' OVERWRITE into table ${APP}.ods_order_status_log partition(dt='$do_date'); "

ods_spu_info="
load data inpath '/origin_data/$APP/db/spu_info/$do_date' OVERWRITE into table ${APP}.ods_spu_info partition(dt='$do_date'); "

ods_activity_rule="
load data inpath '/origin_data/$APP/db/activity_rule/$do_date' OVERWRITE into table ${APP}.ods_activity_rule partition(dt='$do_date');" 

ods_base_dic="
load data inpath '/origin_data/$APP/db/base_dic/$do_date' OVERWRITE into table ${APP}.ods_base_dic partition(dt='$do_date'); "

ods_order_detail_activity="
load data inpath '/origin_data/$APP/db/order_detail_activity/$do_date' OVERWRITE into table ${APP}.ods_order_detail_activity partition(dt='$do_date'); "

ods_order_detail_coupon="
load data inpath '/origin_data/$APP/db/order_detail_coupon/$do_date' OVERWRITE into table ${APP}.ods_order_detail_coupon partition(dt='$do_date'); "

ods_refund_payment="
load data inpath '/origin_data/$APP/db/refund_payment/$do_date' OVERWRITE into table ${APP}.ods_refund_payment partition(dt='$do_date'); "

ods_sku_attr_value="
load data inpath '/origin_data/$APP/db/sku_attr_value/$do_date' OVERWRITE into table ${APP}.ods_sku_attr_value partition(dt='$do_date'); "

ods_sku_sale_attr_value="
load data inpath '/origin_data/$APP/db/sku_sale_attr_value/$do_date' OVERWRITE into table ${APP}.ods_sku_sale_attr_value partition(dt='$do_date'); "

ods_base_province=" 
load data inpath '/origin_data/$APP/db/base_province/$do_date' OVERWRITE into table ${APP}.ods_base_province;"

ods_base_region="
load data inpath '/origin_data/$APP/db/base_region/$do_date' OVERWRITE into table ${APP}.ods_base_region;"

case $1 in
    "ods_order_info"){
        hive -e "$ods_order_info"
    };;
    "ods_order_detail"){
        hive -e "$ods_order_detail"
    };;
    "ods_sku_info"){
        hive -e "$ods_sku_info"
    };;
    "ods_user_info"){
        hive -e "$ods_user_info"
    };;
    "ods_payment_info"){
        hive -e "$ods_payment_info"
    };;
    "ods_base_category1"){
        hive -e "$ods_base_category1"
    };;
    "ods_base_category2"){
        hive -e "$ods_base_category2"
    };;
    "ods_base_category3"){
        hive -e "$ods_base_category3"
    };;
    "ods_base_trademark"){
        hive -e "$ods_base_trademark"
    };;
    "ods_activity_info"){
        hive -e "$ods_activity_info"
    };;
    "ods_cart_info"){
        hive -e "$ods_cart_info"
    };;
    "ods_comment_info"){
        hive -e "$ods_comment_info"
    };;
    "ods_coupon_info"){
        hive -e "$ods_coupon_info"
    };;
    "ods_coupon_use"){
        hive -e "$ods_coupon_use"
    };;
    "ods_favor_info"){
        hive -e "$ods_favor_info"
    };;
    "ods_order_refund_info"){
        hive -e "$ods_order_refund_info"
    };;
    "ods_order_status_log"){
        hive -e "$ods_order_status_log"
    };;
    "ods_spu_info"){
        hive -e "$ods_spu_info"
    };;
    "ods_activity_rule"){
        hive -e "$ods_activity_rule"
    };;
    "ods_base_dic"){
        hive -e "$ods_base_dic"
    };;
    "ods_order_detail_activity"){
        hive -e "$ods_order_detail_activity"
    };;
    "ods_order_detail_coupon"){
        hive -e "$ods_order_detail_coupon"
    };;
    "ods_refund_payment"){
        hive -e "$ods_refund_payment"
    };;
    "ods_sku_attr_value"){
        hive -e "$ods_sku_attr_value"
    };;
    "ods_sku_sale_attr_value"){
        hive -e "$ods_sku_sale_attr_value"
    };;
    "all"){
        hive -e "$ods_order_info$ods_order_detail$ods_sku_info$ods_user_info$ods_payment_info$ods_base_category1$ods_base_category2$ods_base_category3$ods_base_trademark$ods_activity_info$ods_cart_info$ods_comment_info$ods_coupon_info$ods_coupon_use$ods_favor_info$ods_order_refund_info$ods_order_status_log$ods_spu_info$ods_activity_rule$ods_base_dic$ods_order_detail_activity$ods_order_detail_coupon$ods_refund_payment$ods_sku_attr_value$ods_sku_sale_attr_value"
    };;
esac
```



### 5.3、DIM脚本

####  5.3.1、DIM层首日数据装载脚本

```shell
#!/bin/bash

APP=gmall
APP2=gmall_dim

if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi 

dim_user_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP2}.dim_user_info partition (dt = '9999-99-99')
select id,
       login_name,
       nick_name,
       md5(name),
       md5(phone_num),
       md5(email),
       user_level,
       birthday,
       gender,
       create_time,
       operate_time,
       '$do_date',
       '9999-99-99'
from ${APP}.ods_user_info
where dt = '$do_date';
"

dim_sku_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
with
sku as
(
    select
        id,
        price,
        sku_name,
        sku_desc,
        weight,
        is_sale,
        spu_id,
        category3_id,
        tm_id,
        create_time
    from ${APP}.ods_sku_info
    where dt='$do_date'
),
spu as
(
    select
        id,
        spu_name
    from ${APP}.ods_spu_info
    where dt='$do_date'
),
c3 as
(
    select
        id,
        name,
        category2_id
    from ${APP}.ods_base_category3
    where dt='$do_date'
),
c2 as
(
    select
        id,
        name,
        category1_id
    from ${APP}.ods_base_category2
    where dt='$do_date'
),
c1 as
(
    select
        id,
        name
    from ${APP}.ods_base_category1
    where dt='$do_date'
),
tm as
(
    select
        id,
        tm_name
    from ${APP}.ods_base_trademark
    where dt='$do_date'
),
attr as
(
    select
        sku_id,
        collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
    from ${APP}.ods_sku_attr_value
    where dt='$do_date'
    group by sku_id
),
sale_attr as
(
    select
        sku_id,
        collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
    from ${APP}.ods_sku_sale_attr_value
    where dt='$do_date'
    group by sku_id
)

insert overwrite table ${APP2}.dim_sku_info partition(dt='$do_date')
select
    sku.id,
    sku.sku_name,
    sku.price,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    c2.category1_id,
    c1.name,
    c3.category2_id,
    c2.name,
    sku.category3_id,
    c3.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
left join spu on sku.spu_id=spu.id
left join c3 on sku.category3_id=c3.id
left join c2 on c3.category2_id=c2.id
left join c1 on c2.category1_id=c1.id
left join tm on sku.tm_id=tm.id
left join attr on sku.id=attr.sku_id
left join sale_attr on sku.id=sale_attr.sku_id;
"

dim_coupon_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP2}.dim_coupon_info partition (dt = '$do_date')
select id,
       coupon_name,
       coupon_type,
       condition_amount,
       condition_num,
       activity_id,
       benefit_amount,
       benefit_discount,
       create_time,
       range_type,
       limit_num,
       taken_count,
       start_time,
       end_time,
       operate_time,
       expire_time
from ${APP}.ods_coupon_info
where dt = '$do_date';
"

dim_activity_rule_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP2}.dim_activity_rule_info partition (dt = '$do_date')
select ar.id,
       ar.activity_id,
       ai.activity_name,
       ar.activity_type,
       ai.start_time,
       ai.end_time,
       ai.create_time,
       ar.condition_amount,
       ar.condition_num,
       ar.benefit_amount,
       ar.benefit_disscount,
       ar.benefit_level
from (
         select id,
                activity_id,
                activity_type,
                condition_amount,
                condition_num,
                benefit_amount,
                benefit_disscount,
                benefit_level
         from ${APP}.ods_activity_rule
         where dt = '$do_date'
     ) ar
         left join (
    select id,
           activity_name,
           activity_type,
           start_time,
           end_time,
           create_time
    from ${APP}.ods_activity_info
    where dt = '$do_date'
) ai
                   on ai.id = ar.activity_id;
"

case $1 in
"dim_user_info"){
    hive -e "$dim_user_info"
};;
"dim_sku_info"){
    hive -e "$dim_sku_info"
};;
"dim_base_province"){
    hive -e "$dim_base_province"
};;
"dim_coupon_info"){
    hive -e "$dim_coupon_info"
};;
"dim_activity_rule_info"){
    hive -e "$dim_activity_rule_info"
};;
"all"){
    hive -e "$dim_user_info$dim_sku_info$dim_coupon_info$dim_activity_rule_info$dim_base_province"
};;
esac
```

#### 5.3.2、DIM层每日数据装载脚本

```shell
#!/bin/bash

APP=gmall
APP2=gmall_dim

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
   do_date=$2
else 
   do_date=`date -d "-1 day" +%F`
   exit
fi 

dim_user_info="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
with
tmp as
(
    select
        old.id old_id,
        old.login_name old_login_name,
        old.nick_name old_nick_name,
        old.name old_name,
        old.phone_num old_phone_num,
        old.email old_email,
        old.user_level old_user_level,
        old.birthday old_birthday,
        old.gender old_gender,
        old.create_time old_create_time,
        old.operate_time old_operate_time,
        old.start_date old_start_date,
        old.end_date old_end_date,
        new.id new_id,
        new.login_name new_login_name,
        new.nick_name new_nick_name,
        new.name new_name,
        new.phone_num new_phone_num,
        new.email new_email,
        new.user_level new_user_level,
        new.birthday new_birthday,
        new.gender new_gender,
        new.create_time new_create_time,
        new.operate_time new_operate_time,
        new.start_date new_start_date,
        new.end_date new_end_date
    from
    (
        select
            id,
            login_name,
            nick_name,
            name,
            phone_num,
            email,
            user_level,
            birthday,
            gender,
            create_time,
            operate_time,
            start_date,
            end_date
        from ${APP2}.dim_user_info
        where dt='9999-99-99'
        and start_date<'$do_date'
    )old
    full outer join
    (
        select
            id,
            login_name,
            nick_name,
            md5(name) name,
            md5(phone_num) phone_num,
            md5(email) email,
            user_level,
            birthday,
            gender,
            create_time,
            operate_time,
            '$do_date' start_date,
            '9999-99-99' end_date
        from ${APP}.ods_user_info
        where dt='$do_date'
    )new
    on old.id=new.id
)
insert overwrite table ${APP2}.dim_user_info partition(dt)
select
    nvl(new_id,old_id),
    nvl(new_login_name,old_login_name),
    nvl(new_nick_name,old_nick_name),
    nvl(new_name,old_name),
    nvl(new_phone_num,old_phone_num),
    nvl(new_email,old_email),
    nvl(new_user_level,old_user_level),
    nvl(new_birthday,old_birthday),
    nvl(new_gender,old_gender),
    nvl(new_create_time,old_create_time),
    nvl(new_operate_time,old_operate_time),
    nvl(new_start_date,old_start_date),
    nvl(new_end_date,old_end_date),
    nvl(new_end_date,old_end_date) dt
from tmp
union all
select
    old_id,
    old_login_name,
    old_nick_name,
    old_name,
    old_phone_num,
    old_email,
    old_user_level,
    old_birthday,
    old_gender,
    old_create_time,
    old_operate_time,
    old_start_date,
    cast(date_add('$do_date',-1) as string),
    cast(date_add('$do_date',-1) as string) dt
from tmp
where new_id is not null and old_id is not null;
"

dim_sku_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
with
sku as
(
    select
        id,
        price,
        sku_name,
        sku_desc,
        weight,
        is_sale,
        spu_id,
        category3_id,
        tm_id,
        create_time
    from ${APP}.ods_sku_info
    where dt='$do_date'
),
spu as
(
    select
        id,
        spu_name
    from ${APP}.ods_spu_info
    where dt='$do_date'
),
c3 as
(
    select
        id,
        name,
        category2_id
    from ${APP}.ods_base_category3
    where dt='$do_date'
),
c2 as
(
    select
        id,
        name,
        category1_id
    from ${APP}.ods_base_category2
    where dt='$do_date'
),
c1 as
(
    select
        id,
        name
    from ${APP}.ods_base_category1
    where dt='$do_date'
),
tm as
(
    select
        id,
        tm_name
    from ${APP}.ods_base_trademark
    where dt='$do_date'
),
attr as
(
    select
        sku_id,
        collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
    from ${APP}.ods_sku_attr_value
    where dt='$do_date'
    group by sku_id
),
sale_attr as
(
    select
        sku_id,
        collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
    from ${APP}.ods_sku_sale_attr_value
    where dt='$do_date'
    group by sku_id
)

insert overwrite table ${APP2}.dim_sku_info partition(dt='$do_date')
select
    sku.id,
    sku.sku_name,
    sku.price,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    c2.category1_id,
    c1.name,
    c3.category2_id,
    c2.name,
    sku.category3_id,
    c3.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
left join spu on sku.spu_id=spu.id
left join c3 on sku.category3_id=c3.id
left join c2 on c3.category2_id=c2.id
left join c1 on c2.category1_id=c1.id
left join tm on sku.tm_id=tm.id
left join attr on sku.id=attr.sku_id
left join sale_attr on sku.id=sale_attr.sku_id;
"

dim_coupon_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP2}.dim_coupon_info partition (dt = '$do_date')
select id,
       coupon_name,
       coupon_type,
       condition_amount,
       condition_num,
       activity_id,
       benefit_amount,
       benefit_discount,
       create_time,
       range_type,
       limit_num,
       taken_count,
       start_time,
       end_time,
       operate_time,
       expire_time
from ${APP}.ods_coupon_info
where dt = '$do_date';
"

dim_activity_rule_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP2}.dim_activity_rule_info partition (dt = '$do_date')
select ar.id,
       ar.activity_id,
       ai.activity_name,
       ar.activity_type,
       ai.start_time,
       ai.end_time,
       ai.create_time,
       ar.condition_amount,
       ar.condition_num,
       ar.benefit_amount,
       ar.benefit_disscount,
       ar.benefit_level
from (
         select id,
                activity_id,
                activity_type,
                condition_amount,
                condition_num,
                benefit_amount,
                benefit_disscount,
                benefit_level
         from ${APP}.ods_activity_rule
         where dt = '$do_date'
     ) ar
         left join (
    select id,
           activity_name,
           activity_type,
           start_time,
           end_time,
           create_time
    from ${APP}.ods_activity_info
    where dt = '$do_date'
) ai
                   on ai.id = ar.activity_id;
"

case $1 in
"dim_user_info"){
    hive -e "$dim_user_info"
};;
"dim_sku_info"){
    hive -e "$dim_sku_info"
};;
"dim_base_province"){
    hive -e "$dim_base_province"
};;
"dim_coupon_info"){
    hive -e "$dim_coupon_info"
};;
"dim_activity_rule_info"){
    hive -e "$dim_activity_rule_info"
};;
"all"){
    hive -e "$dim_user_info$dim_sku_info$dim_coupon_info$dim_activity_rule_info$dim_base_province"
};;
esac
```



### 5.4、DWD脚本

#### 5.4.1、DWD层业务数据首日装载脚本

```shell
#！ /bin/bash
APP=gmall
APP2=gmall_ods
APP3=gmall_dwd

if [ -n "$2" ] ;then
	do_date=$2
else
	echo "请输入日期参数"
	exit
fi

dwd_order_info="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_order_info partition(dt)
select
    oi.id,
    oi.order_status,
    oi.user_id,
    oi.province_id,
    oi.payment_way,
    oi.delivery_address,
    oi.out_trade_no,
    oi.tracking_no,
    oi.create_time,
    times.ts['1002'] payment_time,
    times.ts['1003'] cancel_time,
    times.ts['1004'] finish_time,
    times.ts['1005'] refund_time,
    times.ts['1006'] refund_finish_time,
    oi.expire_time,
    feight_fee,
    feight_fee_reduce,
    activity_reduce_amount,
    coupon_reduce_amount,
    original_amount,
    final_amount,
    case
        when times.ts['1003'] is not null then date_format(times.ts['1003'],'yyyy-MM-dd')
        when times.ts['1004'] is not null and date_add(date_format(times.ts['1004'],'yyyy-MM-dd'),7)<='2020-06-14' and times.ts['1005'] is null then date_add(date_format(times.ts['1004'],'yyyy-MM-dd'),7)
        when times.ts['1006'] is not null then date_format(times.ts['1006'],'yyyy-MM-dd')
        when oi.expire_time is not null then date_format(oi.expire_time,'yyyy-MM-dd')
        else '9999-99-99'
    end
from
(
    select
        *
    from ${APP}.ods_order_info
    where dt='$do_date'
)oi
left join
(
    select
        order_id,
        str_to_map(concat_ws(',',collect_set(concat(order_status,'=',operate_time))),',','=') ts
    from ${APP}.ods_order_status_log
    where dt='$do_date'
    group by order_id
)times
on oi.id=times.order_id;
"

dwd_order_detail="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP3}.dwd_order_detail partition(dt)
select
    od.id,
    od.order_id,
    oi.user_id,
    od.sku_id,
    oi.province_id,
    oda.activity_id,
    oda.activity_rule_id,
    odc.coupon_id,
    od.create_time,
    od.source_type,
    od.source_id,
    od.sku_num,
    od.order_price*od.sku_num,
    od.split_activity_amount,
    od.split_coupon_amount,
    od.split_final_amount,
    date_format(create_time,'yyyy-MM-dd')
from
(
    select
        *
    from ${APP}.ods_order_detail
    where dt='$do_date'
)od
left join
(
    select
        id,
        user_id,
        province_id
    from ${APP}.ods_order_info
    where dt='$do_date'
)oi
on od.order_id=oi.id
left join
(
    select
        order_detail_id,
        activity_id,
        activity_rule_id
    from ${APP}.ods_order_detail_activity
    where dt='$do_date'
)oda
on od.id=oda.order_detail_id
left join
(
    select
        order_detail_id,
        coupon_id
    from ${APP}.ods_order_detail_coupon
    where dt='$do_date'
)odc
on od.id=odc.order_detail_id;
"

dwd_payment_info="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_payment_info partition(dt)
select
    pi.id,
    pi.order_id,
    pi.user_id,
    oi.province_id,
    pi.trade_no,
    pi.out_trade_no,
    pi.payment_type,
    pi.payment_amount,
    pi.payment_status,
    pi.create_time,
    pi.callback_time,
    nvl(date_format(pi.callback_time,'yyyy-MM-dd'),'9999-99-99')
from
(
    select * from ${APP}.ods_payment_info where dt='$do_date'
)pi
left join
(
    select id,province_id from ${APP}.ods_order_info where dt='$do_date'
)oi
on pi.order_id=oi.id;
"

dwd_cart_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_cart_info partition(dt='$do_date')
select
    id,
    user_id,
    sku_id,
    source_type,
    source_id,
    cart_price,
    is_ordered,
    create_time,
    operate_time,
    order_time,
    sku_num
from ${APP}.ods_cart_info
where dt='$do_date';
"

dwd_comment_info="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_comment_info partition (dt)
select
    id,
    user_id,
    sku_id,
    spu_id,
    order_id,
    appraise,
    create_time,
    date_format(create_time,'yyyy-MM-dd')
from ${APP}.ods_comment_info
where dt='$do_date';
"

dwd_favor_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_favor_info partition(dt='$do_date')
select
    id,
    user_id,
    sku_id,
    spu_id,
    is_cancel,
    create_time,
    cancel_time
from ${APP}.ods_favor_info
where dt='$do_date';
"

dwd_coupon_use="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_coupon_use partition(dt)
select
    id,
    coupon_id,
    user_id,
    order_id,
    coupon_status,
    get_time,
    using_time,
    used_time,
    expire_time,
    coalesce(date_format(used_time,'yyyy-MM-dd'),date_format(expire_time,'yyyy-MM-dd'),'9999-99-99')
from ${APP}.ods_coupon_use
where dt='$do_date';"

dwd_order_refund_info="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_order_refund_info partition(dt)
select
    ri.id,
    ri.user_id,
    ri.order_id,
    ri.sku_id,
    oi.province_id,
    ri.refund_type,
    ri.refund_num,
    ri.refund_amount,
    ri.refund_reason_type,
    ri.create_time,
    date_format(ri.create_time,'yyyy-MM-dd')
from
(
    select * from ${APP}.ods_order_refund_info where dt='$do_date'
)ri
left join
(
    select id,province_id from ${APP}.ods_order_info where dt='$do_date'
)oi
on ri.order_id=oi.id;"

dwd_refund_payment="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_refund_payment partition(dt)
select
    rp.id,
    user_id,
    order_id,
    sku_id,
    province_id,
    trade_no,
    out_trade_no,
    payment_type,
    refund_amount,
    refund_status,
    create_time,
    callback_time,
    nvl(date_format(callback_time,'yyyy-MM-dd'),'9999-99-99')
from
(
    select
        id,
        out_trade_no,
        order_id,
        sku_id,
        payment_type,
        trade_no,
        refund_amount,
        refund_status,
        create_time,
        callback_time
    from ${APP}.ods_refund_payment
    where dt='$do_date'
)rp
left join
(
    select
        id,
        user_id,
        province_id
    from ${APP}.ods_order_info
    where dt='$do_date'
)oi
on rp.order_id=oi.id;"

case $1 in
    dwd_order_info )
        hive -e "$dwd_order_info"
    ;;
    dwd_order_detail )
        hive -e "$dwd_order_detail"
    ;;
    dwd_payment_info )
        hive -e "$dwd_payment_info"
    ;;
    dwd_cart_info )
        hive -e "$dwd_cart_info"
    ;;
    dwd_comment_info )
        hive -e "$dwd_comment_info"
    ;;
    dwd_favor_info )
        hive -e "$dwd_favor_info"
    ;;
    dwd_coupon_use )
        hive -e "$dwd_coupon_use"
    ;;
    dwd_order_refund_info )
        hive -e "$dwd_order_refund_info"
    ;;
    dwd_refund_payment )
        hive -e "$dwd_refund_payment"
    ;;
    all )
        hive -e "$dwd_order_info$dwd_order_detail$dwd_payment_info$dwd_cart_info$dwd_comment_info$dwd_favor_info$dwd_coupon_use$dwd_order_refund_info$dwd_refund_payment"
    ;;
esac
```



#### 5.4.2、 DWD层业务数据每日装载脚本

```shell
#!/bin/bash

APP=gmall
APP2=gmall_dim
APP3=gmall_dwd
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi


# 假设某累积型快照事实表，某天所有的业务记录全部完成，则会导致9999-99-99分区的数据未被覆盖，从而导致数据重复，该函数根据9999-99-99分区的数据的末次修改时间判断其是否被覆盖了，如果未被覆盖，就手动清理
clear_data(){
    current_date=`date +%F`
    current_date_timestamp=`date -d "$current_date" +%s`

    last_modified_date=`hadoop fs -ls /warehouse/gmall/dwd/$1 | grep '9999-99-99' | awk '{print $6}'`
    last_modified_date_timestamp=`date -d "$last_modified_date" +%s`

    if [[ $last_modified_date_timestamp -lt $current_date_timestamp ]]; then
        echo "clear table $1 partition(dt=9999-99-99)"
        hadoop fs -rm -r -f /warehouse/gmall/dwd/$1/dt=9999-99-99/*
    fi
}

dwd_order_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP3}.dwd_order_info partition(dt)
select
    nvl(new.id,old.id),
    nvl(new.order_status,old.order_status),
    nvl(new.user_id,old.user_id),
    nvl(new.province_id,old.province_id),
    nvl(new.payment_way,old.payment_way),
    nvl(new.delivery_address,old.delivery_address),
    nvl(new.out_trade_no,old.out_trade_no),
    nvl(new.tracking_no,old.tracking_no),
    nvl(new.create_time,old.create_time),
    nvl(new.payment_time,old.payment_time),
    nvl(new.cancel_time,old.cancel_time),
    nvl(new.finish_time,old.finish_time),
    nvl(new.refund_time,old.refund_time),
    nvl(new.refund_finish_time,old.refund_finish_time),
    nvl(new.expire_time,old.expire_time),
    nvl(new.feight_fee,old.feight_fee),
    nvl(new.feight_fee_reduce,old.feight_fee_reduce),
    nvl(new.activity_reduce_amount,old.activity_reduce_amount),
    nvl(new.coupon_reduce_amount,old.coupon_reduce_amount),
    nvl(new.original_amount,old.original_amount),
    nvl(new.final_amount,old.final_amount),
    case
        when new.cancel_time is not null then date_format(new.cancel_time,'yyyy-MM-dd')
        when new.finish_time is not null and date_add(date_format(new.finish_time,'yyyy-MM-dd'),7)='$do_date' and new.refund_time is null then '$do_date'
        when new.refund_finish_time is not null then date_format(new.refund_finish_time,'yyyy-MM-dd')
        when new.expire_time is not null then date_format(new.expire_time,'yyyy-MM-dd')
        else '9999-99-99'
    end
from
(
    select
        id,
        order_status,
        user_id,
        province_id,
        payment_way,
        delivery_address,
        out_trade_no,
        tracking_no,
        create_time,
        payment_time,
        cancel_time,
        finish_time,
        refund_time,
        refund_finish_time,
        expire_time,
        feight_fee,
        feight_fee_reduce,
        activity_reduce_amount,
        coupon_reduce_amount,
        original_amount,
        final_amount
    from ${APP3}.dwd_order_info
    where dt='9999-99-99'
)old
full outer join
(
    select
        oi.id,
        oi.order_status,
        oi.user_id,
        oi.province_id,
        oi.payment_way,
        oi.delivery_address,
        oi.out_trade_no,
        oi.tracking_no,
        oi.create_time,
        times.ts['1002'] payment_time,
        times.ts['1003'] cancel_time,
        times.ts['1004'] finish_time,
        times.ts['1005'] refund_time,
        times.ts['1006'] refund_finish_time,
        oi.expire_time,
        feight_fee,
        feight_fee_reduce,
        activity_reduce_amount,
        coupon_reduce_amount,
        original_amount,
        final_amount
    from
    (
        select
            *
        from ${APP}.ods_order_info
        where dt='$do_date'
    )oi
    left join
    (
        select
            order_id,
            str_to_map(concat_ws(',',collect_set(concat(order_status,'=',operate_time))),',','=') ts
        from ${APP}.ods_order_status_log
        where dt='$do_date'
        group by order_id
    )times
    on oi.id=times.order_id
)new
on old.id=new.id;
"

dwd_order_detail="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_order_detail partition(dt='$do_date')
select
    od.id,
    od.order_id,
    oi.user_id,
    od.sku_id,
    oi.province_id,
    oda.activity_id,
    oda.activity_rule_id,
    odc.coupon_id,
    od.create_time,
    od.source_type,
    od.source_id,
    od.sku_num,
    od.order_price*od.sku_num,
    od.split_activity_amount,
    od.split_coupon_amount,
    od.split_final_amount
from
(
    select
        *
    from ${APP}.ods_order_detail
    where dt='$do_date'
)od
left join
(
    select
        id,
        user_id,
        province_id
    from ${APP}.ods_order_info
    where dt='$do_date'
)oi
on od.order_id=oi.id
left join
(
    select
        order_detail_id,
        activity_id,
        activity_rule_id
    from ${APP}.ods_order_detail_activity
    where dt='$do_date'
)oda
on od.id=oda.order_detail_id
left join
(
    select
        order_detail_id,
        coupon_id
    from ${APP}.ods_order_detail_coupon
    where dt='$do_date'
)odc
on od.id=odc.order_detail_id;
"


dwd_payment_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP3}.dwd_payment_info partition(dt)
select
    nvl(new.id,old.id),
    nvl(new.order_id,old.order_id),
    nvl(new.user_id,old.user_id),
    nvl(new.province_id,old.province_id),
    nvl(new.trade_no,old.trade_no),
    nvl(new.out_trade_no,old.out_trade_no),
    nvl(new.payment_type,old.payment_type),
    nvl(new.payment_amount,old.payment_amount),
    nvl(new.payment_status,old.payment_status),
    nvl(new.create_time,old.create_time),
    nvl(new.callback_time,old.callback_time),
    nvl(date_format(nvl(new.callback_time,old.callback_time),'yyyy-MM-dd'),'9999-99-99')
from
(
    select id,
       order_id,
       user_id,
       province_id,
       trade_no,
       out_trade_no,
       payment_type,
       payment_amount,
       payment_status,
       create_time,
       callback_time
    from ${APP3}.dwd_payment_info
    where dt = '9999-99-99'
)old
full outer join
(
    select
        pi.id,
        pi.out_trade_no,
        pi.order_id,
        pi.user_id,
        oi.province_id,
        pi.payment_type,
        pi.trade_no,
        pi.payment_amount,
        pi.payment_status,
        pi.create_time,
        pi.callback_time
    from
    (
        select * from ${APP}.ods_payment_info where dt='$do_date'
    )pi
    left join
    (
        select id,province_id from ${APP}.ods_order_info where dt='$do_date'
    )oi
    on pi.order_id=oi.id
)new
on old.id=new.id;
"

dwd_cart_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_cart_info partition(dt='$do_date')
select
    id,
    user_id,
    sku_id,
    source_type,
    source_id,
    cart_price,
    is_ordered,
    create_time,
    operate_time,
    order_time,
    sku_num
from ${APP}.ods_cart_info
where dt='$do_date';
"


dwd_comment_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_comment_info partition(dt='$do_date')
select
    id,
    user_id,
    sku_id,
    spu_id,
    order_id,
    appraise,
    create_time
from ${APP}.ods_comment_info where dt='$do_date';"


dwd_favor_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_favor_info partition(dt='$do_date')
select
    id,
    user_id,
    sku_id,
    spu_id,
    is_cancel,
    create_time,
    cancel_time
from ${APP}.ods_favor_info
where dt='$do_date';
"


dwd_coupon_use="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP3}.dwd_coupon_use partition(dt)
select
    nvl(new.id,old.id),
    nvl(new.coupon_id,old.coupon_id),
    nvl(new.user_id,old.user_id),
    nvl(new.order_id,old.order_id),
    nvl(new.coupon_status,old.coupon_status),
    nvl(new.get_time,old.get_time),
    nvl(new.using_time,old.using_time),
    nvl(new.used_time,old.used_time),
    nvl(new.expire_time,old.expire_time),
    coalesce(date_format(nvl(new.used_time,old.used_time),'yyyy-MM-dd'),date_format(nvl(new.expire_time,old.expire_time),'yyyy-MM-dd'),'9999-99-99')
from
(
    select
        id,
        coupon_id,
        user_id,
        order_id,
        coupon_status,
        get_time,
        using_time,
        used_time,
        expire_time
    from ${APP3}.dwd_coupon_use
    where dt='9999-99-99'
)old
full outer join
(
    select
        id,
        coupon_id,
        user_id,
        order_id,
        coupon_status,
        get_time,
        using_time,
        used_time,
        expire_time
    from ${APP}.ods_coupon_use
    where dt='$do_date'
)new
on old.id=new.id;
"

dwd_order_refund_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP3}.dwd_order_refund_info partition(dt='$do_date')
select
    ri.id,
    ri.user_id,
    ri.order_id,
    ri.sku_id,
    oi.province_id,
    ri.refund_type,
    ri.refund_num,
    ri.refund_amount,
    ri.refund_reason_type,
    ri.create_time
from
(
    select * from ${APP}.ods_order_refund_info where dt='$do_date'
)ri
left join
(
    select id,province_id from ${APP}.ods_order_info where dt='$do_date'
)oi
on ri.order_id=oi.id;
"


dwd_refund_payment="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP3}.dwd_refund_payment partition(dt)
select
    nvl(new.id,old.id),
    nvl(new.user_id,old.user_id),
    nvl(new.order_id,old.order_id),
    nvl(new.sku_id,old.sku_id),
    nvl(new.province_id,old.province_id),
    nvl(new.trade_no,old.trade_no),
    nvl(new.out_trade_no,old.out_trade_no),
    nvl(new.payment_type,old.payment_type),
    nvl(new.refund_amount,old.refund_amount),
    nvl(new.refund_status,old.refund_status),
    nvl(new.create_time,old.create_time),
    nvl(new.callback_time,old.callback_time),
    nvl(date_format(nvl(new.callback_time,old.callback_time),'yyyy-MM-dd'),'9999-99-99')
from
(
    select
        id,
        user_id,
        order_id,
        sku_id,
        province_id,
        trade_no,
        out_trade_no,
        payment_type,
        refund_amount,
        refund_status,
        create_time,
        callback_time
    from ${APP3}.dwd_refund_payment
    where dt='9999-99-99'
)old
full outer join
(
    select
        rp.id,
        user_id,
        order_id,
        sku_id,
        province_id,
        trade_no,
        out_trade_no,
        payment_type,
        refund_amount,
        refund_status,
        create_time,
        callback_time
    from
    (
        select
            id,
            out_trade_no,
            order_id,
            sku_id,
            payment_type,
            trade_no,
            refund_amount,
            refund_status,
            create_time,
            callback_time
        from ${APP}.ods_refund_payment
        where dt='$do_date'
    )rp
    left join
    (
        select
            id,
            user_id,
            province_id
        from ${APP}.ods_order_info
        where dt='$do_date'
    )oi
    on rp.order_id=oi.id
)new
on old.id=new.id;
"

case $1 in
    dwd_order_info )
        hive -e "$dwd_order_info"
        clear_data dwd_order_info
    ;;
    dwd_order_detail )
        hive -e "$dwd_order_detail"
    ;;
    dwd_payment_info )
        hive -e "$dwd_payment_info"
        clear_data dwd_payment_info
    ;;
    dwd_cart_info )
        hive -e "$dwd_cart_info"
    ;;
    dwd_comment_info )
        hive -e "$dwd_comment_info"
    ;;
    dwd_favor_info )
        hive -e "$dwd_favor_info"
    ;;
    dwd_coupon_use )
        hive -e "$dwd_coupon_use"
        clear_data dwd_coupon_use
    ;;
    dwd_order_refund_info )
        hive -e "$dwd_order_refund_info"
    ;;
    dwd_refund_payment )
        hive -e "$dwd_refund_payment"
        clear_data dwd_refund_payment
    ;;
    all )
        hive -e "$dwd_order_info$dwd_order_detail$dwd_payment_info$dwd_cart_info$dwd_comment_info$dwd_favor_info$dwd_coupon_use$dwd_order_refund_info$dwd_refund_payment"
        clear_data dwd_order_info
        clear_data dwd_payment_info
        clear_data dwd_coupon_use
        clear_data dwd_refund_payment
    ;;
esac
```

#### 5.4.3、DWD日志数据装载脚本

```shell
#!/bin/bash

APP=gmall
APP2=gmall_dwd
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

dwd_start_log="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP2}.dwd_start_log partition (dt='$do_date')
select get_json_object(line,'$.common.ar'),
       get_json_object(line,'$.common.ba'),
       get_json_object(line,'$.common.ch'),
       get_json_object(line,'$.common.is_new'),
       get_json_object(line,'$.common.md'),
       get_json_object(line,'$.common.mid'),
       get_json_object(line,'$.common.os'),
       get_json_object(line,'$.common.uid'),
       get_json_object(line,'$.common.vc'),
       get_json_object(line,'$.start.entry'),
       get_json_object(line,'$.start.loading_time'),
       get_json_object(line,'$.start.open_ad_id'),
       get_json_object(line,'$.start.open_ad_ms'),
       get_json_object(line,'$.start.open_ad_skip_ms'),
       get_json_object(line,'$.ts')
from ${APP}.ods_log
where dt = '$do_date'
and get_json_object(line,'$.start') is not null;
"

dwd_page_log="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP2}.dwd_page_log partition (dt='$do_date')
select get_json_object(line,'$.common.ar'),
       get_json_object(line,'$.common.ba'),
       get_json_object(line,'$.common.ch'),
       get_json_object(line,'$.common.is_new'),
       get_json_object(line,'$.common.md'),
       get_json_object(line,'$.common.mid'),
       get_json_object(line,'$.common.os'),
       get_json_object(line,'$.common.uid'),
       get_json_object(line,'$.common.vc'),
       get_json_object(line,'$.page.during_time'),
       get_json_object(line,'$.page.item'),
       get_json_object(line,'$.page.item_type'),
       get_json_object(line,'$.page.last_page_id'),
       get_json_object(line,'$.page.page_id'),
       get_json_object(line,'$.page.source_type'),
       get_json_object(line,'$.ts')
from ${APP}.ods_log
where dt = '$do_date'
and get_json_object(line,'$.page') is not null;
"

dwd_action_log="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP2}.dwd_action_log partition(dt='$do_date')
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.is_new'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.page.during_time'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.source_type'),
    get_json_object(action,'$.action_id'),
    get_json_object(action,'$.item'),
    get_json_object(action,'$.item_type'),
    get_json_object(action,'$.ts')
from ${APP}.ods_log lateral view ${APP2}.explode_json_array(get_json_object(line,'$.actions')) tmp as action
where dt='$do_date'
and get_json_object(line,'$.actions') is not null;
"

dwd_display_log="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP2}.dwd_displas_log partition(dt='$do_date')
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.is_new'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.page.during_time'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.source_type'),
    get_json_object(line,'$.display_type'),
    get_json_object(displays,'$.item'),
    get_json_object(displays,'$.item_type'),
    get_json_object(displays,'$.order'),
    get_json_object(displays,'$.pos_id'),
    get_json_object(line,'$.ts')
from ${APP}.ods_log lateral view ${APP2}.explode_json_array(get_json_object(line,'$.displays')) tmp as displays
where dt='$do_date'
and get_json_object(line,'$.displays') is not null;
"

dwd_error_log="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP2}.dwd_error_log partition(dt='$do_date')
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.is_new'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.source_type'),
    get_json_object(line,'$.start.entry'),
    get_json_object(line,'$.start.loading_time'),
    get_json_object(line,'$.start.open_ad_id'),
    get_json_object(line,'$.start.open_ad_ms'),
    get_json_object(line,'$.start.open_ad_skip_ms'),
    get_json_object(line,'$.actions'),
    get_json_object(line,'$.displays'),
    get_json_object(line,'$.ts'),
    get_json_object(line,'$.err.error_code'),
    get_json_object(line,'$.err.msg')
from ${APP}.ods_log
where dt='$do_date'
and get_json_object(line,'$.err') is not null;
"

case $1 in
    dwd_start_log )
        hive -e "$dwd_start_log"
    ;;
    dwd_page_log )
        hive -e "$dwd_page_log"
    ;;
    dwd_action_log )
        hive -e "$dwd_action_log"
    ;;
    dwd_display_log )
        hive -e "$dwd_display_log"
    ;;
    dwd_error_log )
        hive -e "$dwd_error_log"
    ;;
    all )
        hive -e "$dwd_start_log$dwd_page_log$dwd_action_log$dwd_display_log$dwd_error_log"
    ;;
esac
```

### 5.5、DWS脚本

#### **5.5.1、DWS层首日数据装载脚本**

```shell
#! /bin/bash

APP=gmall
APP1=gmall_dim
APP2=gmall_dwd
APP3=gmall_dws

if [ -n "$2" ] ;then
	do_date=$2
else
	echo "请输入日期参数"
	exit
fi

dws_visitor_action_daycount="
insert overwrite table ${APP3}.dws_visitor_action_daycount partition (dt = '$do_date')
select t1.mid_id,
       t1.brand,
       t1.model,
       t1.is_new,
       t1.channel,
       t1.os,
       t1.area_code,
       t1.version_code,
       t1.visit_count,
       t3.page_stats
from (
         select mid_id,
                brand,
                model,
                if(array_contains(collect_set(is_new), '0'), '0', '1') is_new, --ods_page_log中，同一天内，同一设备的is_new字段，可能全部为1，可能全部为0，也可能部分为0，部分为1(卸载重装),故做该处理
                collect_set(channel)                                   channel,
                collect_set(os)                                        os,
                collect_set(area_code)                                 area_code,
                collect_set(version_code)                              version_code,
                sum(if(last_page_id is null, 1, 0))                    visit_count
         from ${APP2}.dwd_page_log
         where dt = '$do_date'
           and last_page_id is null
         group by mid_id, model, brand
     ) t1
         join(select mid_id,
                     brand,
                     model,
                     collect_set(named_struct('page_id', page_id, 'page_count', page_count, 'during_time',
                                              during_time)) page_stats
              from (
                       select mid_id,
                              model,
                              brand,
                              page_id,
                              count(*)         page_count,
                              sum(during_time) during_time
                       from ${APP2}.dwd_page_log
                       where dt = '$do_date'
                       group by mid_id, model, brand, page_id
                   ) t2
              group by mid_id, model, brand
) t3
             on t1.mid_id = t3.mid_id
                 and t1.brand = t3.brand
                 and t1.model = t3.model;
"

dws_area_stats_daycount="
set hive.exec.dynamic.partition.mode=nonstrict;
with
tmp_vu as
(
    select
        dt,
        id province_id,
        visit_count,
        login_count,
        visitor_count,
        user_count
    from
    (
        select
            dt,
            area_code,
            count(*) visit_count,--访客访问次数
            count(user_id) login_count,--用户访问次数,等价于sum(if(user_id is not null,1,0))
            count(distinct(mid_id)) visitor_count,--访客人数
            count(distinct(user_id)) user_count--用户人数
        from ${APP2}.dwd_page_log
        where last_page_id is null
        group by dt,area_code
    )tmp
    left join ${APP1}.dim_base_province area
    on tmp.area_code=area.area_code
),
tmp_order as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        province_id,
        count(*) order_count,
        sum(original_amount) order_original_amount,
        sum(final_amount) order_final_amount
    from ${APP2}.dwd_order_info
    group by date_format(create_time,'yyyy-MM-dd'),province_id
),
tmp_pay as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        province_id,
        count(*) payment_count,
        sum(payment_amount) payment_amount
    from ${APP2}.dwd_payment_info
    group by date_format(callback_time,'yyyy-MM-dd'),province_id
),
tmp_ro as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        province_id,
        count(*) refund_order_count,
        sum(refund_amount) refund_order_amount
    from ${APP2}.dwd_order_refund_info
    group by date_format(create_time,'yyyy-MM-dd'),province_id
),
tmp_rp as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        province_id,
        count(*) refund_payment_count,
        sum(refund_amount) refund_payment_amount
    from ${APP2}.dwd_refund_payment
    group by date_format(callback_time,'yyyy-MM-dd'),province_id
)
insert overwrite table ${APP3}.dws_area_stats_daycount partition(dt)
select
    province_id,
    sum(visit_count),
    sum(login_count),
    sum(visitor_count),
    sum(user_count),
    sum(order_count),
    sum(order_original_amount),
    sum(order_final_amount),
    sum(payment_count),
    sum(payment_amount),
    sum(refund_order_count),
    sum(refund_order_amount),
    sum(refund_payment_count),
    sum(refund_payment_amount),
    dt
from
(
    select
        dt,
        province_id,
        visit_count,
        login_count,
        visitor_count,
        user_count,
        0 order_count,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_amount
    from tmp_vu
    union all
    select
        dt,
        province_id,
        0 visit_count,
        0 login_count,
        0 visitor_count,
        0 user_count,
        order_count,
        order_original_amount,
        order_final_amount,
        0 payment_count,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_amount
    from tmp_order
    union all
    select
        dt,
        province_id,
        0 visit_count,
        0 login_count,
        0 visitor_count,
        0 user_count,
        0 order_count,
        0 order_original_amount,
        0 order_final_amount,
        payment_count,
        payment_amount,
        0 refund_order_count,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_amount
    from tmp_pay
    union all
    select
        dt,
        province_id,
        0 visit_count,
        0 login_count,
        0 visitor_count,
        0 user_count,
        0 order_count,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_amount,
        refund_order_count,
        refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_amount
    from tmp_ro
    union all
    select
        dt,
        province_id,
        0 visit_count,
        0 login_count,
        0 visitor_count,
        0 user_count,
        0 order_count,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_amount,
        refund_payment_count,
        refund_payment_amount
    from tmp_rp
)t1
group by dt,province_id;
"

dws_user_action_daycount="
set hive.exec.dynamic.partition.mode=nonstrict;
with tmp_login as (
    select user_id,
           dt,
           count(*) login_count
    from ${APP2}.dwd_page_log
    where user_id is not null
      and last_page_id is null
    group by user_id, dt),
     tmp_cf as (
         select user_id,
                dt,
                sum(if(action_id = 'cart_add', 1, 0))  cart_count,
                sum(if(action_id = 'favor_add', 1, 0)) favor_count
         from ${APP2}.dwd_action_log
         where user_id is not null
           and action_id in ('cart_add', 'favor_add')
         group by user_id, dt),
     tmp_order as (
         select user_id,
                date_format(create_time, 'yyyy-MM-dd')    dt,
                count(*)                                  order_count,
                sum(if(activity_reduce_amount > 0, 1, 0)) order_activity_count,
                sum(activity_reduce_amount)               order_activity_reduce_amount,
                sum(if(coupon_reduce_amount > 0, 1, 0))   order_coupon_count,
                sum(coupon_reduce_amount)                 order_coupon_reduce_amount,
                sum(original_amount)                      order_original_amount,
                sum(final_amount)                         order_final_amount
         from ${APP2}.dwd_order_info
         group by user_id, date_format(create_time, 'yyyy-MM-dd')
     ),
     tmp_pay as (
         select user_id,
                dt,
                count(*)            payment_count,
                sum(payment_amount) payment_amount
         from ${APP2}.dwd_payment_info
         where dt != '9999-99-99'
         group by user_id, dt
     ),
     tmp_ri as (
         select user_id,
                dt,
                count(*)           refund_order_count,
                sum(refund_num)    refund_order_num,
                sum(refund_amount) refund_order_amount
         from ${APP2}.dwd_order_refund_info
         group by user_id, dt
     ),
     tmp_rp as (select user_id,
                       dt,
                       count(*)           refund_payment_count,
                       sum(refund_num)    refund_payment_num,
                       sum(refund_amount) refund_payment_amount
                from (
                         select user_id,
                                dt,
                                refund_amount,
                                order_id,
                                sku_id
                         from ${APP2}.dwd_refund_payment
                         where dt != '9999-99-99'
                     ) rp
                         left join
                     (
                         select refund_num,
                                order_id,
                                sku_id
                         from ${APP2}.dwd_order_refund_info
                         where dt != '9999-99-99') ri
                     on rp.order_id = ri.order_id
                         and rp.sku_id = ri.sku_id
                where dt != '9999-99-99'
                group by user_id, dt),
     tmp_coupon as (
         select coalesce(coupon_get.dt, coupon_using.dt, coupon_used.dt)                dt,
                coalesce(coupon_get.user_id, coupon_using.user_id, coupon_used.user_id) user_id,
                nvl(get_count, 0)                                                       coupon_get_count,
                nvl(using_count, 0)                                                     coupon_using_count,
                nvl(used_count, 0)                                                      coupon_used_count
         from (select user_id,
                      date_format(get_time, 'yyyy-MM-dd') dt,
                      count(*)                            get_count
               from ${APP2}.dwd_coupon_use
               where get_time is not null
               group by user_id, date_format(get_time, 'yyyy-MM-dd')
              ) coupon_get
                  full outer join
              (select user_id,
                      date_format(using_time, 'yyyy-MM-dd') dt,
                      count(*)                              using_count
               from ${APP2}.dwd_coupon_use
               where using_time is not null
               group by user_id, date_format(using_time, 'yyyy-MM-dd')
              ) coupon_using
              on coupon_get.dt = coupon_using.dt
                  and coupon_get.user_id = coupon_using.user_id
                  full outer join
              (select user_id,
                      date_format(used_time, 'yyyy-MM-dd') dt,
                      count(*)                             used_count
               from ${APP2}.dwd_coupon_use
               where used_time is not null
               group by user_id, date_format(used_time, 'yyyy-MM-dd')
              ) coupon_used
              on nvl(coupon_get.dt, coupon_using.dt) = coupon_used.dt
                  and nvl(coupon_get.user_id, coupon_using.user_id) = coupon_used.user_id
     ),
     tmp_comment as (
         select user_id,
                dt,
                sum(if(appraise = '1201', 1, 0)) appraise_good_count,
                sum(if(appraise = '1202', 1, 0)) appraise_mid_count,
                sum(if(appraise = '1203', 1, 0)) appraise_bad_count,
                sum(if(appraise = '1204', 1, 0)) appraise_default_count
         from ${APP2}.dwd_comment_info
         group by user_id, dt
     ),
     tmp_od as (
         select user_id,
                dt,
                collect_set(named_struct('sku_id', sku_id, 'sku_num', sku_num, 'order_count', order_count,
                                         'activity_reduce_amount', cast(activity_reduce_amount as decimal(16, 2)),
                                         'coupon_reduce_amount',
                                         cast(coupon_reduce_amount as decimal(16, 2)), 'original_amount',
                                         cast(original_amount as decimal(16, 2)), 'final_amount',
                                         cast(final_amount as decimal(16, 2)))) order_detail_stats
         from (
                  select user_id,
                         dt,
                         sku_id,
                         sum(sku_num)               sku_num,
                         count(*)                   order_count,
                         sum(split_activity_amount) activity_reduce_amount,
                         sum(split_coupon_amount)   coupon_reduce_amount,
                         sum(original_amount)       original_amount,
                         sum(split_final_amount)    final_amount
                  from ${APP2}.dwd_order_detail
                  group by user_id, dt, sku_id
              ) t1
         group by user_id, dt
     )
insert
overwrite
table
${APP3}.dws_user_action_daycount
partition
(
dt
)
select coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id, tmp_ri.user_id, tmp_rp.user_id,
                tmp_comment.user_id, tmp_coupon.user_id, tmp_od.user_id),
       nvl(login_count, 0),
       nvl(cart_count, 0),
       nvl(favor_count, 0),
       nvl(order_count, 0),
       nvl(order_activity_count, 0),
       nvl(order_activity_reduce_amount, 0),
       nvl(order_coupon_count, 0),
       nvl(order_coupon_reduce_amount, 0),
       nvl(order_original_amount, 0),
       nvl(order_final_amount, 0),
       nvl(payment_count, 0),
       nvl(payment_amount, 0),
       nvl(refund_order_count, 0),
       nvl(refund_order_num, 0),
       nvl(refund_order_amount, 0),
       nvl(refund_payment_count, 0),
       nvl(refund_payment_num, 0),
       nvl(refund_payment_amount, 0),
       nvl(coupon_get_count, 0),
       nvl(coupon_using_count, 0),
       nvl(coupon_used_count, 0),
       nvl(appraise_good_count, 0),
       nvl(appraise_mid_count, 0),
       nvl(appraise_bad_count, 0),
       nvl(appraise_default_count, 0),
       order_detail_stats,
       coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt, tmp_rp.dt, tmp_comment.dt, tmp_coupon.dt,
                tmp_od.dt)
from tmp_login
         full outer join tmp_cf
                         on tmp_login.user_id = tmp_cf.user_id
                             and tmp_login.dt = tmp_cf.dt
         full outer join tmp_order
                         on coalesce(tmp_login.user_id, tmp_cf.user_id) = tmp_order.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt) = tmp_order.dt
         full outer join tmp_pay
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id) = tmp_pay.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt) = tmp_pay.dt
         full outer join tmp_ri
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id) =
                            tmp_ri.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt) = tmp_ri.dt
         full outer join tmp_rp
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                     tmp_ri.user_id) = tmp_rp.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt) = tmp_rp.dt
         full outer join tmp_comment
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                     tmp_ri.user_id, tmp_rp.user_id) = tmp_comment.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt, tmp_rp.dt) =
                                 tmp_comment.dt
         full outer join tmp_coupon
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                     tmp_ri.user_id, tmp_rp.user_id, tmp_comment.user_id) = tmp_coupon.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt, tmp_rp.dt,
                                          tmp_comment.dt) = tmp_coupon.dt
         full outer join tmp_od
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                     tmp_ri.user_id, tmp_rp.user_id, tmp_comment.user_id, tmp_coupon.user_id) =
                            tmp_od.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt, tmp_rp.dt,
                                          tmp_comment.dt, tmp_coupon.dt) = tmp_od.dt;
"

dws_activity_info_daycount="
set hive.exec.dynamic.partition.mode=nonstrict;
with
tmp_order as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        activity_rule,
        activity_id,
        count(*) order_count,
        sum(split_activity_amount) order_reduce_amount,
        sum(original_amount) order_original_amount,
        sum(split_final_amount) order_final_amount
    from ${APP2}.dwd_order_detail
    where activity_id is not null
    group by date_format(create_time,'yyyy-MM-dd'),activity_rule,activity_id
),
tmp_pay as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        activity_rule,
        activity_id,
        count(*) payment_count,
        sum(split_activity_amount) payment_reduce_amount,
        sum(split_final_amount) payment_amount
    from
    (
        select
            activity_rule,
            activity_id,
            order_id,
            split_activity_amount,
            split_final_amount
        from ${APP2}.dwd_order_detail
        where activity_id is not null
    )od
    join
    (
        select
            order_id,
            callback_time
        from ${APP2}.dwd_payment_info
    )pi
    on od.order_id=pi.order_id
    group by date_format(callback_time,'yyyy-MM-dd'),activity_rule,activity_id
)
insert overwrite table ${APP3}.dws_activity_info_daycount partition(dt)
select
    activity_rule,
    activity_id,
    sum(order_count),
    sum(order_reduce_amount),
    sum(order_original_amount),
    sum(order_final_amount),
    sum(payment_count),
    sum(payment_reduce_amount),
    sum(payment_amount),
    dt
from
(
    select
        dt,
        activity_rule,
        activity_id,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        0 payment_count,
        0 payment_reduce_amount,
        0 payment_amount
    from tmp_order
    union all
    select
        dt,
        activity_rule,
        activity_id,
        0 order_count,
        0 order_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount
    from tmp_pay
)t1
group by dt,activity_rule,activity_id;
"

dws_sku_action_daycount="
set hive.exec.dynamic.partition.mode=nonstrict;
with
tmp_order as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        sku_id,
        count(*) order_count,
        sum(sku_num) order_num,
        sum(if(split_activity_amount>0,1,0)) order_activity_count,
        sum(if(split_coupon_amount>0,1,0)) order_coupon_count,
        sum(split_activity_amount) order_activity_reduce_amount,
        sum(split_coupon_amount) order_coupon_reduce_amount,
        sum(original_amount) order_original_amount,
        sum(split_final_amount) order_final_amount
    from ${APP2}.dwd_order_detail
    group by date_format(create_time,'yyyy-MM-dd'),sku_id
),
tmp_pay as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        sku_id,
        count(*) payment_count,
        sum(sku_num) payment_num,
        sum(split_final_amount) payment_amount
    from ${APP2}.dwd_order_detail od
    join
    (
        select
            order_id,
            callback_time
        from ${APP2}.dwd_payment_info
        where callback_time is not null
    )pi on pi.order_id=od.order_id
    group by date_format(callback_time,'yyyy-MM-dd'),sku_id
),
tmp_ri as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        sku_id,
        count(*) refund_order_count,
        sum(refund_num) refund_order_num,
        sum(refund_amount) refund_order_amount
    from ${APP2}.dwd_order_refund_info
    group by date_format(create_time,'yyyy-MM-dd'),sku_id
),
tmp_rp as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        rp.sku_id,
        count(*) refund_payment_count,
        sum(ri.refund_num) refund_payment_num,
        sum(refund_amount) refund_payment_amount
    from
    (
        select
            order_id,
            sku_id,
            refund_amount,
            callback_time
        from ${APP2}.dwd_refund_payment
    )rp
    left join
    (
        select
            order_id,
            sku_id,
            refund_num
        from ${APP2}.dwd_order_refund_info
    )ri
    on rp.order_id=ri.order_id
    and rp.sku_id=ri.sku_id
    group by date_format(callback_time,'yyyy-MM-dd'),rp.sku_id
),
tmp_cf as
(
    select
        dt,
        item sku_id,
        sum(if(action_id='cart_add',1,0)) cart_count,
        sum(if(action_id='favor_add',1,0)) favor_count
    from ${APP2}.dwd_action_log
    where action_id in ('cart_add','favor_add')
    group by dt,item
),
tmp_comment as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        sku_id,
        sum(if(appraise='1201',1,0)) appraise_good_count,
        sum(if(appraise='1202',1,0)) appraise_mid_count,
        sum(if(appraise='1203',1,0)) appraise_bad_count,
        sum(if(appraise='1204',1,0)) appraise_default_count
    from ${APP2}.dwd_comment_info
    group by date_format(create_time,'yyyy-MM-dd'),sku_id
)
insert overwrite table ${APP3}.dws_sku_action_daycount partition(dt)
select
    sku_id,
    sum(order_count),
    sum(order_num),
    sum(order_activity_count),
    sum(order_coupon_count),
    sum(order_activity_reduce_amount),
    sum(order_coupon_reduce_amount),
    sum(order_original_amount),
    sum(order_final_amount),
    sum(payment_count),
    sum(payment_num),
    sum(payment_amount),
    sum(refund_order_count),
    sum(refund_order_num),
    sum(refund_order_amount),
    sum(refund_payment_count),
    sum(refund_payment_num),
    sum(refund_payment_amount),
    sum(cart_count),
    sum(favor_count),
    sum(appraise_good_count),
    sum(appraise_mid_count),
    sum(appraise_bad_count),
    sum(appraise_default_count),
    dt
from
(
    select
        dt,
        sku_id,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_order
    union all
    select
        dt,
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        payment_count,
        payment_num,
        payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_pay
    union all
    select
        dt,
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_ri
    union all
    select
        dt,
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_rp
    union all
    select
        dt,
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        cart_count,
        favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_cf
    union all
    select
        dt,
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        0 cart_count,
        0 favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from tmp_comment
)t1
group by dt,sku_id;
"

dws_coupon_info_daycount="
set hive.exec.dynamic.partition.mode=nonstrict;
with
tmp_cu as
(
    select
        coalesce(coupon_get.dt,coupon_using.dt,coupon_used.dt,coupon_exprie.dt) dt,
        coalesce(coupon_get.coupon_id,coupon_using.coupon_id,coupon_used.coupon_id,coupon_exprie.coupon_id) coupon_id,
        nvl(get_count,0) get_count,
        nvl(order_count,0) order_count,
        nvl(payment_count,0) payment_count,
        nvl(expire_count,0) expire_count
    from
    (
        select
            date_format(get_time,'yyyy-MM-dd') dt,
            coupon_id,
            count(*) get_count
        from ${APP2}.dwd_coupon_use
        group by date_format(get_time,'yyyy-MM-dd'),coupon_id
    )coupon_get
    full outer join
    (
        select
            date_format(using_time,'yyyy-MM-dd') dt,
            coupon_id,
            count(*) order_count
        from ${APP2}.dwd_coupon_use
        where using_time is not null
        group by date_format(using_time,'yyyy-MM-dd'),coupon_id
    )coupon_using
    on coupon_get.dt=coupon_using.dt
    and coupon_get.coupon_id=coupon_using.coupon_id
    full outer join
    (
        select
            date_format(used_time,'yyyy-MM-dd') dt,
            coupon_id,
            count(*) payment_count
        from ${APP2}.dwd_coupon_use
        where used_time is not null
        group by date_format(used_time,'yyyy-MM-dd'),coupon_id
    )coupon_used
    on nvl(coupon_get.dt,coupon_using.dt)=coupon_used.dt
    and nvl(coupon_get.coupon_id,coupon_using.coupon_id)=coupon_used.coupon_id
    full outer join
    (
        select
            date_format(expire_time,'yyyy-MM-dd') dt,
            coupon_id,
            count(*) expire_count
        from ${APP2}.dwd_coupon_use
        where expire_time is not null
        group by date_format(expire_time,'yyyy-MM-dd'),coupon_id
    )coupon_exprie
    on coalesce(coupon_get.dt,coupon_using.dt,coupon_used.dt)=coupon_exprie.dt
    and coalesce(coupon_get.coupon_id,coupon_using.coupon_id,coupon_used.coupon_id)=coupon_exprie.coupon_id
),
tmp_order as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        coupon_id,
        sum(split_coupon_amount) order_reduce_amount,
        sum(original_amount) order_original_amount,
        sum(split_final_amount) order_final_amount
    from ${APP2}.dwd_order_detail
    where coupon_id is not null
    group by date_format(create_time,'yyyy-MM-dd'),coupon_id
),
tmp_pay as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        coupon_id,
        sum(split_coupon_amount) payment_reduce_amount,
        sum(split_final_amount) payment_amount
    from
    (
        select
            order_id,
            coupon_id,
            split_coupon_amount,
            split_final_amount
        from ${APP2}.dwd_order_detail
        where coupon_id is not null
    )od
    join
    (
        select
            order_id,
            callback_time
        from ${APP2}.dwd_payment_info
    )pi
    on od.order_id=pi.order_id
    group by date_format(callback_time,'yyyy-MM-dd'),coupon_id
)
insert overwrite table ${APP3}.dws_coupon_info_daycount partition(dt)
select
    coupon_id,
    sum(get_count),
    sum(order_count),
    sum(order_reduce_amount),
    sum(order_original_amount),
    sum(order_final_amount),
    sum(payment_count),
    sum(payment_reduce_amount),
    sum(payment_amount),
    sum(expire_count),
    dt
from
(
    select
        dt,
        coupon_id,
        get_count,
        order_count,
        0 order_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        payment_count,
        0 payment_reduce_amount,
        0 payment_amount,
        expire_count
    from tmp_cu
    union all
    select
        dt,
        coupon_id,
        0 get_count,
        0 order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        0 payment_count,
        0 payment_reduce_amount,
        0 payment_amount,
        0 expire_count
    from tmp_order
    union all
    select
        dt,
        coupon_id,
        0 get_count,
        0 order_count,
        0 order_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        payment_reduce_amount,
        payment_amount,
        0 expire_count
    from tmp_pay
)t1
group by dt,coupon_id;
"

case $1 in
    "dws_visitor_action_daycount" )
        hive -e "$dws_visitor_action_daycount"
    ;;
    "dws_user_action_daycount" )
        hive -e "$dws_user_action_daycount"
    ;;
    "dws_activity_info_daycount" )
        hive -e "$dws_activity_info_daycount"
    ;;
    "dws_area_stats_daycount" )
        hive -e "$dws_area_stats_daycount"
    ;;
    "dws_sku_action_daycount" )
        hive -e "$dws_sku_action_daycount"
    ;;
    "dws_coupon_info_daycount" )
        hive -e "$dws_coupon_info_daycount"
    ;;
    "all" )
        hive -e "$dws_visitor_action_daycount$dws_user_action_daycount$dws_activity_info_daycount$dws_area_stats_daycount$dws_sku_action_daycount$dws_coupon_info_daycount"
    ;;
esac
```

#### 5.5.2、DWS层每日数据转成脚本

```shell
#! /bin/bash

APP=gmall
APP1=gmall_dim
APP2=gmall_dwd
APP3=gmall_dws

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi


dws_visitor_action_daycount="
insert overwrite table ${APP3}.dws_visitor_action_daycount partition (dt = '$do_date')
select t1.mid_id,
       t1.brand,
       t1.model,
       t1.is_new,
       t1.channel,
       t1.os,
       t1.area_code,
       t1.version_code,
       t1.visit_count,
       t3.page_stats
from (
         select mid_id,
                brand,
                model,
                if(array_contains(collect_set(is_new), '0'), '0', '1') is_new,--ods_page_log中，同一天内，同一设备的is_new字段，可能全部为1，可能全部为0，也可能部分为0，部分为1(卸载重装),故做该处理
                collect_set(channel)                                   channel,
                collect_set(os)                                        os,
                collect_set(area_code)                                 area_code,
                collect_set(version_code)                              version_code,
                sum(if(last_page_id is null, 1, 0))                    visit_count
         from ${APP2}.dwd_page_log
         where dt = '$do_date'
           and last_page_id is null
         group by mid_id, model, brand
     ) t1
         join(select mid_id,
                     brand,
                     model,
                     collect_set(named_struct('page_id', page_id, 'page_count', page_count, 'during_time',
                                              during_time)) page_stats
              from (
                       select mid_id,
                              model,
                              brand,
                              page_id,
                              count(*)         page_count,
                              sum(during_time) during_time
                       from ${APP2}.dwd_page_log
                       where dt = '$do_date'
                       group by mid_id, model, brand, page_id
                   ) t2
              group by mid_id, model, brand
) t3
             on t1.mid_id = t3.mid_id
                 and t1.brand = t3.brand
                 and t1.model = t3.model;
"

dws_user_action_daycount="
with tmp_login as
         (
             select user_id,
                    count(*) login_count
             from ${APP2}.dwd_page_log
             where dt = '$do_date'
               and user_id is not null
               and last_page_id is null
             group by user_id
         ),
     tmp_cf as
         (
             select user_id,
                    sum(if(action_id = 'cart_add', 1, 0))  cart_count,
                    sum(if(action_id = 'favor_add', 1, 0)) favor_count
             from ${APP2}.dwd_action_log
             where dt = '$do_date'
               and user_id is not null
               and action_id in ('cart_add', 'favor_add')
             group by user_id
         ),
     tmp_order as
         (
             select user_id,
                    count(*)                                  order_count,
                    sum(if(activity_reduce_amount > 0, 1, 0)) order_activity_count,
                    sum(if(coupon_reduce_amount > 0, 1, 0))   order_coupon_count,
                    sum(activity_reduce_amount)               order_activity_reduce_amount,
                    sum(coupon_reduce_amount)                 order_coupon_reduce_amount,
                    sum(original_amount)                      order_original_amount,
                    sum(final_amount)                         order_final_amount
             from ${APP2}.dwd_order_info
             where (dt = '$do_date'
                 or dt = '9999-99-99')
               and date_format(create_time, 'yyyy-MM-dd') = '$do_date'
             group by user_id
         ),
     tmp_pay as
         (
             select user_id,
                    count(*)            payment_count,
                    sum(payment_amount) payment_amount
             from ${APP2}.dwd_payment_info
             where dt = '$do_date'
             group by user_id
         ),
     tmp_ri as
         (
             select user_id,
                    count(*)           refund_order_count,
                    sum(refund_num)    refund_order_num,
                    sum(refund_amount) refund_order_amount
             from ${APP2}.dwd_order_refund_info
             where dt = '$do_date'
             group by user_id
         ),
     tmp_rp as
         (
             select rp.user_id,
                    count(*)              refund_payment_count,
                    sum(ri.refund_num)    refund_payment_num,
                    sum(rp.refund_amount) refund_payment_amount
             from (
                      select user_id,
                             order_id,
                             sku_id,
                             refund_amount
                      from ${APP2}.dwd_refund_payment
                      where dt = '$do_date'
                  ) rp
                      left join
                  (
                      select user_id,
                             order_id,
                             sku_id,
                             refund_num
                      from ${APP2}.dwd_order_refund_info
                      where dt >= date_add('$do_date', -15)
                  ) ri
                  on rp.order_id = ri.order_id
                      and rp.sku_id = rp.sku_id
             group by rp.user_id
         ),
     tmp_coupon as
         (
             select user_id,
                    sum(if(date_format(get_time, 'yyyy-MM-dd') = '$do_date', 1, 0))   coupon_get_count,
                    sum(if(date_format(using_time, 'yyyy-MM-dd') = '$do_date', 1, 0)) coupon_using_count,
                    sum(if(date_format(used_time, 'yyyy-MM-dd') = '$do_date', 1, 0))  coupon_used_count
             from ${APP2}.dwd_coupon_use
             where (dt = '$do_date' or dt = '9999-99-99')
               and (date_format(get_time, 'yyyy-MM-dd') = '$do_date'
                 or date_format(using_time, 'yyyy-MM-dd') = '$do_date'
                 or date_format(used_time, 'yyyy-MM-dd') = '$do_date')
             group by user_id
         ),
     tmp_comment as
         (
             select user_id,
                    sum(if(appraise = '1201', 1, 0)) appraise_good_count,
                    sum(if(appraise = '1202', 1, 0)) appraise_mid_count,
                    sum(if(appraise = '1203', 1, 0)) appraise_bad_count,
                    sum(if(appraise = '1204', 1, 0)) appraise_default_count
             from ${APP2}.dwd_comment_info
             where dt = '$do_date'
             group by user_id
         ),
     tmp_od as
         (
             select user_id,
                    collect_set(named_struct('sku_id', sku_id, 'sku_num', sku_num, 'order_count', order_count,
                                             'activity_reduce_amount', activity_reduce_amount, 'coupon_reduce_amount',
                                             coupon_reduce_amount, 'original_amount', original_amount, 'final_amount',
                                             final_amount)) order_detail_stats
             from (
                      select user_id,
                             sku_id,
                             sum(sku_num)                                       sku_num,
                             count(*)                                           order_count,
                             cast(sum(split_activity_amount) as decimal(16, 2)) activity_reduce_amount,
                             cast(sum(split_coupon_amount) as decimal(16, 2))   coupon_reduce_amount,
                             cast(sum(original_amount) as decimal(16, 2))       original_amount,
                             cast(sum(split_final_amount) as decimal(16, 2))    final_amount
                      from ${APP2}.dwd_order_detail
                      where dt = '$do_date'
                      group by user_id, sku_id
                  ) t1
             group by user_id
         )
insert
overwrite
table
${APP3}.dws_user_action_daycount
partition
(
dt = '$do_date'
)
select coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id, tmp_ri.user_id, tmp_rp.user_id,
                tmp_comment.user_id, tmp_coupon.user_id, tmp_od.user_id),
       nvl(login_count, 0),
       nvl(cart_count, 0),
       nvl(favor_count, 0),
       nvl(order_count, 0),
       nvl(order_activity_count, 0),
       nvl(order_activity_reduce_amount, 0),
       nvl(order_coupon_count, 0),
       nvl(order_coupon_reduce_amount, 0),
       nvl(order_original_amount, 0),
       nvl(order_final_amount, 0),
       nvl(payment_count, 0),
       nvl(payment_amount, 0),
       nvl(refund_order_count, 0),
       nvl(refund_order_num, 0),
       nvl(refund_order_amount, 0),
       nvl(refund_payment_count, 0),
       nvl(refund_payment_num, 0),
       nvl(refund_payment_amount, 0),
       nvl(coupon_get_count, 0),
       nvl(coupon_using_count, 0),
       nvl(coupon_used_count, 0),
       nvl(appraise_good_count, 0),
       nvl(appraise_mid_count, 0),
       nvl(appraise_bad_count, 0),
       nvl(appraise_default_count, 0),
       order_detail_stats
from tmp_login
         full outer join tmp_cf on tmp_login.user_id = tmp_cf.user_id
         full outer join tmp_order on coalesce(tmp_login.user_id, tmp_cf.user_id) = tmp_order.user_id
         full outer join tmp_pay on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id) = tmp_pay.user_id
         full outer join tmp_ri on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id) =
                                   tmp_ri.user_id
         full outer join tmp_rp on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                            tmp_ri.user_id) = tmp_rp.user_id
         full outer join tmp_comment on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                                 tmp_ri.user_id, tmp_rp.user_id) = tmp_comment.user_id
         full outer join tmp_coupon on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                                tmp_ri.user_id, tmp_rp.user_id, tmp_comment.user_id) =
                                       tmp_coupon.user_id
         full outer join tmp_od on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                            tmp_ri.user_id, tmp_rp.user_id, tmp_comment.user_id, tmp_coupon.user_id) =
                                   tmp_od.user_id;
"

dws_activity_info_daycount="
with
tmp_order as
(
    select
        activity_rule,
        activity_id,
        count(*) order_count,
        sum(split_activity_amount) order_reduce_amount,
        sum(original_amount) order_original_amount,
        sum(split_final_amount) order_final_amount
    from ${APP2}.dwd_order_detail
    where dt='$do_date'
    and activity_id is not null
    group by activity_rule,activity_id
),
tmp_pay as
(
    select
        activity_rule,
        activity_id,
        count(*) payment_count,
        sum(split_activity_amount) payment_reduce_amount,
        sum(split_final_amount) payment_amount
    from ${APP2}.dwd_order_detail
    where (dt='$do_date'
    or dt=date_add('$do_date',-1))
    and activity_id is not null
    and order_id in
    (
        select order_id from ${APP2}.dwd_payment_info where dt='$do_date'
    )
    group by activity_rule,activity_id
)
insert overwrite table ${APP3}.dws_activity_info_daycount partition(dt='$do_date')
select
    activity_rule,
    activity_id,
    sum(order_count),
    sum(order_reduce_amount),
    sum(order_original_amount),
    sum(order_final_amount),
    sum(payment_count),
    sum(payment_reduce_amount),
    sum(payment_amount)
from
(
    select
        activity_rule,
        activity_id,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        0 payment_count,
        0 payment_reduce_amount,
        0 payment_amount
    from tmp_order
    union all
    select
        activity_rule,
        activity_id,
        0 order_count,
        0 order_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount
    from tmp_pay
)t1
group by activity_rule,activity_id;
"

dws_sku_action_daycount="
with
tmp_order as
(
    select
        sku_id,
        count(*) order_count,
        sum(sku_num) order_num,
        sum(if(split_activity_amount>0,1,0)) order_activity_count,
        sum(if(split_coupon_amount>0,1,0)) order_coupon_count,
        sum(split_activity_amount) order_activity_reduce_amount,
        sum(split_coupon_amount) order_coupon_reduce_amount,
        sum(original_amount) order_original_amount,
        sum(split_final_amount) order_final_amount
    from ${APP2}.dwd_order_detail
    where dt='$do_date'
    group by sku_id
),
tmp_pay as
(
    select
        sku_id,
        count(*) payment_count,
        sum(sku_num) payment_num,
        sum(split_final_amount) payment_amount
    from ${APP2}.dwd_order_detail
    where (dt='$do_date'
    or dt=date_add('$do_date',-1))
    and order_id in
    (
        select order_id from ${APP2}.dwd_payment_info where dt='$do_date'
    )
    group by sku_id
),
tmp_ri as
(
    select
        sku_id,
        count(*) refund_order_count,
        sum(refund_num) refund_order_num,
        sum(refund_amount) refund_order_amount
    from ${APP2}.dwd_order_refund_info
    where dt='$do_date'
    group by sku_id
),
tmp_rp as
(
    select
        rp.sku_id,
        count(*) refund_payment_count,
        sum(ri.refund_num) refund_payment_num,
        sum(refund_amount) refund_payment_amount
    from
    (
        select
            order_id,
            sku_id,
            refund_amount
        from ${APP2}.dwd_refund_payment
        where dt='$do_date'
    )rp
    left join
    (
        select
            order_id,
            sku_id,
            refund_num
        from ${APP2}.dwd_order_refund_info
        where dt>=date_add('$do_date',-15)
    )ri
    on rp.order_id=ri.order_id
    and rp.sku_id=ri.sku_id
    group by rp.sku_id
),
tmp_cf as
(
    select
        item sku_id,
        sum(if(action_id='cart_add',1,0)) cart_count,
        sum(if(action_id='favor_add',1,0)) favor_count
    from ${APP2}.dwd_action_log
    where dt='$do_date'
    and action_id in ('cart_add','favor_add')
    group by item
),
tmp_comment as
(
    select
        sku_id,
        sum(if(appraise='1201',1,0)) appraise_good_count,
        sum(if(appraise='1202',1,0)) appraise_mid_count,
        sum(if(appraise='1203',1,0)) appraise_bad_count,
        sum(if(appraise='1204',1,0)) appraise_default_count
    from ${APP2}.dwd_comment_info
    where dt='$do_date'
    group by sku_id
)
insert overwrite table ${APP3}.dws_sku_action_daycount partition(dt='$do_date')
select
    sku_id,
    sum(order_count),
    sum(order_num),
    sum(order_activity_count),
    sum(order_coupon_count),
    sum(order_activity_reduce_amount),
    sum(order_coupon_reduce_amount),
    sum(order_original_amount),
    sum(order_final_amount),
    sum(payment_count),
    sum(payment_num),
    sum(payment_amount),
    sum(refund_order_count),
    sum(refund_order_num),
    sum(refund_order_amount),
    sum(refund_payment_count),
    sum(refund_payment_num),
    sum(refund_payment_amount),
    sum(cart_count),
    sum(favor_count),
    sum(appraise_good_count),
    sum(appraise_mid_count),
    sum(appraise_bad_count),
    sum(appraise_default_count)
from
(
    select
        sku_id,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_order
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        payment_count,
        payment_num,
        payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_pay
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_ri
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_rp
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        cart_count,
        favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_cf
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        0 cart_count,
        0 favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from tmp_comment
)t1
group by sku_id;
"

dws_coupon_info_daycount="
with
tmp_cu as
(
    select
        coupon_id,
        sum(if(date_format(get_time,'yyyy-MM-dd')='$do_date',1,0)) get_count,
        sum(if(date_format(using_time,'yyyy-MM-dd')='$do_date',1,0)) order_count,
        sum(if(date_format(used_time,'yyyy-MM-dd')='$do_date',1,0)) payment_count,
        sum(if(date_format(expire_time,'yyyy-MM-dd')='$do_date',1,0)) expire_count
    from ${APP2}.dwd_coupon_use
    where dt='9999-99-99'
    or dt='$do_date'
    group by coupon_id
),
tmp_order as
(
    select
        coupon_id,
        sum(split_coupon_amount) order_reduce_amount,
        sum(original_amount) order_original_amount,
        sum(split_final_amount) order_final_amount
    from ${APP2}.dwd_order_detail
    where dt='$do_date'
    and coupon_id is not null
    group by coupon_id
),
tmp_pay as
(
    select
        coupon_id,
        sum(split_coupon_amount) payment_reduce_amount,
        sum(split_final_amount) payment_amount
    from ${APP2}.dwd_order_detail
    where (dt='$do_date'
    or dt=date_add('$do_date',-1))
    and coupon_id is not null
    and order_id in
    (
        select order_id from ${APP2}.dwd_payment_info where dt='$do_date'
    )
    group by coupon_id
)
insert overwrite table ${APP3}.dws_coupon_info_daycount partition(dt='$do_date')
select
    coupon_id,
    sum(get_count),
    sum(order_count),
    sum(order_reduce_amount),
    sum(order_original_amount),
    sum(order_final_amount),
    sum(payment_count),
    sum(payment_reduce_amount),
    sum(payment_amount),
    sum(expire_count)
from
(
    select
        coupon_id,
        get_count,
        order_count,
        0 order_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        payment_count,
        0 payment_reduce_amount,
        0 payment_amount,
        expire_count
    from tmp_cu
    union all
    select
        coupon_id,
        0 get_count,
        0 order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        0 payment_count,
        0 payment_reduce_amount,
        0 payment_amount,
        0 expire_count
    from tmp_order
    union all
    select
        coupon_id,
        0 get_count,
        0 order_count,
        0 order_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        payment_reduce_amount,
        payment_amount,
        0 expire_count
    from tmp_pay
)t1
group by coupon_id;
"

dws_area_stats_daycount="
with
tmp_vu as
(
    select
        id province_id,
        visit_count,
        login_count,
        visitor_count,
        user_count
    from
    (
        select
            area_code,
            count(*) visit_count,--访客访问次数
            count(user_id) login_count,--用户访问次数,等价于sum(if(user_id is not null,1,0))
            count(distinct(mid_id)) visitor_count,--访客人数
            count(distinct(user_id)) user_count--用户人数
        from ${APP2}.dwd_page_log
        where dt='$do_date'
        and last_page_id is null
        group by area_code
    )tmp
    left join ${APP1}.dim_base_province area
    on tmp.area_code=area.area_code
),
tmp_order as
(
    select
        province_id,
        count(*) order_count,
        sum(original_amount) order_original_amount,
        sum(final_amount) order_final_amount
    from ${APP2}.dwd_order_info
    where dt='$do_date'
    or dt='9999-99-99'
    and date_format(create_time,'yyyy-MM-dd')='$do_date'
    group by province_id
),
tmp_pay as
(
    select
        province_id,
        count(*) payment_count,
        sum(payment_amount) payment_amount
    from ${APP2}.dwd_payment_info
    where dt='$do_date'
    group by province_id
),
tmp_ro as
(
    select
        province_id,
        count(*) refund_order_count,
        sum(refund_amount) refund_order_amount
    from ${APP2}.dwd_order_refund_info
    where dt='$do_date'
    group by province_id
),
tmp_rp as
(
    select
        province_id,
        count(*) refund_payment_count,
        sum(refund_amount) refund_payment_amount
    from ${APP2}.dwd_refund_payment
    where dt='$do_date'
    group by province_id
)
insert overwrite table ${APP3}.dws_area_stats_daycount partition(dt='$do_date')
select
    province_id,
    sum(visit_count),
    sum(login_count),
    sum(visitor_count),
    sum(user_count),
    sum(order_count),
    sum(order_original_amount),
    sum(order_final_amount),
    sum(payment_count),
    sum(payment_amount),
    sum(refund_order_count),
    sum(refund_order_amount),
    sum(refund_payment_count),
    sum(refund_payment_amount)
from
(
    select
        province_id,
        visit_count,
        login_count,
        visitor_count,
        user_count,
        0 order_count,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_amount
    from tmp_vu
    union all
    select
        province_id,
        0 visit_count,
        0 login_count,
        0 visitor_count,
        0 user_count,
        order_count,
        order_original_amount,
        order_final_amount,
        0 payment_count,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_amount
    from tmp_order
    union all
    select
        province_id,
        0 visit_count,
        0 login_count,
        0 visitor_count,
        0 user_count,
        0 order_count,
        0 order_original_amount,
        0 order_final_amount,
        payment_count,
        payment_amount,
        0 refund_order_count,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_amount
    from tmp_pay
    union all
    select
        province_id,
        0 visit_count,
        0 login_count,
        0 visitor_count,
        0 user_count,
        0 order_count,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_amount,
        refund_order_count,
        refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_amount
    from tmp_ro
    union all
    select
        province_id,
        0 visit_count,
        0 login_count,
        0 visitor_count,
        0 user_count,
        0 order_count,
 
 0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_amount,
        refund_payment_count,
        refund_payment_amount
    from tmp_rp
)t1
group by province_id;
"




case $1 in
    "dws_visitor_action_daycount" )
        hive -e "$dws_visitor_action_daycount"
    ;;
    "dws_user_action_daycount" )
        hive -e "$dws_user_action_daycount"
    ;;
    "dws_activity_info_daycount" )
        hive -e "$dws_activity_info_daycount"
    ;;
    "dws_area_stats_daycount" )
        hive -e "$dws_area_stats_daycount"
    ;;
    "dws_sku_action_daycount" )
        hive -e "$dws_sku_action_daycount"
    ;;
    "ws_coupon_info_daycount" 
    )
        hive -e "$dws_coupon_info_daycount"
    ;;
    "all" )
        hive -e "$dws_visitor_action_daycount$dws_user_action_daycount$dws_activity_info_daycount$dws_area_stats_daycount$dws_sku_action_daycount$dws_coupon_info_daycount"
    ;;
esac
```



### 5.6、DWT脚本

##### 5.6.1、DWT层首日数据导入脚本

```shell
#!/bin/bash

APP=gmall
APP1=gmall_dim
APP2=gmall_dwd
APP3=gmall_dws
APP4=gmall_dwt

if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi 

dwt_visitor_topic="
insert overwrite table ${APP4}.dwt_visitor_topic partition(dt='$do_date')
select
    nvl(1d_ago.mid_id,old.mid_id),
    nvl(1d_ago.brand,old.brand),
    nvl(1d_ago.model,old.model),
    nvl(1d_ago.channel,old.channel),
    nvl(1d_ago.os,old.os),
    nvl(1d_ago.area_code,old.area_code),
    nvl(1d_ago.version_code,old.version_code),
    case when old.mid_id is null and 1d_ago.is_new=1 then '$do_date'
         when old.mid_id is null and 1d_ago.is_new=0 then '2020-06-13'--无法获取准确的首次登录日期，给定一个数仓搭建日之前的日期
         else old.visit_date_first end,
    if(1d_ago.mid_id is not null,'$do_date',old.visit_date_last),
    nvl(1d_ago.visit_count,0),
    if(1d_ago.mid_id is null,0,1),
    nvl(old.visit_last_7d_count,0)+nvl(1d_ago.visit_count,0)- nvl(7d_ago.visit_count,0),
    nvl(old.visit_last_7d_day_count,0)+if(1d_ago.mid_id is null,0,1)- if(7d_ago.mid_id is null,0,1),
    nvl(old.visit_last_30d_count,0)+nvl(1d_ago.visit_count,0)- nvl(30d_ago.visit_count,0),
    nvl(old.visit_last_30d_day_count,0)+if(1d_ago.mid_id is null,0,1)- if(30d_ago.mid_id is null,0,1),
    nvl(old.visit_count,0)+nvl(1d_ago.visit_count,0),
    nvl(old.visit_day_count,0)+if(1d_ago.mid_id is null,0,1)
from
(
    select
        mid_id,
        brand,
        model,
        channel,
        os,
        area_code,
        version_code,
        visit_date_first,
        visit_date_last,
        visit_last_1d_count,
        visit_last_1d_day_count,
        visit_last_7d_count,
        visit_last_7d_day_count,
        visit_last_30d_count,
        visit_last_30d_day_count,
        visit_count,
        visit_day_count
    from ${APP4}.dwt_visitor_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        mid_id,
        brand,
        model,
        is_new,
        channel,
        os,
        area_code,
        version_code,
        visit_count
    from ${APP3}.dws_visitor_action_daycount
    where dt='$do_date'
)1d_ago
on old.mid_id=1d_ago.mid_id
left join
(
    select
        mid_id,
        brand,
        model,
        is_new,
        channel,
        os,
        area_code,
        version_code,
        visit_count
    from ${APP3}.dws_visitor_action_daycount
    where dt=date_add('$do_date',-7)
)7d_ago
on old.mid_id=7d_ago.mid_id
left join
(
    select
        mid_id,
        brand,
        model,
        is_new,
        channel,
        os,
        area_code,
        version_code,
        visit_count
    from ${APP3}.dws_visitor_action_daycount
    where dt=date_add('$do_date',-30)
)30d_ago
on old.mid_id=30d_ago.mid_id;
"

dwt_user_topic="
insert overwrite table ${APP4}.dwt_user_topic partition(dt='$do_date')
select
    id,
    login_date_first,--以用户的创建日期作为首次登录日期
    nvl(login_date_last,date_add('$do_date',-1)),--若有历史登录记录，则根据历史记录获取末次登录日期，否则统一指定一个日期
    nvl(login_last_1d_count,0),
    nvl(login_last_1d_day_count,0),
    nvl(login_last_7d_count,0),
    nvl(login_last_7d_day_count,0),
    nvl(login_last_30d_count,0),
    nvl(login_last_30d_day_count,0),
    nvl(login_count,0),
    nvl(login_day_count,0),
    order_date_first,
    order_date_last,
    nvl(order_last_1d_count,0),
    nvl(order_activity_last_1d_count,0),
    nvl(order_activity_reduce_last_1d_amount,0),
    nvl(order_coupon_last_1d_count,0),
    nvl(order_coupon_reduce_last_1d_amount,0),
    nvl(order_last_1d_original_amount,0),
    nvl(order_last_1d_final_amount,0),
    nvl(order_last_7d_count,0),
    nvl(order_activity_last_7d_count,0),
    nvl(order_activity_reduce_last_7d_amount,0),
    nvl(order_coupon_last_7d_count,0),
    nvl(order_coupon_reduce_last_7d_amount,0),
    nvl(order_last_7d_original_amount,0),
    nvl(order_last_7d_final_amount,0),
    nvl(order_last_30d_count,0),
    nvl(order_activity_last_30d_count,0),
    nvl(order_activity_reduce_last_30d_amount,0),
    nvl(order_coupon_last_30d_count,0),
    nvl(order_coupon_reduce_last_30d_amount,0),
    nvl(order_last_30d_original_amount,0),
    nvl(order_last_30d_final_amount,0),
    nvl(order_count,0),
    nvl(order_activity_count,0),
    nvl(order_activity_reduce_amount,0),
    nvl(order_coupon_count,0),
    nvl(order_coupon_reduce_amount,0),
    nvl(order_original_amount,0),
    nvl(order_final_amount,0),
    payment_date_first,
    payment_date_last,
    nvl(payment_last_1d_count,0),
    nvl(payment_last_1d_amount,0),
    nvl(payment_last_7d_count,0),
    nvl(payment_last_7d_amount,0),
    nvl(payment_last_30d_count,0),
    nvl(payment_last_30d_amount,0),
    nvl(payment_count,0),
    nvl(payment_amount,0),
    nvl(refund_order_last_1d_count,0),
    nvl(refund_order_last_1d_num,0),
    nvl(refund_order_last_1d_amount,0),
    nvl(refund_order_last_7d_count,0),
    nvl(refund_order_last_7d_num,0),
    nvl(refund_order_last_7d_amount,0),
    nvl(refund_order_last_30d_count,0),
    nvl(refund_order_last_30d_num,0),
    nvl(refund_order_last_30d_amount,0),
    nvl(refund_order_count,0),
    nvl(refund_order_num,0),
    nvl(refund_order_amount,0),
    nvl(refund_payment_last_1d_count,0),
    nvl(refund_payment_last_1d_num,0),
    nvl(refund_payment_last_1d_amount,0),
    nvl(refund_payment_last_7d_count,0),
    nvl(refund_payment_last_7d_num,0),
    nvl(refund_payment_last_7d_amount,0),
    nvl(refund_payment_last_30d_count,0),
    nvl(refund_payment_last_30d_num,0),
    nvl(refund_payment_last_30d_amount,0),
    nvl(refund_payment_count,0),
    nvl(refund_payment_num,0),
    nvl(refund_payment_amount,0),
    nvl(cart_last_1d_count,0),
    nvl(cart_last_7d_count,0),
    nvl(cart_last_30d_count,0),
    nvl(cart_count,0),
    nvl(favor_last_1d_count,0),
    nvl(favor_last_7d_count,0),
    nvl(favor_last_30d_count,0),
    nvl(favor_count,0),
    nvl(coupon_last_1d_get_count,0),
    nvl(coupon_last_1d_using_count,0),
    nvl(coupon_last_1d_used_count,0),
    nvl(coupon_last_7d_get_count,0),
    nvl(coupon_last_7d_using_count,0),
    nvl(coupon_last_7d_used_count,0),
    nvl(coupon_last_30d_get_count,0),
    nvl(coupon_last_30d_using_count,0),
    nvl(coupon_last_30d_used_count,0),
    nvl(coupon_get_count,0),
    nvl(coupon_using_count,0),
    nvl(coupon_used_count,0),
    nvl(appraise_last_1d_good_count,0),
    nvl(appraise_last_1d_mid_count,0),
    nvl(appraise_last_1d_bad_count,0),
    nvl(appraise_last_1d_default_count,0),
    nvl(appraise_last_7d_good_count,0),
    nvl(appraise_last_7d_mid_count,0),
    nvl(appraise_last_7d_bad_count,0),
    nvl(appraise_last_7d_default_count,0),
    nvl(appraise_last_30d_good_count,0),
    nvl(appraise_last_30d_mid_count,0),
    nvl(appraise_last_30d_bad_count,0),
    nvl(appraise_last_30d_default_count,0),
    nvl(appraise_good_count,0),
    nvl(appraise_mid_count,0),
    nvl(appraise_bad_count,0),
    nvl(appraise_default_count,0)
from
(
    select
        id,
        date_format(create_time,'yyyy-MM-dd') login_date_first
    from ${APP1}.dim_user_info
    where dt='9999-99-99'
)t1
left join
(
    select
        user_id user_id,
        max(dt) login_date_last,
        sum(if(dt='$do_date',login_count,0)) login_last_1d_count,
        sum(if(dt='$do_date' and login_count>0,1,0)) login_last_1d_day_count,
        sum(if(dt>=date_add('$do_date',-6),login_count,0)) login_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6) and login_count>0,1,0)) login_last_7d_day_count,
        sum(if(dt>=date_add('$do_date',-29),login_count,0)) login_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29) and login_count>0,1,0)) login_last_30d_day_count,
        sum(login_count) login_count,
        sum(if(login_count>0,1,0)) login_day_count,
        min(if(order_count>0,dt,null)) order_date_first,
        max(if(order_count>0,dt,null)) order_date_last,
        sum(if(dt='$do_date',order_count,0)) order_last_1d_count,
        sum(if(dt='$do_date',order_activity_count,0)) order_activity_last_1d_count,
        sum(if(dt='$do_date',order_activity_reduce_amount,0)) order_activity_reduce_last_1d_amount,
        sum(if(dt='$do_date',order_coupon_count,0)) order_coupon_last_1d_count,
        sum(if(dt='$do_date',order_coupon_reduce_amount,0)) order_coupon_reduce_last_1d_amount,
        sum(if(dt='$do_date',order_original_amount,0)) order_last_1d_original_amount,
        sum(if(dt='$do_date',order_final_amount,0)) order_last_1d_final_amount,
        sum(if(dt>=date_add('$do_date',-6),order_count,0)) order_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),order_activity_count,0)) order_activity_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),order_activity_reduce_amount,0)) order_activity_reduce_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-6),order_coupon_count,0)) order_coupon_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),order_coupon_reduce_amount,0)) order_coupon_reduce_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-6),order_original_amount,0)) order_last_7d_original_amount,
        sum(if(dt>=date_add('$do_date',-6),order_final_amount,0)) order_last_7d_final_amount,
        sum(if(dt>=date_add('$do_date',-29),order_count,0)) order_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),order_activity_count,0)) order_activity_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),order_activity_reduce_amount,0)) order_activity_reduce_last_30d_amount,
        sum(if(dt>=date_add('$do_date',-29),order_coupon_count,0)) order_coupon_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),order_coupon_reduce_amount,0)) order_coupon_reduce_last_30d_amount,
        sum(if(dt>=date_add('$do_date',-29),order_original_amount,0)) order_last_30d_original_amount,
        sum(if(dt>=date_add('$do_date',-29),order_final_amount,0)) order_last_30d_final_amount,
        sum(order_count) order_count,
        sum(order_activity_count) order_activity_count,
        sum(order_activity_reduce_amount) order_activity_reduce_amount,
        sum(order_coupon_count) order_coupon_count,
        sum(order_coupon_reduce_amount) order_coupon_reduce_amount,
        sum(order_original_amount) order_original_amount,
        sum(order_final_amount) order_final_amount,
        min(if(payment_count>0,dt,null)) payment_date_first,
        max(if(payment_count>0,dt,null)) payment_date_last,
        sum(if(dt='$do_date',payment_count,0)) payment_last_1d_count,
        sum(if(dt='$do_date',payment_amount,0)) payment_last_1d_amount,
        sum(if(dt>=date_add('$do_date',-6),payment_count,0)) payment_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),payment_amount,0)) payment_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-29),payment_count,0)) payment_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),payment_amount,0)) payment_last_30d_amount,
        sum(payment_count) payment_count,
        sum(payment_amount) payment_amount,
        sum(if(dt='$do_date',refund_order_count,0)) refund_order_last_1d_count,
        sum(if(dt='$do_date',refund_order_num,0)) refund_order_last_1d_num,
        sum(if(dt='$do_date',refund_order_amount,0)) refund_order_last_1d_amount,
        sum(if(dt>=date_add('$do_date',-6),refund_order_count,0)) refund_order_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),refund_order_num,0)) refund_order_last_7d_num,
        sum(if(dt>=date_add('$do_date',-6),refund_order_amount,0)) refund_order_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-29),refund_order_count,0)) refund_order_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),refund_order_num,0)) refund_order_last_30d_num,
        sum(if(dt>=date_add('$do_date',-29),refund_order_amount,0)) refund_order_last_30d_amount,
        sum(refund_order_count) refund_order_count,
        sum(refund_order_num) refund_order_num,
        sum(refund_order_amount) refund_order_amount,
        sum(if(dt='$do_date',refund_payment_count,0)) refund_payment_last_1d_count,
        sum(if(dt='$do_date',refund_payment_num,0)) refund_payment_last_1d_num,
        sum(if(dt='$do_date',refund_payment_amount,0)) refund_payment_last_1d_amount,
        sum(if(dt>=date_add('$do_date',-6),refund_payment_count,0)) refund_payment_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),refund_payment_num,0)) refund_payment_last_7d_num,
        sum(if(dt>=date_add('$do_date',-6),refund_payment_amount,0)) refund_payment_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-29),refund_payment_count,0)) refund_payment_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),refund_payment_num,0)) refund_payment_last_30d_num,
        sum(if(dt>=date_add('$do_date',-29),refund_payment_amount,0)) refund_payment_last_30d_amount,
        sum(refund_payment_count) refund_payment_count,
        sum(refund_payment_num) refund_payment_num,
        sum(refund_payment_amount) refund_payment_amount,
        sum(if(dt='$do_date',cart_count,0)) cart_last_1d_count,
        sum(if(dt>=date_add('$do_date',-6),cart_count,0)) cart_last_7d_count,
        sum(if(dt>=date_add('$do_date',-29),cart_count,0)) cart_last_30d_count,
        sum(cart_count) cart_count,
        sum(if(dt='$do_date',favor_count,0)) favor_last_1d_count,
        sum(if(dt>=date_add('$do_date',-6),favor_count,0)) favor_last_7d_count,
        sum(if(dt>=date_add('$do_date',-29),favor_count,0)) favor_last_30d_count,
        sum(favor_count) favor_count,
        sum(if(dt='$do_date',coupon_get_count,0)) coupon_last_1d_get_count,
        sum(if(dt='$do_date',coupon_using_count,0)) coupon_last_1d_using_count,
        sum(if(dt='$do_date',coupon_used_count,0)) coupon_last_1d_used_count,
        sum(if(dt>=date_add('$do_date',-6),coupon_get_count,0)) coupon_last_7d_get_count,
        sum(if(dt>=date_add('$do_date',-6),coupon_using_count,0)) coupon_last_7d_using_count,
        sum(if(dt>=date_add('$do_date',-6),coupon_used_count,0)) coupon_last_7d_used_count,
        sum(if(dt>=date_add('$do_date',-29),coupon_get_count,0)) coupon_last_30d_get_count,
        sum(if(dt>=date_add('$do_date',-29),coupon_using_count,0)) coupon_last_30d_using_count,
        sum(if(dt>=date_add('$do_date',-29),coupon_used_count,0)) coupon_last_30d_used_count,
        sum(coupon_get_count) coupon_get_count,
        sum(coupon_using_count) coupon_using_count,
        sum(coupon_used_count) coupon_used_count,
        sum(if(dt='$do_date',appraise_good_count,0)) appraise_last_1d_good_count,
        sum(if(dt='$do_date',appraise_mid_count,0)) appraise_last_1d_mid_count,
        sum(if(dt='$do_date',appraise_bad_count,0)) appraise_last_1d_bad_count,
        sum(if(dt='$do_date',appraise_default_count,0)) appraise_last_1d_default_count,
        sum(if(dt>=date_add('$do_date',-6),appraise_good_count,0)) appraise_last_7d_good_count,
        sum(if(dt>=date_add('$do_date',-6),appraise_mid_count,0)) appraise_last_7d_mid_count,
        sum(if(dt>=date_add('$do_date',-6),appraise_bad_count,0)) appraise_last_7d_bad_count,
        sum(if(dt>=date_add('$do_date',-6),appraise_default_count,0)) appraise_last_7d_default_count,
        sum(if(dt>=date_add('$do_date',-29),appraise_good_count,0)) appraise_last_30d_good_count,
        sum(if(dt>=date_add('$do_date',-29),appraise_mid_count,0)) appraise_last_30d_mid_count,
        sum(if(dt>=date_add('$do_date',-29),appraise_bad_count,0)) appraise_last_30d_bad_count,
        sum(if(dt>=date_add('$do_date',-29),appraise_default_count,0)) appraise_last_30d_default_count,
        sum(appraise_good_count) appraise_good_count,
        sum(appraise_mid_count) appraise_mid_count,
        sum(appraise_bad_count) appraise_bad_count,
        sum(appraise_default_count) appraise_default_count
    from ${APP3}.dws_user_action_daycount
    group by user_id
)t2
on t1.id=t2.user_id;
"

dwt_sku_topic="
insert overwrite table ${APP4}.dwt_sku_topic partition(dt='$do_date')
select
    id,
    nvl(order_last_1d_count,0),
    nvl(order_last_1d_num,0),
    nvl(order_activity_last_1d_count,0),
    nvl(order_coupon_last_1d_count,0),
    nvl(order_activity_reduce_last_1d_amount,0),
    nvl(order_coupon_reduce_last_1d_amount,0),
    nvl(order_last_1d_original_amount,0),
    nvl(order_last_1d_final_amount,0),
    nvl(order_last_7d_count,0),
    nvl(order_last_7d_num,0),
    nvl(order_activity_last_7d_count,0),
    nvl(order_coupon_last_7d_count,0),
    nvl(order_activity_reduce_last_7d_amount,0),
    nvl(order_coupon_reduce_last_7d_amount,0),
    nvl(order_last_7d_original_amount,0),
    nvl(order_last_7d_final_amount,0),
    nvl(order_last_30d_count,0),
    nvl(order_last_30d_num,0),
    nvl(order_activity_last_30d_count,0),
    nvl(order_coupon_last_30d_count,0),
    nvl(order_activity_reduce_last_30d_amount,0),
    nvl(order_coupon_reduce_last_30d_amount,0),
    nvl(order_last_30d_original_amount,0),
    nvl(order_last_30d_final_amount,0),
    nvl(order_count,0),
    nvl(order_num,0),
    nvl(order_activity_count,0),
    nvl(order_coupon_count,0),
    nvl(order_activity_reduce_amount,0),
    nvl(order_coupon_reduce_amount,0),
    nvl(order_original_amount,0),
    nvl(order_final_amount,0),
    nvl(payment_last_1d_count,0),
    nvl(payment_last_1d_num,0),
    nvl(payment_last_1d_amount,0),
    nvl(payment_last_7d_count,0),
    nvl(payment_last_7d_num,0),
    nvl(payment_last_7d_amount,0),
    nvl(payment_last_30d_count,0),
    nvl(payment_last_30d_num,0),
    nvl(payment_last_30d_amount,0),
    nvl(payment_count,0),
    nvl(payment_num,0),
    nvl(payment_amount,0),
    nvl(refund_order_last_1d_count,0),
    nvl(refund_order_last_1d_num,0),
    nvl(refund_order_last_1d_amount,0),
    nvl(refund_order_last_7d_count,0),
    nvl(refund_order_last_7d_num,0),
    nvl(refund_order_last_7d_amount,0),
    nvl(refund_order_last_30d_count,0),
    nvl(refund_order_last_30d_num,0),
    nvl(refund_order_last_30d_amount,0),
    nvl(refund_order_count,0),
    nvl(refund_order_num,0),
    nvl(refund_order_amount,0),
    nvl(refund_payment_last_1d_count,0),
    nvl(refund_payment_last_1d_num,0),
    nvl(refund_payment_last_1d_amount,0),
    nvl(refund_payment_last_7d_count,0),
    nvl(refund_payment_last_7d_num,0),
    nvl(refund_payment_last_7d_amount,0),
    nvl(refund_payment_last_30d_count,0),
    nvl(refund_payment_last_30d_num,0),
    nvl(refund_payment_last_30d_amount,0),
    nvl(refund_payment_count,0),
    nvl(refund_payment_num,0),
    nvl(refund_payment_amount,0),
    nvl(cart_last_1d_count,0),
    nvl(cart_last_7d_count,0),
    nvl(cart_last_30d_count,0),
    nvl(cart_count,0),
    nvl(favor_last_1d_count,0),
    nvl(favor_last_7d_count,0),
    nvl(favor_last_30d_count,0),
    nvl(favor_count,0),
    nvl(appraise_last_1d_good_count,0),
    nvl(appraise_last_1d_mid_count,0),
    nvl(appraise_last_1d_bad_count,0),
    nvl(appraise_last_1d_default_count,0),
    nvl(appraise_last_7d_good_count,0),
    nvl(appraise_last_7d_mid_count,0),
    nvl(appraise_last_7d_bad_count,0),
    nvl(appraise_last_7d_default_count,0),
    nvl(appraise_last_30d_good_count,0),
    nvl(appraise_last_30d_mid_count,0),
    nvl(appraise_last_30d_bad_count,0),
    nvl(appraise_last_30d_default_count,0),
    nvl(appraise_good_count,0),
    nvl(appraise_mid_count,0),
    nvl(appraise_bad_count,0),
    nvl(appraise_default_count,0)
from
(
    select
        sku_id id
    from ${APP1}.dim_sku_info
    where dt='$do_date'
)t1
left join
(
    select
        sku_id,
        sum(if(dt='$do_date',order_count,0)) order_last_1d_count,
        sum(if(dt='$do_date',order_num,0)) order_last_1d_num,
        sum(if(dt='$do_date',order_activity_count,0)) order_activity_last_1d_count,
        sum(if(dt='$do_date',order_coupon_count,0)) order_coupon_last_1d_count,
        sum(if(dt='$do_date',order_activity_reduce_amount,0)) order_activity_reduce_last_1d_amount,
        sum(if(dt='$do_date',order_coupon_reduce_amount,0)) order_coupon_reduce_last_1d_amount,
        sum(if(dt='$do_date',order_original_amount,0)) order_last_1d_original_amount,
        sum(if(dt='$do_date',order_final_amount,0)) order_last_1d_final_amount,
        sum(if(dt>=date_add('$do_date',-6),order_count,0)) order_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),order_num,0)) order_last_7d_num,
        sum(if(dt>=date_add('$do_date',-6),order_activity_count,0)) order_activity_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),order_coupon_count,0)) order_coupon_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),order_activity_reduce_amount,0)) order_activity_reduce_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-6),order_coupon_reduce_amount,0)) order_coupon_reduce_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-6),order_original_amount,0)) order_last_7d_original_amount,
        sum(if(dt>=date_add('$do_date',-6),order_final_amount,0)) order_last_7d_final_amount,
        sum(if(dt>=date_add('$do_date',-29),order_count,0)) order_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),order_num,0)) order_last_30d_num,
        sum(if(dt>=date_add('$do_date',-29),order_activity_count,0)) order_activity_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),order_coupon_count,0)) order_coupon_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),order_activity_reduce_amount,0)) order_activity_reduce_last_30d_amount,
        sum(if(dt>=date_add('$do_date',-29),order_coupon_reduce_amount,0)) order_coupon_reduce_last_30d_amount,
        sum(if(dt>=date_add('$do_date',-29),order_original_amount,0)) order_last_30d_original_amount,
        sum(if(dt>=date_add('$do_date',-29),order_final_amount,0)) order_last_30d_final_amount,
        sum(order_count) order_count,
        sum(order_num) order_num,
        sum(order_activity_count) order_activity_count,
        sum(order_coupon_count) order_coupon_count,
        sum(order_activity_reduce_amount) order_activity_reduce_amount,
        sum(order_coupon_reduce_amount) order_coupon_reduce_amount,
        sum(order_original_amount) order_original_amount,
        sum(order_final_amount) order_final_amount,
        sum(if(dt='$do_date',payment_count,0)) payment_last_1d_count,
        sum(if(dt='$do_date',payment_num,0)) payment_last_1d_num,
        sum(if(dt='$do_date',payment_amount,0)) payment_last_1d_amount,
        sum(if(dt>=date_add('$do_date',-6),payment_count,0)) payment_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),payment_num,0)) payment_last_7d_num,
        sum(if(dt>=date_add('$do_date',-6),payment_amount,0)) payment_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-29),payment_count,0)) payment_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),payment_num,0)) payment_last_30d_num,
        sum(if(dt>=date_add('$do_date',-29),payment_amount,0)) payment_last_30d_amount,
        sum(payment_count) payment_count,
        sum(payment_num) payment_num,
        sum(payment_amount) payment_amount,
        sum(if(dt='$do_date',refund_order_count,0)) refund_order_last_1d_count,
        sum(if(dt='$do_date',refund_order_num,0)) refund_order_last_1d_num,
        sum(if(dt='$do_date',refund_order_amount,0)) refund_order_last_1d_amount,
        sum(if(dt>=date_add('$do_date',-6),refund_order_count,0)) refund_order_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),refund_order_num,0)) refund_order_last_7d_num,
        sum(if(dt>=date_add('$do_date',-6),refund_order_amount,0)) refund_order_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-29),refund_order_count,0)) refund_order_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),refund_order_num,0)) refund_order_last_30d_num,
        sum(if(dt>=date_add('$do_date',-29),refund_order_amount,0)) refund_order_last_30d_amount,
        sum(refund_order_count) refund_order_count,
        sum(refund_order_num) refund_order_num,
        sum(refund_order_amount) refund_order_amount,
        sum(if(dt='$do_date',refund_payment_count,0)) refund_payment_last_1d_count,
        sum(if(dt='$do_date',refund_payment_num,0)) refund_payment_last_1d_num,
        sum(if(dt='$do_date',refund_payment_amount,0)) refund_payment_last_1d_amount,
        sum(if(dt>=date_add('$do_date',-6),refund_payment_count,0)) refund_payment_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),refund_payment_num,0)) refund_payment_last_7d_num,
        sum(if(dt>=date_add('$do_date',-6),refund_payment_amount,0)) refund_payment_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-29),refund_payment_count,0)) refund_payment_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),refund_payment_num,0)) refund_payment_last_30d_num,
        sum(if(dt>=date_add('$do_date',-29),refund_payment_amount,0)) refund_payment_last_30d_amount,
        sum(refund_payment_count) refund_payment_count,
        sum(refund_payment_num) refund_payment_num,
        sum(refund_payment_amount) refund_payment_amount,
        sum(if(dt='$do_date',cart_count,0)) cart_last_1d_count,
        sum(if(dt>=date_add('$do_date',-6),cart_count,0)) cart_last_7d_count,
        sum(if(dt>=date_add('$do_date',-29),cart_count,0)) cart_last_30d_count,
        sum(cart_count) cart_count,
        sum(if(dt='$do_date',favor_count,0)) favor_last_1d_count,
        sum(if(dt>=date_add('$do_date',-6),favor_count,0)) favor_last_7d_count,
        sum(if(dt>=date_add('$do_date',-29),favor_count,0)) favor_last_30d_count,
        sum(favor_count) favor_count,
        sum(if(dt='$do_date',appraise_good_count,0)) appraise_last_1d_good_count,
        sum(if(dt='$do_date',appraise_mid_count,0)) appraise_last_1d_mid_count,
        sum(if(dt='$do_date',appraise_bad_count,0)) appraise_last_1d_bad_count,
        sum(if(dt='$do_date',appraise_default_count,0)) appraise_last_1d_default_count,
        sum(if(dt>=date_add('$do_date',-6),appraise_good_count,0)) appraise_last_7d_good_count,
        sum(if(dt>=date_add('$do_date',-6),appraise_mid_count,0)) appraise_last_7d_mid_count,
        sum(if(dt>=date_add('$do_date',-6),appraise_bad_count,0)) appraise_last_7d_bad_count,
        sum(if(dt>=date_add('$do_date',-6),appraise_default_count,0)) appraise_last_7d_default_count,
        sum(if(dt>=date_add('$do_date',-29),appraise_good_count,0)) appraise_last_30d_good_count,
        sum(if(dt>=date_add('$do_date',-29),appraise_mid_count,0)) appraise_last_30d_mid_count,
        sum(if(dt>=date_add('$do_date',-29),appraise_bad_count,0)) appraise_last_30d_bad_count,
        sum(if(dt>=date_add('$do_date',-29),appraise_default_count,0)) appraise_last_30d_default_count,
        sum(appraise_good_count) appraise_good_count,
        sum(appraise_mid_count) appraise_mid_count,
        sum(appraise_bad_count) appraise_bad_count,
        sum(appraise_default_count) appraise_default_count
    from ${APP3}.dws_sku_action_daycount
    group by sku_id
)t2
on t1.id=t2.sku_id;
"

dwt_coupon_topic="
insert overwrite table ${APP4}.dwt_coupon_topic partition(dt='$do_date')
select
    id,
    nvl(get_last_1d_count,0),
    nvl(get_last_7d_count,0),
    nvl(get_last_30d_count,0),
    nvl(get_count,0),
    nvl(order_last_1d_count,0),
    nvl(order_last_1d_reduce_amount,0),
    nvl(order_last_1d_original_amount,0),
    nvl(order_last_1d_final_amount,0),
    nvl(order_last_7d_count,0),
    nvl(order_last_7d_reduce_amount,0),
    nvl(order_last_7d_original_amount,0),
    nvl(order_last_7d_final_amount,0),
    nvl(order_last_30d_count,0),
    nvl(order_last_30d_reduce_amount,0),
    nvl(order_last_30d_original_amount,0),
    nvl(order_last_30d_final_amount,0),
    nvl(order_count,0),
    nvl(order_reduce_amount,0),
    nvl(order_original_amount,0),
    nvl(order_final_amount,0),
    nvl(payment_last_1d_count,0),
    nvl(payment_last_1d_reduce_amount,0),
    nvl(payment_last_1d_amount,0),
    nvl(payment_last_7d_count,0),
    nvl(payment_last_7d_reduce_amount,0),
    nvl(payment_last_7d_amount,0),
    nvl(payment_last_30d_count,0),
    nvl(payment_last_30d_reduce_amount,0),
    nvl(payment_last_30d_amount,0),
    nvl(payment_count,0),
    nvl(payment_reduce_amount,0),
    nvl(payment_amount,0),
    nvl(expire_last_1d_count,0),
    nvl(expire_last_7d_count,0),
    nvl(expire_last_30d_count,0),
    nvl(expire_count,0)
from
(
    select
        coupon_id id
    from ${APP1}.dim_coupon_info
    where dt='$do_date'
)t1
left join
(
    select
        coupon_id coupon_id,
        sum(if(dt='$do_date',get_count,0)) get_last_1d_count,
        sum(if(dt>=date_add('$do_date',-6),get_count,0)) get_last_7d_count,
        sum(if(dt>=date_add('$do_date',-29),get_count,0)) get_last_30d_count,
        sum(get_count) get_count,
        sum(if(dt='$do_date',order_count,0)) order_last_1d_count,
        sum(if(dt='$do_date',order_reduce_amount,0)) order_last_1d_reduce_amount,
        sum(if(dt='$do_date',order_original_amount,0)) order_last_1d_original_amount,
        sum(if(dt='$do_date',order_final_amount,0)) order_last_1d_final_amount,
        sum(if(dt>=date_add('$do_date',-6),order_count,0)) order_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),order_reduce_amount,0)) order_last_7d_reduce_amount,
        sum(if(dt>=date_add('$do_date',-6),order_original_amount,0)) order_last_7d_original_amount,
        sum(if(dt>=date_add('$do_date',-6),order_final_amount,0)) order_last_7d_final_amount,
        sum(if(dt>=date_add('$do_date',-29),order_count,0)) order_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),order_reduce_amount,0)) order_last_30d_reduce_amount,
        sum(if(dt>=date_add('$do_date',-29),order_original_amount,0)) order_last_30d_original_amount,
        sum(if(dt>=date_add('$do_date',-29),order_final_amount,0)) order_last_30d_final_amount,
        sum(order_count) order_count,
        sum(order_reduce_amount) order_reduce_amount,
        sum(order_original_amount) order_original_amount,
        sum(order_final_amount) order_final_amount,
        sum(if(dt='$do_date',payment_count,0)) payment_last_1d_count,
        sum(if(dt='$do_date',payment_reduce_amount,0)) payment_last_1d_reduce_amount,
        sum(if(dt='$do_date',payment_amount,0)) payment_last_1d_amount,
        sum(if(dt>=date_add('$do_date',-6),payment_count,0)) payment_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),payment_reduce_amount,0)) payment_last_7d_reduce_amount,
        sum(if(dt>=date_add('$do_date',-6),payment_amount,0)) payment_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-29),payment_count,0)) payment_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),payment_reduce_amount,0)) payment_last_30d_reduce_amount,
        sum(if(dt>=date_add('$do_date',-29),payment_amount,0)) payment_last_30d_amount,
        sum(payment_count) payment_count,
        sum(payment_reduce_amount) payment_reduce_amount,
        sum(payment_amount) payment_amount,
        sum(if(dt='$do_date',expire_count,0)) expire_last_1d_count,
        sum(if(dt>=date_add('$do_date',-6),expire_count,0)) expire_last_7d_count,
        sum(if(dt>=date_add('$do_date',-29),expire_count,0)) expire_last_30d_count,
        sum(expire_count) expire_count
    from ${APP3}.dws_coupon_info_daycount
    group by coupon_id
)t2
on t1.id=t2.coupon_id;
"

dwt_activity_topic="
insert overwrite table ${APP4}.dwt_activity_topic partition(dt='$do_date')
select
    t1.activity_rule_id,
    t1.activity_id,
    nvl(order_last_1d_count,0),
    nvl(order_last_1d_reduce_amount,0),
    nvl(order_last_1d_original_amount,0),
    nvl(order_last_1d_final_amount,0),
    nvl(order_count,0),
    nvl(order_reduce_amount,0),
    nvl(order_original_amount,0),
    nvl(order_final_amount,0),
    nvl(payment_last_1d_count,0),
    nvl(payment_last_1d_reduce_amount,0),
    nvl(payment_last_1d_amount,0),
    nvl(payment_count,0),
    nvl(payment_reduce_amount,0),
    nvl(payment_amount,0)
from
(
    select
        activity_rule_id,
        activity_id
    from ${APP1}.dim_activity_rule_info
    where dt='$do_date'
)t1
left join
(
    select
        activity_rule_id,
        activity_id,
        sum(if(dt='$do_date',order_count,0)) order_last_1d_count,
        sum(if(dt='$do_date',order_reduce_amount,0)) order_last_1d_reduce_amount,
        sum(if(dt='$do_date',order_original_amount,0)) order_last_1d_original_amount,
        sum(if(dt='$do_date',order_final_amount,0)) order_last_1d_final_amount,
        sum(order_count) order_count,
        sum(order_reduce_amount) order_reduce_amount,
        sum(order_original_amount) order_original_amount,
        sum(order_final_amount) order_final_amount,
        sum(if(dt='$do_date',payment_count,0)) payment_last_1d_count,
        sum(if(dt='$do_date',payment_reduce_amount,0)) payment_last_1d_reduce_amount,
        sum(if(dt='$do_date',payment_amount,0)) payment_last_1d_amount,
        sum(payment_count) payment_count,
        sum(payment_reduce_amount) payment_reduce_amount,
        sum(payment_amount) payment_amount
    from ${APP3}.dws_activity_info_daycount
    group by activity_rule_id,activity_id
)t2
on t1.activity_rule_id=t2.activity_rule_id
and t1.activity_id=t2.activity_id;
"

dwt_area_topic="
insert overwrite table ${APP4}.dwt_area_topic partition(dt='$do_date')
select
    id,
    nvl(visit_last_1d_count,0),
    nvl(login_last_1d_count,0),
    nvl(visit_last_7d_count,0),
    nvl(login_last_7d_count,0),
    nvl(visit_last_30d_count,0),
    nvl(login_last_30d_count,0),
    nvl(visit_count,0),
    nvl(login_count,0),
    nvl(order_last_1d_count,0),
    nvl(order_last_1d_original_amount,0),
    nvl(order_last_1d_final_amount,0),
    nvl(order_last_7d_count,0),
    nvl(order_last_7d_original_amount,0),
    nvl(order_last_7d_final_amount,0),
    nvl(order_last_30d_count,0),
    nvl(order_last_30d_original_amount,0),
    nvl(order_last_30d_final_amount,0),
    nvl(order_count,0),
    nvl(order_original_amount,0),
    nvl(order_final_amount,0),
    nvl(payment_last_1d_count,0),
    nvl(payment_last_1d_amount,0),
    nvl(payment_last_7d_count,0),
    nvl(payment_last_7d_amount,0),
    nvl(payment_last_30d_count,0),
    nvl(payment_last_30d_amount,0),
    nvl(payment_count,0),
    nvl(payment_amount,0),
    nvl(refund_order_last_1d_count,0),
    nvl(refund_order_last_1d_amount,0),
    nvl(refund_order_last_7d_count,0),
    nvl(refund_order_last_7d_amount,0),
    nvl(refund_order_last_30d_count,0),
    nvl(refund_order_last_30d_amount,0),
    nvl(refund_order_count,0),
    nvl(refund_order_amount,0),
    nvl(refund_payment_last_1d_count,0),
    nvl(refund_payment_last_1d_amount,0),
    nvl(refund_payment_last_7d_count,0),
    nvl(refund_payment_last_7d_amount,0),
    nvl(refund_payment_last_30d_count,0),
    nvl(refund_payment_last_30d_amount,0),
    nvl(refund_payment_count,0),
    nvl(refund_payment_amount,0)
from
(
    select
        id
    from ${APP1}.dim_base_province
)t1
left join
(
    select
        province_id province_id,
        sum(if(dt='$do_date',visit_count,0)) visit_last_1d_count,
        sum(if(dt='$do_date',login_count,0)) login_last_1d_count,
        sum(if(dt>=date_add('$do_date',-6),visit_count,0)) visit_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),login_count,0)) login_last_7d_count,
        sum(if(dt>=date_add('$do_date',-29),visit_count,0)) visit_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),login_count,0)) login_last_30d_count,
        sum(visit_count) visit_count,
        sum(login_count) login_count,
        sum(if(dt='$do_date',order_count,0)) order_last_1d_count,
        sum(if(dt='$do_date',order_original_amount,0)) order_last_1d_original_amount,
        sum(if(dt='$do_date',order_final_amount,0)) order_last_1d_final_amount,
        sum(if(dt>=date_add('$do_date',-6),order_count,0)) order_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),order_original_amount,0)) order_last_7d_original_amount,
        sum(if(dt>=date_add('$do_date',-6),order_final_amount,0)) order_last_7d_final_amount,
        sum(if(dt>=date_add('$do_date',-29),order_count,0)) order_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),order_original_amount,0)) order_last_30d_original_amount,
        sum(if(dt>=date_add('$do_date',-29),order_final_amount,0)) order_last_30d_final_amount,
        sum(order_count) order_count,
        sum(order_original_amount) order_original_amount,
        sum(order_final_amount) order_final_amount,
        sum(if(dt='$do_date',payment_count,0)) payment_last_1d_count,
        sum(if(dt='$do_date',payment_amount,0)) payment_last_1d_amount,
        sum(if(dt>=date_add('$do_date',-6),payment_count,0)) payment_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),payment_amount,0)) payment_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-29),payment_count,0)) payment_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),payment_amount,0)) payment_last_30d_amount,
        sum(payment_count) payment_count,
        sum(payment_amount) payment_amount,
        sum(if(dt='$do_date',refund_order_count,0)) refund_order_last_1d_count,
        sum(if(dt='$do_date',refund_order_amount,0)) refund_order_last_1d_amount,
        sum(if(dt>=date_add('$do_date',-6),refund_order_count,0)) refund_order_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),refund_order_amount,0)) refund_order_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-29),refund_order_count,0)) refund_order_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),refund_order_amount,0)) refund_order_last_30d_amount,
        sum(refund_order_count) refund_order_count,
        sum(refund_order_amount) refund_order_amount,
        sum(if(dt='$do_date',refund_payment_count,0)) refund_payment_last_1d_count,
        sum(if(dt='$do_date',refund_payment_amount,0)) refund_payment_last_1d_amount,
        sum(if(dt>=date_add('$do_date',-6),refund_payment_count,0)) refund_payment_last_7d_count,
        sum(if(dt>=date_add('$do_date',-6),refund_payment_amount,0)) refund_payment_last_7d_amount,
        sum(if(dt>=date_add('$do_date',-29),refund_payment_count,0)) refund_payment_last_30d_count,
        sum(if(dt>=date_add('$do_date',-29),refund_payment_amount,0)) refund_payment_last_30d_amount,
        sum(refund_payment_count) refund_payment_count,
        sum(refund_payment_amount) refund_payment_amount
    from ${APP3}.dws_area_stats_daycount
    group by province_id
)t2
on t1.id=t2.province_id;
"


case $1 in
    "dwt_visitor_topic" )
        hive -e "$dwt_visitor_topic"
    ;;
    "dwt_user_topic" )
        hive -e "$dwt_user_topic"
    ;;
    "dwt_sku_topic" )
        hive -e "$dwt_sku_topic"
    ;;
    "dwt_activity_topic" )
        hive -e "$dwt_activity_topic"
    ;;
    "dwt_coupon_topic" )
        hive -e "$dwt_coupon_topic"
    ;;
    "dwt_area_topic" )
        hive -e "$dwt_area_topic"
    ;;
    "all" )
        hive -e "$dwt_visitor_topic$dwt_user_topic$dwt_sku_topic$dwt_activity_topic$dwt_coupon_topic$dwt_area_topic"
    ;;
esac
```

##### 5.6.2、DWT层每日数据导入脚本

```shell
#!/bin/bash

APP=gmall
APP1=gmall_dim
APP2=gmall_dwd
APP3=gmall_dws
APP4=gmall_dwt
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

clear_date=`date -d "$do_date -2 day" +%F`

dwt_visitor_topic="
insert overwrite table ${APP4}.dwt_visitor_topic partition(dt='$do_date')
select
    nvl(1d_ago.mid_id,old.mid_id),
    nvl(1d_ago.brand,old.brand),
    nvl(1d_ago.model,old.model),
    nvl(1d_ago.channel,old.channel),
    nvl(1d_ago.os,old.os),
    nvl(1d_ago.area_code,old.area_code),
    nvl(1d_ago.version_code,old.version_code),
    case when old.mid_id is null and 1d_ago.is_new=1 then '$do_date'
         when old.mid_id is null and 1d_ago.is_new=0 then '2020-06-13'--无法获取准确的首次登录日期，给定一个数仓搭建日之前的日期
         else old.visit_date_first end,
    if(1d_ago.mid_id is not null,'$do_date',old.visit_date_last),
    nvl(1d_ago.visit_count,0),
    if(1d_ago.mid_id is null,0,1),
    nvl(old.visit_last_7d_count,0)+nvl(1d_ago.visit_count,0)- nvl(7d_ago.visit_count,0),
    nvl(old.visit_last_7d_day_count,0)+if(1d_ago.mid_id is null,0,1)- if(7d_ago.mid_id is null,0,1),
    nvl(old.visit_last_30d_count,0)+nvl(1d_ago.visit_count,0)- nvl(30d_ago.visit_count,0),
    nvl(old.visit_last_30d_day_count,0)+if(1d_ago.mid_id is null,0,1)- if(30d_ago.mid_id is null,0,1),
    nvl(old.visit_count,0)+nvl(1d_ago.visit_count,0),
    nvl(old.visit_day_count,0)+if(1d_ago.mid_id is null,0,1)
from
(
    select
        mid_id,
        brand,
        model,
        channel,
        os,
        area_code,
        version_code,
        visit_date_first,
        visit_date_last,
        visit_last_1d_count,
        visit_last_1d_day_count,
        visit_last_7d_count,
        visit_last_7d_day_count,
        visit_last_30d_count,
        visit_last_30d_day_count,
        visit_count,
        visit_day_count
    from ${APP4}.dwt_visitor_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        mid_id,
        brand,
        model,
        is_new,
        channel,
        os,
        area_code,
        version_code,
        visit_count
    from ${APP3}.dws_visitor_action_daycount
    where dt='$do_date'
)1d_ago
on old.mid_id=1d_ago.mid_id
left join
(
    select
        mid_id,
        brand,
        model,
        is_new,
        channel,
        os,
        area_code,
        version_code,
        visit_count
    from ${APP3}.dws_visitor_action_daycount
    where dt=date_add('$do_date',-7)
)7d_ago
on old.mid_id=7d_ago.mid_id
left join
(
    select
        mid_id,
        brand,
        model,
        is_new,
        channel,
        os,
        area_code,
        version_code,
        visit_count
    from ${APP3}.dws_visitor_action_daycount
    where dt=date_add('$do_date',-30)
)30d_ago
on old.mid_id=30d_ago.mid_id;
alter table ${APP4}.dwt_visitor_topic drop partition(dt='$clear_date');
"

dwt_user_topic="
insert overwrite table ${APP4}.dwt_user_topic partition(dt='$do_date')
select
    nvl(1d_ago.user_id,old.user_id),
    nvl(old.login_date_first,'$do_date'),
    if(1d_ago.user_id is not null,'$do_date',old.login_date_last),
    nvl(1d_ago.login_count,0),
    if(1d_ago.user_id is not null,1,0),
    nvl(old.login_last_7d_count,0)+nvl(1d_ago.login_count,0)- nvl(7d_ago.login_count,0),
    nvl(old.login_last_7d_day_count,0)+if(1d_ago.user_id is null,0,1)- if(7d_ago.user_id is null,0,1),
    nvl(old.login_last_30d_count,0)+nvl(1d_ago.login_count,0)- nvl(30d_ago.login_count,0),
    nvl(old.login_last_30d_day_count,0)+if(1d_ago.user_id is null,0,1)- if(30d_ago.user_id is null,0,1),
    nvl(old.login_count,0)+nvl(1d_ago.login_count,0),
    nvl(old.login_day_count,0)+if(1d_ago.user_id is not null,1,0),
    if(old.order_date_first is null and 1d_ago.order_count>0, '$do_date', old.order_date_first),
    if(1d_ago.order_count>0,'$do_date',old.order_date_last),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_activity_count,0),
    nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(1d_ago.order_coupon_count,0),
    nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_last_7d_count,0)+nvl(1d_ago.order_count,0)- nvl(7d_ago.order_count,0),
    nvl(old.order_activity_last_7d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(7d_ago.order_activity_count,0),
    nvl(old.order_activity_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(7d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_last_7d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(7d_ago.order_coupon_count,0),
    nvl(old.order_coupon_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(7d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_7d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(7d_ago.order_original_amount,0.0),
    nvl(old.order_last_7d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(7d_ago.order_final_amount,0.0),
    nvl(old.order_last_30d_count,0)+nvl(1d_ago.order_count,0)- nvl(30d_ago.order_count,0),
    nvl(old.order_activity_last_30d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(30d_ago.order_activity_count,0),
    nvl(old.order_activity_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(30d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_last_30d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(30d_ago.order_coupon_count,0),
    nvl(old.order_coupon_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(30d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_30d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(30d_ago.order_original_amount,0.0),
    nvl(old.order_last_30d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(30d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_activity_count,0)+nvl(1d_ago.order_activity_count,0),
    nvl(old.order_activity_reduce_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_count,0)+nvl(1d_ago.order_coupon_count,0),
    nvl(old.order_coupon_reduce_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    if(old.payment_date_first is null and 1d_ago.payment_count>0, '$do_date', old.payment_date_first),
    if(1d_ago.payment_count>0,'$do_date',old.payment_date_last),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_last_7d_count,0)+nvl(1d_ago.payment_count,0)-nvl(7d_ago.payment_count,0),
    nvl(old.payment_last_7d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)-nvl(7d_ago.payment_amount,0.0),
    nvl(old.payment_last_30d_count,0)+nvl(1d_ago.payment_count,0)-nvl(30d_ago.payment_count,0),
    nvl(old.payment_last_30d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(30d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0),
    nvl(1d_ago.refund_order_count,0),
    nvl(1d_ago.refund_order_num,0),
    nvl(1d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_7d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(7d_ago.refund_order_count,0),
    nvl(old.refund_order_last_7d_num,0)+nvl(1d_ago.refund_order_num, 0)- nvl(7d_ago.refund_order_num,0),
    nvl(old.refund_order_last_7d_amount,0.0)+ nvl(1d_ago.refund_order_amount,0.0)- nvl(7d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_30d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(30d_ago.refund_order_count,0),
    nvl(old.refund_order_last_30d_num,0)+nvl(1d_ago.refund_order_num, 0)- nvl(30d_ago.refund_order_num,0),
    nvl(old.refund_order_last_30d_amount,0.0)+ nvl(1d_ago.refund_order_amount,0.0)- nvl(30d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_count,0)+nvl(1d_ago.refund_order_count,0),
    nvl(old.refund_order_num,0)+nvl(1d_ago.refund_order_num,0),
    nvl(old.refund_order_amount,0.0)+ nvl(1d_ago.refund_order_amount,0.0),
    nvl(1d_ago.refund_payment_count,0),
    nvl(1d_ago.refund_payment_num,0),
    nvl(1d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_7d_count,0)+nvl(1d_ago.refund_payment_count,0)-nvl(7d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_7d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(7d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_7d_amount,0.0)+ nvl(1d_ago.refund_payment_amount,0.0)- nvl(7d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_30d_count,0)+nvl(1d_ago.refund_payment_count,0)-nvl(30d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_30d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(30d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_30d_amount,0.0)+ nvl(1d_ago.refund_payment_amount,0.0)- nvl(30d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_count,0)+nvl(1d_ago.refund_payment_count,0),
    nvl(old.refund_payment_num,0)+nvl(1d_ago.refund_payment_num,0),
    nvl(old.refund_payment_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0),
    nvl(1d_ago.cart_count,0),
    nvl(old.cart_last_7d_count,0)+nvl(1d_ago.cart_count,0)-nvl(7d_ago.cart_count,0),
    nvl(old.cart_last_30d_count,0)+nvl(1d_ago.cart_count,0)-nvl(30d_ago.cart_count,0),
    nvl(old.cart_count,0)+nvl(1d_ago.cart_count,0),
    nvl(1d_ago.favor_count,0),
    nvl(old.favor_last_7d_count,0)+nvl(1d_ago.favor_count,0)- nvl(7d_ago.favor_count,0),
    nvl(old.favor_last_30d_count,0)+nvl(1d_ago.favor_count,0)- nvl(30d_ago.favor_count,0),
    nvl(old.favor_count,0)+nvl(1d_ago.favor_count,0),
    nvl(1d_ago.coupon_get_count,0),
    nvl(1d_ago.coupon_using_count,0),
    nvl(1d_ago.coupon_used_count,0),
    nvl(old.coupon_last_7d_get_count,0)+nvl(1d_ago.coupon_get_count,0)- nvl(7d_ago.coupon_get_count,0),
    nvl(old.coupon_last_7d_using_count,0)+nvl(1d_ago.coupon_using_count,0)- nvl(7d_ago.coupon_using_count,0),
    nvl(old.coupon_last_7d_used_count,0)+ nvl(1d_ago.coupon_used_count,0)- nvl(7d_ago.coupon_used_count,0),
    nvl(old.coupon_last_30d_get_count,0)+nvl(1d_ago.coupon_get_count,0)- nvl(30d_ago.coupon_get_count,0),
    nvl(old.coupon_last_30d_using_count,0)+nvl(1d_ago.coupon_using_count,0)- nvl(30d_ago.coupon_using_count,0),
    nvl(old.coupon_last_30d_used_count,0)+ nvl(1d_ago.coupon_used_count,0)- nvl(30d_ago.coupon_used_count,0),
    nvl(old.coupon_get_count,0)+nvl(1d_ago.coupon_get_count,0),
    nvl(old.coupon_using_count,0)+nvl(1d_ago.coupon_using_count,0),
    nvl(old.coupon_used_count,0)+nvl(1d_ago.coupon_used_count,0),
    nvl(1d_ago.appraise_good_count,0),
    nvl(1d_ago.appraise_mid_count,0),
    nvl(1d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_7d_default_count,0)+nvl(1d_ago.appraise_default_count,0)-nvl(7d_ago.appraise_default_count,0),
    nvl(old.appraise_last_7d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(7d_ago.appraise_good_count,0),
    nvl(old.appraise_last_7d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)-nvl(7d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_7d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)-nvl(7d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_7d_default_count,0)+nvl(1d_ago.appraise_default_count,0)-nvl(7d_ago.appraise_default_count,0),
    nvl(old.appraise_last_30d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(30d_ago.appraise_good_count,0),
    nvl(old.appraise_last_30d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)-nvl(30d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_30d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)-nvl(30d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_30d_default_count,0)+nvl(1d_ago.appraise_default_count,0)-nvl(30d_ago.appraise_default_count,0),
    nvl(old.appraise_good_count,0)+nvl(1d_ago.appraise_good_count,0),
    nvl(old.appraise_mid_count,0)+nvl(1d_ago.appraise_mid_count, 0),
    nvl(old.appraise_bad_count,0)+nvl(1d_ago.appraise_bad_count,0),
    nvl(old.appraise_default_count,0)+nvl(1d_ago.appraise_default_count,0)
from
(
    select
        user_id,
        login_date_first,
        login_date_last,
        login_date_1d_count,
        login_last_1d_day_count,
        login_last_7d_count,
        login_last_7d_day_count,
        login_last_30d_count,
        login_last_30d_day_count,
        login_count,
        login_day_count,
        order_date_first,
        order_date_last,
        order_last_1d_count,
        order_activity_last_1d_count,
        order_activity_reduce_last_1d_amount,
        order_coupon_last_1d_count,
        order_coupon_reduce_last_1d_amount,
        order_last_1d_original_amount,
        order_last_1d_final_amount,
        order_last_7d_count,
        order_activity_last_7d_count,
        order_activity_reduce_last_7d_amount,
        order_coupon_last_7d_count,
        order_coupon_reduce_last_7d_amount,
        order_last_7d_original_amount,
        order_last_7d_final_amount,
        order_last_30d_count,
        order_activity_last_30d_count,
        order_activity_reduce_last_30d_amount,
        order_coupon_last_30d_count,
        order_coupon_reduce_last_30d_amount,
        order_last_30d_original_amount,
        order_last_30d_final_amount,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_date_first,
        payment_date_last,
        payment_last_1d_count,
        payment_last_1d_amount,
        payment_last_7d_count,
        payment_last_7d_amount,
        payment_last_30d_count,
        payment_last_30d_amount,
        payment_count,
        payment_amount,
        refund_order_last_1d_count,
        refund_order_last_1d_num,
        refund_order_last_1d_amount,
        refund_order_last_7d_count,
        refund_order_last_7d_num,
        refund_order_last_7d_amount,
        refund_order_last_30d_count,
        refund_order_last_30d_num,
        refund_order_last_30d_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_last_1d_count,
        refund_payment_last_1d_num,
        refund_payment_last_1d_amount,
        refund_payment_last_7d_count,
        refund_payment_last_7d_num,
        refund_payment_last_7d_amount,
        refund_payment_last_30d_count,
        refund_payment_last_30d_num,
        refund_payment_last_30d_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_last_1d_count,
        cart_last_7d_count,
        cart_last_30d_count,
        cart_count,
        favor_last_1d_count,
        favor_last_7d_count,
        favor_last_30d_count,
        favor_count,
        coupon_last_1d_get_count,
        coupon_last_1d_using_count,
        coupon_last_1d_used_count,
        coupon_last_7d_get_count,
        coupon_last_7d_using_count,
        coupon_last_7d_used_count,
        coupon_last_30d_get_count,
        coupon_last_30d_using_count,
        coupon_last_30d_used_count,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_last_1d_good_count,
        appraise_last_1d_mid_count,
        appraise_last_1d_bad_count,
        appraise_last_1d_default_count,
        appraise_last_7d_good_count,
        appraise_last_7d_mid_count,
        appraise_last_7d_bad_count,
        appraise_last_7d_default_count,
        appraise_last_30d_good_count,
        appraise_last_30d_mid_count,
        appraise_last_30d_bad_count,
        appraise_last_30d_default_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP4}.dwt_user_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        user_id,
        login_count,
        cart_count,
        favor_count,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP3}.dws_user_action_daycount
    where dt='$do_date'
)1d_ago
on old.user_id=1d_ago.user_id
left join
(
    select
        user_id,
        login_count,
        cart_count,
        favor_count,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP3}.dws_user_action_daycount
    where dt=date_add('$do_date',-7)
)7d_ago
on old.user_id=7d_ago.user_id
left join
(
    select
        user_id,
        login_count,
        cart_count,
        favor_count,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP3}.dws_user_action_daycount
    where dt=date_add('$do_date',-30)
)30d_ago
on old.user_id=30d_ago.user_id;
alter table ${APP4}.dwt_user_topic drop partition(dt='$clear_date');
"

dwt_sku_topic="
insert overwrite table ${APP4}.dwt_sku_topic partition(dt='$do_date')
select
    nvl(1d_ago.sku_id,old.sku_id),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_num,0),
    nvl(1d_ago.order_activity_count,0),
    nvl(1d_ago.order_coupon_count,0),
    nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_last_7d_count,0)+nvl(1d_ago.order_count,0)- nvl(7d_ago.order_count,0),
    nvl(old.order_last_7d_num,0)+nvl(1d_ago.order_num,0)- nvl(7d_ago.order_num,0),
    nvl(old.order_activity_last_7d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(7d_ago.order_activity_count,0),
    nvl(old.order_coupon_last_7d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(7d_ago.order_coupon_count,0),
    nvl(old.order_activity_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(7d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(7d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_7d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(7d_ago.order_original_amount,0.0),
    nvl(old.order_last_7d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(7d_ago.order_final_amount,0.0),
    nvl(old.order_last_30d_count,0)+nvl(1d_ago.order_count,0)- nvl(30d_ago.order_count,0),
    nvl(old.order_last_30d_num,0)+nvl(1d_ago.order_num,0)- nvl(30d_ago.order_num,0),
    nvl(old.order_activity_last_30d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(30d_ago.order_activity_count,0),
    nvl(old.order_coupon_last_30d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(30d_ago.order_coupon_count,0),
    nvl(old.order_activity_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(30d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(30d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_30d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(30d_ago.order_original_amount,0.0),
    nvl(old.order_last_30d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(30d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_num,0)+nvl(1d_ago.order_num,0),
    nvl(old.order_activity_count,0)+nvl(1d_ago.order_activity_count,0),
    nvl(old.order_coupon_count,0)+nvl(1d_ago.order_coupon_count,0),
    nvl(old.order_activity_reduce_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_reduce_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_num,0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_last_7d_count,0)+nvl(1d_ago.payment_count,0)- nvl(7d_ago.payment_count,0),
    nvl(old.payment_last_7d_num,0)+nvl(1d_ago.payment_num,0)- nvl(7d_ago.payment_num,0),
    nvl(old.payment_last_7d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(7d_ago.payment_amount,0.0),
    nvl(old.payment_last_30d_count,0)+nvl(1d_ago.payment_count,0)- nvl(30d_ago.payment_count,0),
    nvl(old.payment_last_30d_num,0)+nvl(1d_ago.payment_num,0)- nvl(30d_ago.payment_num,0),
    nvl(old.payment_last_30d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(30d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_num,0)+nvl(1d_ago.payment_num,0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0),
    nvl(old.refund_order_last_1d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(1d_ago.refund_order_count,0),
    nvl(old.refund_order_last_1d_num,0)+nvl(1d_ago.refund_order_num,0)- nvl(1d_ago.refund_order_num,0),
    nvl(old.refund_order_last_1d_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0)- nvl(1d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_7d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(7d_ago.refund_order_count,0),
    nvl(old.refund_order_last_7d_num,0)+nvl(1d_ago.refund_order_num,0)- nvl(7d_ago.refund_order_num,0),
    nvl(old.refund_order_last_7d_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0)- nvl(7d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_30d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(30d_ago.refund_order_count,0),
    nvl(old.refund_order_last_30d_num,0)+nvl(1d_ago.refund_order_num,0)- nvl(30d_ago.refund_order_num,0),
    nvl(old.refund_order_last_30d_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0)- nvl(30d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_count,0)+nvl(1d_ago.refund_order_count,0),
    nvl(old.refund_order_num,0)+nvl(1d_ago.refund_order_num,0),
    nvl(old.refund_order_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0),
    nvl(1d_ago.refund_payment_count,0),
    nvl(1d_ago.refund_payment_num,0),
    nvl(1d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_7d_count,0)+nvl(1d_ago.refund_payment_count,0)- nvl(7d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_7d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(7d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_7d_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0)- nvl(7d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_30d_count,0)+nvl(1d_ago.refund_payment_count,0)- nvl(30d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_30d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(30d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_30d_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0)- nvl(30d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_count,0)+nvl(1d_ago.refund_payment_count,0),
    nvl(old.refund_payment_num,0)+nvl(1d_ago.refund_payment_num,0),
    nvl(old.refund_payment_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0),
    nvl(1d_ago.cart_count,0),
    nvl(old.cart_last_7d_count,0)+nvl(1d_ago.cart_count,0)- nvl(7d_ago.cart_count,0),
    nvl(old.cart_last_30d_count,0)+nvl(1d_ago.cart_count,0)- nvl(30d_ago.cart_count,0),
    nvl(old.cart_count,0)+nvl(1d_ago.cart_count,0),
    nvl(1d_ago.favor_count,0),
    nvl(old.favor_last_7d_count,0)+nvl(1d_ago.favor_count,0)- nvl(7d_ago.favor_count,0),
    nvl(old.favor_last_30d_count,0)+nvl(1d_ago.favor_count,0)- nvl(30d_ago.favor_count,0),
    nvl(old.favor_count,0)+nvl(1d_ago.favor_count,0),
    nvl(1d_ago.appraise_good_count,0),
    nvl(1d_ago.appraise_mid_count,0),
    nvl(1d_ago.appraise_bad_count,0),
    nvl(1d_ago.appraise_default_count,0),
    nvl(old.appraise_last_7d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(7d_ago.appraise_good_count,0),
    nvl(old.appraise_last_7d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)- nvl(7d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_7d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)- nvl(7d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_7d_default_count,0)+nvl(1d_ago.appraise_default_count,0)- nvl(7d_ago.appraise_default_count,0),
    nvl(old.appraise_last_30d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(30d_ago.appraise_good_count,0),
    nvl(old.appraise_last_30d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)- nvl(30d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_30d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)- nvl(30d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_30d_default_count,0)+nvl(1d_ago.appraise_default_count,0)- nvl(30d_ago.appraise_default_count,0),
    nvl(old.appraise_good_count,0)+nvl(1d_ago.appraise_good_count,0),
    nvl(old.appraise_mid_count,0)+nvl(1d_ago.appraise_mid_count,0),
    nvl(old.appraise_bad_count,0)+nvl(1d_ago.appraise_bad_count,0),
    nvl(old.appraise_default_count,0)+nvl(1d_ago.appraise_default_count,0)
from
(
    select
        sku_id,
        order_last_1d_count,
        order_last_1d_num,
        order_activity_last_1d_count,
        order_coupon_last_1d_count,
        order_activity_reduce_last_1d_amount,
        order_coupon_reduce_last_1d_amount,
        order_last_1d_original_amount,
        order_last_1d_final_amount,
        order_last_7d_count,
        order_last_7d_num,
        order_activity_last_7d_count,
        order_coupon_last_7d_count,
        order_activity_reduce_last_7d_amount,
        order_coupon_reduce_last_7d_amount,
        order_last_7d_original_amount,
        order_last_7d_final_amount,
        order_last_30d_count,
        order_last_30d_num,
        order_activity_last_30d_count,
        order_coupon_last_30d_count,
        order_activity_reduce_last_30d_amount,
        order_coupon_reduce_last_30d_amount,
        order_last_30d_original_amount,
        order_last_30d_final_amount,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_last_1d_count,
        payment_last_1d_num,
        payment_last_1d_amount,
        payment_last_7d_count,
        payment_last_7d_num,
        payment_last_7d_amount,
        payment_last_30d_count,
        payment_last_30d_num,
        payment_last_30d_amount,
        payment_count,
        payment_num,
        payment_amount,
        refund_order_last_1d_count,
        refund_order_last_1d_num,
        refund_order_last_1d_amount,
        refund_order_last_7d_count,
        refund_order_last_7d_num,
        refund_order_last_7d_amount,
        refund_order_last_30d_count,
        refund_order_last_30d_num,
        refund_order_last_30d_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_last_1d_count,
        refund_payment_last_1d_num,
        refund_payment_last_1d_amount,
        refund_payment_last_7d_count,
        refund_payment_last_7d_num,
        refund_payment_last_7d_amount,
        refund_payment_last_30d_count,
        refund_payment_last_30d_num,
        refund_payment_last_30d_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_last_1d_count,
        cart_last_7d_count,
        cart_last_30d_count,
        cart_count,
        favor_last_1d_count,
        favor_last_7d_count,
        favor_last_30d_count,
        favor_count,
        appraise_last_1d_good_count,
        appraise_last_1d_mid_count,
        appraise_last_1d_bad_count,
        appraise_last_1d_default_count,
        appraise_last_7d_good_count,
        appraise_last_7d_mid_count,
        appraise_last_7d_bad_count,
        appraise_last_7d_default_count,
        appraise_last_30d_good_count,
        appraise_last_30d_mid_count,
        appraise_last_30d_bad_count,
        appraise_last_30d_default_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP4}.dwt_sku_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        sku_id,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_num,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_count,
        favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP3}.dws_sku_action_daycount
    where dt='$do_date'
)1d_ago
on old.sku_id=1d_ago.sku_id
left join
(
    select
        sku_id,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_num,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_count,
        favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP3}.dws_sku_action_daycount
    where dt=date_add('$do_date',-7)
)7d_ago
on old.sku_id=7d_ago.sku_id
left join
(
    select
        sku_id,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_num,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_count,
        favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP3}.dws_sku_action_daycount
    where dt=date_add('$do_date',-30)
)30d_ago
on old.sku_id=30d_ago.sku_id;
alter table ${APP4}.dwt_sku_topic drop partition(dt='$clear_date');
"

dwt_activity_topic="
insert overwrite table ${APP4}.dwt_activity_topic partition(dt='$do_date')
select
    nvl(1d_ago.activity_rule_id,old.activity_rule_id),
    nvl(1d_ago.activity_id,old.activity_id),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_reduce_amount,0.0)+nvl(1d_ago.order_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_reduce_amount,0.0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_reduce_amount,0.0)+nvl(1d_ago.payment_reduce_amount,0.0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0)
from
(
    select
        activity_rule_id,
        activity_id,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount
    from ${APP4}.dwt_activity_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        activity_rule_id,
        activity_id,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount
    from ${APP3}.dws_activity_info_daycount
    where dt='$do_date'
)1d_ago
on old.activity_rule_id=1d_ago.activity_rule_id;
alter table ${APP4}.dwt_activity_topic drop partition(dt='$clear_date');
"

dwt_coupon_topic="
insert overwrite table ${APP4}.dwt_coupon_topic partition(dt='$do_date')
select
    nvl(1d_ago.coupon_id,old.coupon_id),
    nvl(1d_ago.get_count,0),
    nvl(old.get_last_7d_count,0)+nvl(1d_ago.get_count,0)- nvl(7d_ago.get_count,0),
    nvl(old.get_last_30d_count,0)+nvl(1d_ago.get_count,0)- nvl(30d_ago.get_count,0),
    nvl(old.get_count,0)+nvl(1d_ago.get_count,0),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_last_7d_count,0)+nvl(1d_ago.order_count,0)- nvl(7d_ago.order_count,0),
    nvl(old.order_last_7d_reduce_amount,0.0)+nvl(1d_ago.order_reduce_amount,0.0)- nvl(7d_ago.order_reduce_amount,0.0),
    nvl(old.order_last_7d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(7d_ago.order_original_amount,0.0),
    nvl(old.order_last_7d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(7d_ago.order_final_amount,0.0),
    nvl(old.order_last_30d_count,0)+nvl(1d_ago.order_count,0)- nvl(30d_ago.order_count,0),
    nvl(old.order_last_30d_reduce_amount,0.0)+nvl(1d_ago.order_reduce_amount,0.0)- nvl(30d_ago.order_reduce_amount,0.0),
    nvl(old.order_last_30d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(30d_ago.order_original_amount,0.0),
    nvl(old.order_last_30d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(30d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_reduce_amount,0.0)+nvl(1d_ago.order_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    nvl(old.payment_last_1d_count,0)+nvl(1d_ago.payment_count,0)- nvl(1d_ago.payment_count,0),
    nvl(old.payment_last_1d_reduce_amount,0.0)+nvl(1d_ago.payment_reduce_amount,0.0)- nvl(1d_ago.payment_reduce_amount,0.0),
    nvl(old.payment_last_1d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_last_7d_count,0)+nvl(1d_ago.payment_count,0)- nvl(7d_ago.payment_count,0),
    nvl(old.payment_last_7d_reduce_amount,0.0)+nvl(1d_ago.payment_reduce_amount,0.0)- nvl(7d_ago.payment_reduce_amount,0.0),
    nvl(old.payment_last_7d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(7d_ago.payment_amount,0.0),
    nvl(old.payment_last_30d_count,0)+nvl(1d_ago.payment_count,0)- nvl(30d_ago.payment_count,0),
    nvl(old.payment_last_30d_reduce_amount,0.0)+nvl(1d_ago.payment_reduce_amount,0.0)- nvl(30d_ago.payment_reduce_amount,0.0),
    nvl(old.payment_last_30d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(30d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_reduce_amount,0.0)+nvl(1d_ago.payment_reduce_amount,0.0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0),
    nvl(1d_ago.expire_count,0),
    nvl(old.expire_last_7d_count,0)+nvl(1d_ago.expire_count,0)- nvl(7d_ago.expire_count,0),
    nvl(old.expire_last_30d_count,0)+nvl(1d_ago.expire_count,0)- nvl(30d_ago.expire_count,0),
    nvl(old.expire_count,0)+nvl(1d_ago.expire_count,0)
from
(
    select
        coupon_id,
        get_last_1d_count,
        get_last_7d_count,
        get_last_30d_count,
        get_count,
        order_last_1d_count,
        order_last_1d_reduce_amount,
        order_last_1d_original_amount,
        order_last_1d_final_amount,
        order_last_7d_count,
        order_last_7d_reduce_amount,
        order_last_7d_original_amount,
        order_last_7d_final_amount,
        order_last_30d_count,
        order_last_30d_reduce_amount,
        order_last_30d_original_amount,
        order_last_30d_final_amount,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_last_1d_count,
        payment_last_1d_reduce_amount,
        payment_last_1d_amount,
        payment_last_7d_count,
        payment_last_7d_reduce_amount,
        payment_last_7d_amount,
        payment_last_30d_count,
        payment_last_30d_reduce_amount,
        payment_last_30d_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount,
        expire_last_1d_count,
        expire_last_7d_count,
        expire_last_30d_count,
        expire_count
    from ${APP4}.dwt_coupon_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        coupon_id,
        get_count,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount,
        expire_count
    from ${APP3}.dws_coupon_info_daycount
    where dt='$do_date'
)1d_ago
on old.coupon_id=1d_ago.coupon_id
left join
(
    select
        coupon_id,
        get_count,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount,
        expire_count
    from ${APP3}.dws_coupon_info_daycount
    where dt=date_add('$do_date',-7)
)7d_ago
on old.coupon_id=7d_ago.coupon_id
left join
(
    select
        coupon_id,
        get_count,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount,
        expire_count
    from ${APP3}.dws_coupon_info_daycount
    where dt=date_add('$do_date',-30)
)30d_ago
on old.coupon_id=30d_ago.coupon_id;
alter table ${APP4}.dwt_coupon_topic drop partition(dt='$clear_date');
"

dwt_area_topic="
insert overwrite table ${APP4}.dwt_area_topic partition(dt='$do_date')
select
    nvl(old.province_id, 1d_ago.province_id),
    nvl(1d_ago.visit_count,0),
    nvl(1d_ago.login_count,0),
    nvl(old.visit_last_7d_count,0)+nvl(1d_ago.visit_count,0)- nvl(7d_ago.visit_count,0),
    nvl(old.login_last_7d_count,0)+nvl(1d_ago.login_count,0)- nvl(7d_ago.login_count,0),
    nvl(old.visit_last_30d_count,0)+nvl(1d_ago.visit_count,0)- nvl(30d_ago.visit_count,0),
    nvl(old.login_last_30d_count,0)+nvl(1d_ago.login_count,0)- nvl(30d_ago.login_count,0),
    nvl(old.visit_count,0)+nvl(1d_ago.visit_count,0),
    nvl(old.login_count,0)+nvl(1d_ago.login_count,0),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_last_7d_count,0)+nvl(1d_ago.order_count,0)- nvl(7d_ago.order_count,0),
    nvl(old.order_last_7d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(7d_ago.order_original_amount,0.0),
    nvl(old.order_last_7d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(7d_ago.order_final_amount,0.0),
    nvl(old.order_last_30d_count,0)+nvl(1d_ago.order_count,0)- nvl(30d_ago.order_count,0),
    nvl(old.order_last_30d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(30d_ago.order_original_amount,0.0),
    nvl(old.order_last_30d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(30d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_last_7d_count,0)+nvl(1d_ago.payment_count,0)- nvl(7d_ago.payment_count,0),
    nvl(old.payment_last_7d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(7d_ago.payment_amount,0.0),
    nvl(old.payment_last_30d_count,0)+nvl(1d_ago.payment_count,0)- nvl(30d_ago.payment_count,0),
    nvl(old.payment_last_30d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(30d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0),
    nvl(1d_ago.refund_order_count,0),
    nvl(1d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_7d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(7d_ago.refund_order_count,0),
    nvl(old.refund_order_last_7d_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0)- nvl(7d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_30d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(30d_ago.refund_order_count,0),
    nvl(old.refund_order_last_30d_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0)- nvl(30d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_count,0)+nvl(1d_ago.refund_order_count,0),
    nvl(old.refund_order_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0),
    nvl(1d_ago.refund_payment_count,0),
    nvl(1d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_7d_count,0)+nvl(1d_ago.refund_payment_count,0)- nvl(7d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_7d_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0)- nvl(7d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_30d_count,0)+nvl(1d_ago.refund_payment_count,0)- nvl(30d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_30d_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0)- nvl(30d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_count,0)+nvl(1d_ago.refund_payment_count,0),
    nvl(old.refund_payment_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0)

from
(
    select
        province_id,
        visit_last_1d_count,
        login_last_1d_count,
        visit_last_7d_count,
        login_last_7d_count,
        visit_last_30d_count,
        login_last_30d_count,
        visit_count,
        login_count,
        order_last_1d_count,
        order_last_1d_original_amount,
        order_last_1d_final_amount,
        order_last_7d_count,
        order_last_7d_original_amount,
        order_last_7d_final_amount,
        order_last_30d_count,
        order_last_30d_original_amount,
        order_last_30d_final_amount,
        order_count,
        order_original_amount,
        order_final_amount,
        payment_last_1d_count,
        payment_last_1d_amount,
        payment_last_7d_count,
        payment_last_7d_amount,
        payment_last_30d_count,
        payment_last_30d_amount,
        payment_count,
        payment_amount,
        refund_order_last_1d_count,
        refund_order_last_1d_amount,
        refund_order_last_7d_count,
        refund_order_last_7d_amount,
        refund_order_last_30d_count,
        refund_order_last_30d_amount,
        refund_order_count,
        refund_order_amount,
        refund_payment_last_1d_count,
        refund_payment_last_1d_amount,
        refund_payment_last_7d_count,
        refund_payment_last_7d_amount,
        refund_payment_last_30d_count,
        refund_payment_last_30d_amount,
        refund_payment_count,
        refund_payment_amount
    from ${APP4}.dwt_area_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        province_id,
        visit_count,
        login_count,
        order_count,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_amount,
        refund_payment_count,
        refund_payment_amount
    from ${APP3}.dws_area_stats_daycount
    where dt='$do_date'
)1d_ago
on old.province_id=1d_ago.province_id
left join
(
    select
        province_id,
        visit_count,
        login_count,
        order_count,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_amount,
        refund_payment_count,
        refund_payment_amount
    from ${APP3}.dws_area_stats_daycount
    where dt=date_add('$do_date',-7)
)7d_ago
on old.province_id= 7d_ago.province_id
left join
(
    select
        province_id,
        visit_count,
        login_count,
        order_count,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_amount,
        refund_payment_count,
        refund_payment_amount
    from ${APP3}.dws_area_stats_daycount
    where dt=date_add('$do_date',-30)
)30d_ago
on old.province_id= 30d_ago.province_id;
alter table ${APP4}.dwt_area_topic drop partition(dt='$clear_date');
"


case $1 in
    "dwt_visitor_topic" )
        hive -e "$dwt_visitor_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_visitor_topic/dt=$clear_date
    ;;
    "dwt_user_topic" )
        hive -e "$dwt_user_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_user_topic/dt=$clear_date
    ;;
    "dwt_sku_topic" )
        hive -e "$dwt_sku_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_sku_topic/dt=$clear_date
    ;;
    "dwt_activity_topic" )
        hive -e "$dwt_activity_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_activity_topic/dt=$clear_date
    ;;
    "dwt_coupon_topic" )
        hive -e "$dwt_coupon_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_coupon_topic/dt=$clear_date
    ;;
    "dwt_area_topic" )
        hive -e "$dwt_area_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_area_topic/dt=$clear_date
    ;;
    "all" )
        hive -e "$dwt_visitor_topic$dwt_user_topic$dwt_sku_topic$dwt_activity_topic$dwt_coupon_topic$dwt_area_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_visitor_topic/dt=$clear_date
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_user_topic/dt=$clear_date
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_sku_topic/dt=$clear_date
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_activity_topic/dt=$clear_date
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_coupon_topic/dt=$clear_date
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_area_topic/dt=$clear_date
    ;;
esac
```



# 数据仓库中表的相关属性



## 1.1 电商系统表结构 (ods层数据构建)

### 1.1.1 活动信息表（activity_info）

| **字段名**        | **字段说明**                 |
| ----------------- | ---------------------------- |
| **id**            | 活动id                       |
| **activity_name** | 活动名称                     |
| **activity_type** | 活动类型（1：满减，2：折扣） |
| **activity_desc** | 活动描述                     |
| **start_time**    | 开始时间                     |
| **end_time**      | 结束时间                     |
| **create_time**   | 创建时间                     |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_activity_info;
CREATE EXTERNAL TABLE ods_activity_info(
    `id` STRING COMMENT '编号',
    `activity_name` STRING  COMMENT '活动名称',
    `activity_type` STRING  COMMENT '活动类型',
    `start_time` STRING  COMMENT '开始时间',
    `end_time` STRING  COMMENT '结束时间',
    `create_time` STRING  COMMENT '创建时间'
) COMMENT '活动信息表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_activity_info/';
```



### 1.1.2 活动规则表（activity_rule）

| **id**               | **编号** |
| -------------------- | -------- |
| **activity_id**      | 活动ID   |
| **activity_type**    | 活动类型 |
| **condition_amount** | 满减金额 |
| **condition_num**    | 满减件数 |
| **benefit_amount**   | 优惠金额 |
| **benefit_discount** | 优惠折扣 |
| **benefit_level**    | 优惠级别 |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_activity_rule;
CREATE EXTERNAL TABLE ods_activity_rule(
    `id` STRING COMMENT '编号',
    `activity_id` STRING  COMMENT '活动ID',
    `activity_type` STRING COMMENT '活动类型',
    `condition_amount` DECIMAL(16,2) COMMENT '满减金额',
    `condition_num` BIGINT COMMENT '满减件数',
    `benefit_amount` DECIMAL(16,2) COMMENT '优惠金额',
    `benefit_discount` DECIMAL(16,2) COMMENT '优惠折扣',
    `benefit_level` STRING COMMENT '优惠级别'
) COMMENT '活动规则表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'



LOCATION '/warehouse/gmall/ods/ods_activity_rule/';
```



### 1.1.3 活动商品关联表（activity_sku）

| **字段名**      | **字段说明** |
| --------------- | ------------ |
| **id**          | 编号         |
| **activity_id** | 活动id       |
| **sku_id**      | sku_id       |
| **create_time** | 创建时间     |



### 1.1.4 平台属性表（base_attr_info）

| **字段名**         | **字段说明** |
| ------------------ | ------------ |
| **id**             | 编号         |
| **attr_name**      | 属性名称     |
| **category_id**    | 分类id       |
| **category_level** | 分类层级     |

### 1.1.5 平台属性值表（base_attr_value）

| **字段名**     | **字段说明** |
| -------------- | ------------ |
| **id**         | 编号         |
| **value_name** | 属性值名称   |
| **attr_id**    | 属性id       |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_sku_attr_value;
CREATE EXTERNAL TABLE ods_sku_attr_value(
    `id` STRING COMMENT '编号',
    `attr_id` STRING COMMENT '平台属性ID',
    `value_id` STRING COMMENT '平台属性值ID',
    `sku_id` STRING COMMENT '商品ID',
    `attr_name` STRING COMMENT '平台属性名称',
    `value_name` STRING COMMENT '平台属性值名称'
) COMMENT 'sku平台属性表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_sku_attr_value/';
```



### 1.1.6 一级分类表（base_category1）

| **字段名** | **字段说明** |
| ---------- | ------------ |
| **id**     | 编号         |
| **name**   | 分类名称     |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_base_category1;
CREATE EXTERNAL TABLE ods_base_category1(
    `id` STRING COMMENT 'id',
    `name` STRING COMMENT '名称'
) COMMENT '商品一级分类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_base_category1/';
```



### 1.1.7 二级分类表（base_category2）

| **字段名**       | **字段说明** |
| ---------------- | ------------ |
| **id**           | 编号         |
| **name**         | 二级分类名称 |
| **category1_id** | 一级分类编号 |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_base_category2;
CREATE EXTERNAL TABLE ods_base_category2(
    `id` STRING COMMENT ' id',
    `name` STRING COMMENT '名称',
    `category1_id` STRING COMMENT '一级品类id'
) COMMENT '商品二级分类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_base_category2/';
```



### 1.1.8 三级分类表（base_category3）

| **字段名**       | **字段说明** |
| ---------------- | ------------ |
| **id**           | 编号         |
| **name**         | 三级分类名称 |
| **category2_id** | 二级分类编号 |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_base_category3;
CREATE EXTERNAL TABLE ods_base_category3(
    `id` STRING COMMENT ' id',
    `name` STRING COMMENT '名称',
    `category2_id` STRING COMMENT '二级品类id'
) COMMENT '商品三级分类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_base_category3/';
```



### 1.1.9 字典表（base_dic）

| **字段名**       | **字段说明** |
| ---------------- | ------------ |
| **dic_code**     | 编号         |
| **dic_name**     | 编码名称     |
| **parent_code**  | 父编号       |
| **create_time**  | 创建日期     |
| **operate_time** | 修改日期     |

**建表语句：**

```
ROP TABLE IF EXISTS ods_base_dic;
CREATE EXTERNAL TABLE ods_base_dic(
    `dic_code` STRING COMMENT '编号',
    `dic_name` STRING COMMENT '编码名称',
    `parent_code` STRING COMMENT '父编码',
    `create_time` STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '操作日期'
) COMMENT '编码字典表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_base_dic/';
```



### 1.1.10 省份表（base_province）

| **字段名**     | **字段说明** |
| -------------- | ------------ |
| **id**         | id           |
| **name**       | 省名称       |
| **region_id**  | 大区id       |
| **area_code**  | 行政区位码   |
| **iso_code**   | 国际编码     |
| **iso_3166_2** | ISO3166编码  |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_base_province;
CREATE EXTERNAL TABLE ods_base_province (
    `id` STRING COMMENT '编号',
    `name` STRING COMMENT '省份名称',
    `region_id` STRING COMMENT '地区ID',
    `area_code` STRING COMMENT '地区编码',
    `iso_code` STRING COMMENT 'ISO-3166编码，供可视化使用',
    `iso_3166_2` STRING COMMENT 'IOS-3166-2编码，供可视化使用'
)  COMMENT '省份表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_base_province/';
```



### 1.1.11 地区表（base_region）

| **字段名**      | **字段说明** |
| --------------- | ------------ |
| **id**          | 大区id       |
| **region_name** | 大区名称     |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_base_region;
CREATE EXTERNAL TABLE ods_base_region (
    `id` STRING COMMENT '编号',
    `region_name` STRING COMMENT '地区名称'
)  COMMENT '地区表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_base_region/';
```



### 1.1.12 品牌表（base_trademark）

| **字段名**   | **字段说明**       |
| ------------ | ------------------ |
| **id**       | 编号               |
| **tm_name**  | 属性值             |
| **logo_url** | 品牌logo的图片路径 |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_base_trademark;
CREATE EXTERNAL TABLE ods_base_trademark (
    `id` STRING COMMENT '编号',
    `tm_name` STRING COMMENT '品牌名称'
)  COMMENT '品牌表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_base_trademark/';
```



### 1.1.13 购物车表（cart_info）

| **字段名**       | **字段说明**     |
| ---------------- | ---------------- |
| **id**           | 编号             |
| **user_id**      | 用户id           |
| **sku_id**       | skuid            |
| **cart_price**   | 放入购物车时价格 |
| **sku_num**      | 数量             |
| **img_url**      | 图片文件         |
| **sku_name**     | sku名称 (冗余)   |
| **is_checked**   | 是否已经下单     |
| **create_time**  | 创建时间         |
| **operate_time** | 修改时间         |
| **is_ordered**   | 是否已经下单     |
| **order_time**   | 下单时间         |
| **source_type**  | 来源类型         |
| **source_id**    | 来源编号         |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_cart_info;
CREATE EXTERNAL TABLE ods_cart_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户id',
    `sku_id` STRING COMMENT 'skuid',
    `cart_price` DECIMAL(16,2)  COMMENT '放入购物车时价格',
    `sku_num` BIGINT COMMENT '数量',
    `sku_name` STRING COMMENT 'sku名称 (冗余)',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `is_ordered` STRING COMMENT '是否已经下单',
    `order_time` STRING COMMENT '下单时间',
    `source_type` STRING COMMENT '来源类型',
    `source_id` STRING COMMENT '来源编号'
) COMMENT '加购表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_cart_info/';
```



### 1.1.14 评价表（comment_info）

| **字段名**       | **字段说明**              |
| ---------------- | ------------------------- |
| **id**           | 编号                      |
| **user_id**      | 用户id                    |
| **nick_name**    | 用户昵称                  |
| **head_img**     | 图片                      |
| **sku_id**       | 商品sku_id                |
| **spu_id**       | 商品spu_id                |
| **order_id**     | 订单编号                  |
| **appraise**     | 评价 1 好评 2 中评 3 差评 |
| **comment_txt**  | 评价内容                  |
| **create_time**  | 创建时间                  |
| **operate_time** | 修改时间                  |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_comment_info;
CREATE EXTERNAL TABLE ods_comment_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `sku_id` STRING COMMENT '商品sku',
    `spu_id` STRING COMMENT '商品spu',
    `order_id` STRING COMMENT '订单ID',
    `appraise` STRING COMMENT '评价',
    `create_time` STRING COMMENT '评价时间'
) COMMENT '商品评论表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_comment_info/';
```



### 1.1.15 优惠券信息表（coupon_info）

| **字段名**           | **字段说明**                                        |
| -------------------- | --------------------------------------------------- |
| **id**               | 购物券编号                                          |
| **coupon_name**      | 购物券名称                                          |
| **coupon_type**      | 购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券  |
| **condition_amount** | 满额数（3）                                         |
| **condition_num**    | 满件数（4）                                         |
| **activity_id**      | 活动编号                                            |
| **benefit_amount**   | 减金额（1 3）                                       |
| **benefit_discount** | 折扣（2 4）                                         |
| **create_time**      | 创建时间                                            |
| **range_type**       | 范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌 |
| **limit_num**        | 最多领用次数                                        |
| **taken_count**      | 已领用次数                                          |
| **start_time**       | 可以领取的开始日期                                  |
| **end_time**         | 可以领取的结束日期                                  |
| **operate_time**     | 修改时间                                            |
| **expire_time**      | 过期时间                                            |
| **range_desc**       | 范围描述                                            |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_coupon_info;
CREATE EXTERNAL TABLE ods_coupon_info(
    `id` STRING COMMENT '购物券编号',
    `coupon_name` STRING COMMENT '购物券名称',
    `coupon_type` STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` DECIMAL(16,2) COMMENT '满额数',
    `condition_num` BIGINT COMMENT '满件数',
    `activity_id` STRING COMMENT '活动编号',
    `benefit_amount` DECIMAL(16,2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16,2) COMMENT '折扣',
    `create_time` STRING COMMENT '创建时间',
    `range_type` STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `limit_num` BIGINT COMMENT '最多领用次数',
    `taken_count` BIGINT COMMENT '已领用次数',
    `start_time` STRING COMMENT '开始领取时间',
    `end_time` STRING COMMENT '结束领取时间',
    `operate_time` STRING COMMENT '修改时间',
    `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_coupon_info/';
```



### 1.1.16 优惠券优惠范围表（coupon_range）

| **字段名**     | **字段说明**                                        |
| -------------- | --------------------------------------------------- |
| **id**         | 购物券编号                                          |
| **coupon_id**  | 优惠券id                                            |
| **range_type** | 范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌 |
| **range_id**   | 范围id                                              |

### 1.1.17 优惠券领用表（coupon_use）

| **字段名**        | **字段说明**                      |
| ----------------- | --------------------------------- |
| **id**            | 编号                              |
| **coupon_id**     | 购物券ID                          |
| **user_id**       | 用户ID                            |
| **order_id**      | 订单ID                            |
| **coupon_status** | 购物券状态（1：未使用 2：已使用） |
| **get_time**      | 获取时间                          |
| **using_time**    | 使用时间                          |
| **used_time**     | 支付时间                          |
| **expire_time**   | 过期时间                          |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_coupon_use;
CREATE EXTERNAL TABLE ods_coupon_use(
    `id` STRING COMMENT '编号',
    `coupon_id` STRING  COMMENT '优惠券ID',
    `user_id` STRING  COMMENT 'skuid',
    `order_id` STRING  COMMENT 'spuid',
    `coupon_status` STRING  COMMENT '优惠券状态',
    `get_time` STRING  COMMENT '领取时间',
    `using_time` STRING  COMMENT '使用时间(下单)',
    `used_time` STRING  COMMENT '使用时间(支付)',
    `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券领用表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_coupon_use/';
```



### 1.1.18 收藏表（favor_info）

| **字段名**      | **字段说明**               |
| --------------- | -------------------------- |
| **id**          | 编号                       |
| **user_id**     | 用户id                     |
| **sku_id**      | skuid                      |
| **spu_id**      | 商品id                     |
| **is_cancel**   | 是否已取消 0 正常 1 已取消 |
| **create_time** | 创建时间                   |
| **cancel_time** | 修改时间                   |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_favor_info;
CREATE EXTERNAL TABLE ods_favor_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户id',
    `sku_id` STRING COMMENT 'skuid',
    `spu_id` STRING COMMENT 'spuid',
    `is_cancel` STRING COMMENT '是否取消',
    `create_time` STRING COMMENT '收藏时间',
    `cancel_time` STRING COMMENT '取消时间'
) COMMENT '商品收藏表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_favor_info/';
```



### 1.1.19 订单明细表（order_detail）

| **字段名**                | **字段说明**             |
| ------------------------- | ------------------------ |
| **id**                    | 编号                     |
| **order_id**              | 订单编号                 |
| **sku_id**                | sku_id                   |
| **sku_name**              | sku名称（冗余)           |
| **img_url**               | 图片名称（冗余)          |
| **order_price**           | 购买价格(下单时sku价格） |
| **sku_num**               | 购买个数                 |
| **create_time**           | 创建时间                 |
| **source_type**           | 来源类型                 |
| **source_id**             | 来源编号                 |
| **split_total_amount**    | 分摊总金额               |
| **split_activity_amount** | 分摊活动减免金额         |
| **split_coupon_amount**   | 分摊优惠券减免金额       |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_order_detail;
CREATE EXTERNAL TABLE ods_order_detail(
    `id` STRING COMMENT '编号',
    `order_id` STRING  COMMENT '订单号',
    `sku_id` STRING COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
    `order_price` DECIMAL(16,2) COMMENT '商品价格',
    `sku_num` BIGINT COMMENT '商品数量',
    `create_time` STRING COMMENT '创建时间',
    `source_type` STRING COMMENT '来源类型',
    `source_id` STRING COMMENT '来源编号',
    `split_final_amount` DECIMAL(16,2) COMMENT '分摊最终金额',
    `split_activity_amount` DECIMAL(16,2) COMMENT '分摊活动优惠',
    `split_coupon_amount` DECIMAL(16,2) COMMENT '分摊优惠券优惠'
) COMMENT '订单详情表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_order_detail/';
```



### 1.1.20 订单明细活动关联表（order_detail_activity）

| **字段名**           | **字段说明** |
| -------------------- | ------------ |
| **id**               | 编号         |
| **order_id**         | 订单id       |
| **order_detail_id**  | 订单明细id   |
| **activity_id**      | 活动ID       |
| **activity_rule_id** | 活动规则     |
| **sku_id**           | skuID        |
| **create_time**      | 获取时间     |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_order_detail_activity;
CREATE EXTERNAL TABLE ods_order_detail_activity(
    `id` STRING COMMENT '编号',
    `order_id` STRING  COMMENT '订单号',
    `order_detail_id` STRING COMMENT '订单明细id',
    `activity_id` STRING COMMENT '活动id',
    `activity_rule_id` STRING COMMENT '活动规则id',
    `sku_id` BIGINT COMMENT '商品id',
    `create_time` STRING COMMENT '创建时间'
) COMMENT '订单详情活动关联表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_order_detail_activity/';
```



### 1.1.21 订单明细优惠券关联表（order_detail_coupon）

| **字段名**          | **字段说明** |
| ------------------- | ------------ |
| **id**              | 编号         |
| **order_id**        | 订单id       |
| **order_detail_id** | 订单明细id   |
| **coupon_id**       | 购物券ID     |
| **coupon_use_id**   | 购物券领用id |
| **sku_id**          | skuID        |
| **create_time**     | 获取时间     |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_order_detail_coupon;
CREATE EXTERNAL TABLE ods_order_detail_coupon(
    `id` STRING COMMENT '编号',
    `order_id` STRING  COMMENT '订单号',
    `order_detail_id` STRING COMMENT '订单明细id',
    `coupon_id` STRING COMMENT '优惠券id',
    `coupon_use_id` STRING COMMENT '优惠券领用记录id',
    `sku_id` STRING COMMENT '商品id',
    `create_time` STRING COMMENT '创建时间'
) COMMENT '订单明细优惠券关联表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_order_detail_coupon/';
```



### 1.1.22 订单表(order_info）

| **字段名**                 | **字段说明**                |
| -------------------------- | --------------------------- |
| **id**                     | 编号                        |
| **consignee**              | 收货人                      |
| **consignee_tel**          | 收件人电话                  |
| **total_amount**           | 总金额                      |
| **order_status**           | 订单状态                    |
| **user_id**                | 用户id                      |
| **payment_way**            | 付款方式                    |
| **delivery_address**       | 送货地址                    |
| **order_comment**          | 订单备注                    |
| **out_trade_no**           | 订单交易编号（第三方支付用) |
| **trade_body**             | 订单描述(第三方支付用)      |
| **create_time**            | 创建时间                    |
| **operate_time**           | 操作时间                    |
| **expire_time**            | 失效时间                    |
| **process_status**         | 进度状态                    |
| **tracking_no**            | 物流单编号                  |
| **parent_order_id**        | 父订单编号                  |
| **img_url**                | 图片路径                    |
| **province_id**            | 地区                        |
| **activity_reduce_amount** | 促销金额                    |
| **coupon_reduce_amount**   | 优惠金额                    |
| **original_total_amount**  | 原价金额                    |
| **feight_fee**             | 运费                        |
| **feight_fee_reduce**      | 运费减免                    |
| **refundable_time**        | 可退款日期（签收后30天）    |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_order_info;
CREATE EXTERNAL TABLE ods_order_info (
    `id` STRING COMMENT '订单号',
    `final_amount` DECIMAL(16,2) COMMENT '订单最终金额',
    `order_status` STRING COMMENT '订单状态',
    `user_id` STRING COMMENT '用户id',
    `payment_way` STRING COMMENT '支付方式',
    `delivery_address` STRING COMMENT '送货地址',
    `out_trade_no` STRING COMMENT '支付流水号',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `expire_time` STRING COMMENT '过期时间',
    `tracking_no` STRING COMMENT '物流单编号',
    `province_id` STRING COMMENT '省份ID',
    `activity_reduce_amount` DECIMAL(16,2) COMMENT '活动减免金额',
    `coupon_reduce_amount` DECIMAL(16,2) COMMENT '优惠券减免金额',
    `original_amount` DECIMAL(16,2)  COMMENT '订单原价金额',
    `feight_fee` DECIMAL(16,2)  COMMENT '运费',
    `feight_fee_reduce` DECIMAL(16,2)  COMMENT '运费减免'
) COMMENT '订单表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_order_info/';
```



### 1.1.23 退单表（order_refund_info）

| **字段名**             | **字段说明**                    |
| ---------------------- | ------------------------------- |
| **id**                 | 编号                            |
| **user_id**            | 用户id                          |
| **order_id**           | 订单id                          |
| **sku_id**             | skuid                           |
| **refund_type**        | 退款类型                        |
| **refund_num**         | 退货件数                        |
| **refund_amount**      | 退款金额                        |
| **refund_reason_type** | 原因类型                        |
| **refund_reason_txt**  | 原因内容                        |
| **refund_status**      | 退款状态（0：待审批 1：已退款） |
| **create_time**        | 创建时间                        |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_order_refund_info;
CREATE EXTERNAL TABLE ods_order_refund_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `order_id` STRING COMMENT '订单ID',
    `sku_id` STRING COMMENT '商品ID',
    `refund_type` STRING COMMENT '退单类型',
    `refund_num` BIGINT COMMENT '退单件数',
    `refund_amount` DECIMAL(16,2) COMMENT '退单金额',
    `refund_reason_type` STRING COMMENT '退单原因类型',
    `refund_status` STRING COMMENT '退单状态',--退单状态应包含买家申请、卖家审核、卖家收货、退款完成等状态。此处未涉及到，故该表按增量处理
    `create_time` STRING COMMENT '退单时间'
) COMMENT '退单表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_order_refund_info/';
```



### 1.1.24 订单状态流水表（order_status_log）

| **字段名**       | **字段说明** |
| ---------------- | ------------ |
| **id**           | 编号         |
| **order_id**     | 订单编号     |
| **order_status** | 订单状态     |
| **operate_time** | 操作时间     |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_order_status_log;
CREATE EXTERNAL TABLE ods_order_status_log (
    `id` STRING COMMENT '编号',
    `order_id` STRING COMMENT '订单ID',
    `order_status` STRING COMMENT '订单状态',
    `operate_time` STRING COMMENT '修改时间'
)  COMMENT '订单状态表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_order_status_log/';
```



### 1.1.25 支付表（payment_info）

| **字段名**           | **字段说明**            |
| -------------------- | ----------------------- |
| **id**               | 编号                    |
| **out_trade_no**     | 对外业务编号            |
| **order_id**         | 订单编号                |
| **user_id**          |                         |
| **payment_type**     | 支付类型（微信 支付宝） |
| **trade_no**         | 交易编号                |
| **total_amount**     | 支付金额                |
| **subject**          | 交易内容                |
| **payment_status**   | 支付状态                |
| **create_time**      | 创建时间                |
| **callback_time**    | 回调时间                |
| **callback_content** | 回调信息                |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_payment_info;
CREATE EXTERNAL TABLE ods_payment_info(
    `id` STRING COMMENT '编号',
    `out_trade_no` STRING COMMENT '对外业务编号',
    `order_id` STRING COMMENT '订单编号',
    `user_id` STRING COMMENT '用户编号',
    `payment_type` STRING COMMENT '支付类型',
    `trade_no` STRING COMMENT '交易编号',
    `payment_amount` DECIMAL(16,2) COMMENT '支付金额',
    `subject` STRING COMMENT '交易内容',
    `payment_status` STRING COMMENT '支付状态',
    `create_time` STRING COMMENT '创建时间',
    `callback_time` STRING COMMENT '回调时间'
)  COMMENT '支付流水表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_payment_info/';
```



### 1.1.26 退款表（refund_payment）

| **字段名**           | **字段说明**            |
| -------------------- | ----------------------- |
| **id**               | 编号                    |
| **out_trade_no**     | 对外业务编号            |
| **order_id**         | 订单编号                |
| **sku_id**           | 商品sku_id              |
| **payment_type**     | 支付类型（微信 支付宝） |
| **trade_no**         | 交易编号                |
| **total_amount**     | 退款金额                |
| **subject**          | 交易内容                |
| **refund_status**    | 退款状态                |
| **create_time**      | 创建时间                |
| **callback_time**    | 回调时间                |
| **callback_content** | 回调信息                |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_refund_payment;
CREATE EXTERNAL TABLE ods_refund_payment(
    `id` STRING COMMENT '编号',
    `out_trade_no` STRING COMMENT '对外业务编号',
    `order_id` STRING COMMENT '订单编号',
    `sku_id` STRING COMMENT 'SKU编号',
    `payment_type` STRING COMMENT '支付类型',
    `trade_no` STRING COMMENT '交易编号',
    `refund_amount` DECIMAL(16,2) COMMENT '支付金额',
    `subject` STRING COMMENT '交易内容',
    `refund_status` STRING COMMENT '支付状态',
    `create_time` STRING COMMENT '创建时间',
    `callback_time` STRING COMMENT '回调时间'
)  COMMENT '支付流水表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_refund_payment/';
```



### 1.1.27 SKU平台属性表（sku_attr_value）

| **字段名**     | **字段说明**  |
| -------------- | ------------- |
| **id**         | 编号          |
| **attr_id**    | 属性id（冗余) |
| **value_id**   | 属性值id      |
| **sku_id**     | skuid         |
| **attr_name**  | 属性名称      |
| **value_name** | 属性值名称    |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_sku_attr_value;
CREATE EXTERNAL TABLE ods_sku_attr_value(
    `id` STRING COMMENT '编号',
    `attr_id` STRING COMMENT '平台属性ID',
    `value_id` STRING COMMENT '平台属性值ID',
    `sku_id` STRING COMMENT '商品ID',
    `attr_name` STRING COMMENT '平台属性名称',
    `value_name` STRING COMMENT '平台属性值名称'
) COMMENT 'sku平台属性表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_sku_attr_value/';
```



### 1.1.28 SKU信息表（sku_info）

| **字段名**          | **字段说明**            |
| ------------------- | ----------------------- |
| **id**              | 库存id(itemID)          |
| **spu_id**          | 商品id                  |
| **price**           | 价格                    |
| **sku_name**        | sku名称                 |
| **sku_desc**        | 商品规格描述            |
| **weight**          | 重量                    |
| **tm_id**           | 品牌(冗余)              |
| **category3_id**    | 三级分类id（冗余)       |
| **sku_default_img** | 默认显示图片(冗余)      |
| **is_sale**         | 是否销售（1：是 0：否） |
| **create_time**     | 创建时间                |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_sku_info;
CREATE EXTERNAL TABLE ods_sku_info(
    `id` STRING COMMENT 'skuId',
    `spu_id` STRING COMMENT 'spuid',
    `price` DECIMAL(16,2) COMMENT '价格',
    `sku_name` STRING COMMENT '商品名称',
    `sku_desc` STRING COMMENT '商品描述',
    `weight` DECIMAL(16,2) COMMENT '重量',
    `tm_id` STRING COMMENT '品牌id',
    `category3_id` STRING COMMENT '品类id',
    `is_sale` STRING COMMENT '是否在售',
    `create_time` STRING COMMENT '创建时间'
) COMMENT 'SKU商品表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_sku_info/';
```



### 1.1.29 SKU销售属性表（sku_sale_attr_value）

| **字段名**               | **字段说明**   |
| ------------------------ | -------------- |
| **id**                   | id             |
| **sku_id**               | 库存单元id     |
| **spu_id**               | spu_id（冗余） |
| **sale_attr_value_id**   | 销售属性值id   |
| **sale_attr_id**         | 销售属性id     |
| **sale_attr_name**       | 销售属性值名称 |
| **sale_attr_value_name** | 销售属性值名称 |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_sku_sale_attr_value;
CREATE EXTERNAL TABLE ods_sku_sale_attr_value(
    `id` STRING COMMENT '编号',
    `sku_id` STRING COMMENT 'sku_id',
    `spu_id` STRING COMMENT 'spu_id',
    `sale_attr_value_id` STRING COMMENT '销售属性值id',
    `sale_attr_id` STRING COMMENT '销售属性id',
    `sale_attr_name` STRING COMMENT '销售属性名称',
    `sale_attr_value_name` STRING COMMENT '销售属性值名称'
) COMMENT 'sku销售属性名称'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_sku_sale_attr_value/';
```



### 1.1.30 SPU信息表（spu_info）

| **字段名**       | **字段说明**        |
| ---------------- | ------------------- |
| **id**           | 商品id              |
| **spu_name**     | 商品名称            |
| **description**  | 商品描述(后台简述） |
| **category3_id** | 三级分类id          |
| **tm_id**        | 品牌id              |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_spu_info;
CREATE EXTERNAL TABLE ods_spu_info(
    `id` STRING COMMENT 'spuid',
    `spu_name` STRING COMMENT 'spu名称',
    `category3_id` STRING COMMENT '品类id',
    `tm_id` STRING COMMENT '品牌id'
) COMMENT 'SPU商品表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_spu_info/';
```



### 1.1.31 SPU销售属性表（spu_sale_attr）

| **字段名**            | **字段说明**         |
| --------------------- | -------------------- |
| **id**                | 编号（业务中无关联） |
| **spu_id**            | 商品id               |
| **base_sale_attr_id** | 销售属性id           |
| **sale_attr_name**    | 销售属性名称（冗余） |

### 1.1.32 SPU销售属性值表（spu_sale_attr_value）

| **字段名**               | **字段说明**         |
| ------------------------ | -------------------- |
| **id**                   | 销售属性值编号       |
| **spu_id**               | 商品id               |
| **base_sale_attr_id**    | 销售属性id           |
| **sale_attr_value_name** | 销售属性值名称       |
| **sale_attr_name**       | 销售属性名称（冗余） |

### 1.1.33 用户地址表（user_address）

| **字段名**       | **字段说明** |
| ---------------- | ------------ |
| **id**           | 编号         |
| **user_id**      | 用户id       |
| **province_id**  | 省份id       |
| **user_address** | 用户地址     |
| **consignee**    | 收件人       |
| **phone_num**    | 联系方式     |
| **is_default**   | 是否是默认   |

### 1.1.34 用户信息表（user_info）

| **字段名**       | **字段说明** |
| ---------------- | ------------ |
| **id**           | 编号         |
| **login_name**   | 用户名称     |
| **nick_name**    | 用户昵称     |
| **passwd**       | 用户密码     |
| **name**         | 用户姓名     |
| **phone_num**    | 手机号       |
| **email**        | 邮箱         |
| **head_img**     | 头像         |
| **user_level**   | 用户级别     |
| **birthday**     | 用户生日     |
| **gender**       | 性别 M男,F女 |
| **create_time**  | 创建时间     |
| **operate_time** | 修改时间     |
| **status**       | 状态         |

**建表语句：**

```hive
DROP TABLE IF EXISTS ods_user_info;
CREATE EXTERNAL TABLE ods_user_info(
    `id` STRING COMMENT '用户id',
    `login_name` STRING COMMENT '用户名称',
    `nick_name` STRING COMMENT '用户昵称',
    `name` STRING COMMENT '用户姓名',
    `phone_num` STRING COMMENT '手机号码',
    `email` STRING COMMENT '邮箱',
    `user_level` STRING COMMENT '用户等级',
    `birthday` STRING COMMENT '生日',
    `gender` STRING COMMENT '性别',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间'
) COMMENT '用户表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/t'
LOCATION '/warehouse/gmall/ods/ods_user_info/';
```



## 1.2 电商系统表结构（dim层数据构建）

|              | 商品 | 分类 | 品牌 | 平台 | 销售 | 购物卷 | 活动 | 省份 | 地区 | 时间 | 用户 |
| ------------ | ---- | ---- | ---- | ---- | ---- | ------ | ---- | ---- | ---- | ---- | ---- |
| 商品维度表   | √    | √    | √    | √    | √    |        |      |      |      |      |      |
| 优惠卷维度表 |      |      |      |      |      | √      |      |      |      |      |      |
| 活动维度表   |      |      |      |      |      |        | √    |      |      |      |      |
| 地区维度表   |      |      |      |      |      |        |      | √    | √    |      |      |
| 用户维度表   |      |      |      |      |      |        |      |      |      |      | √    |
| 时间维度表   |      |      |      |      |      |        |      |      |      | √    |      |

| 商品          |          |          |          |          |              |               |         |
| ------------- | -------- | -------- | -------- | -------- | ------------ | ------------- | ------- |
| 商品（sku）表 |          |          |          |          |              | 商品（spu）表 |         |
| 商品id        | 商品名称 | 商品价格 | 商品描述 | 商品重量 | 商品是否在售 | spu(品类)编号 | spu名称 |

| 分类       |              |              |              |              |              |
| ---------- | ------------ | ------------ | ------------ | ------------ | ------------ |
| 一级品类表 |              | 二级级品类表 |              | 三级级品类表 |              |
| 一级分类id | 一级分类名称 | 二级分类id   | 二级分类名称 | 三级分类id   | 三级分类名称 |

| 品牌          |          |
| ------------- | -------- |
| 商品（spu）表 | 品牌表   |
| 品牌id        | 品牌名称 |

| 平台           |              |              |                |
| -------------- | ------------ | ------------ | -------------- |
| 商品平台属性表 |              |              |                |
| 平台属性id     | 平台属性值id | 平台属性名称 | 平台属性值名称 |

| 销售           |              |              |                |
| -------------- | ------------ | ------------ | -------------- |
| 商品销售属性表 |              |              |                |
| 销售属性id     | 销售属性值id | 销售属性名称 | 销售属性值名称 |

| 购物卷       |            |            |        |        |          |        |      |          |          |              |            |              |              |          |          |
| ------------ | ---------- | ---------- | ------ | ------ | -------- | ------ | ---- | -------- | -------- | ------------ | ---------- | ------------ | ------------ | -------- | -------- |
| 优惠券信息表 |            |            |        |        |          |        |      |          |          |              |            |              |              |          |          |
| 购物卷id     | 购物卷名称 | 购物卷类型 | 满减额 | 满减数 | 活动编号 | 减金额 | 折扣 | 创建时间 | 范围类型 | 最多领用几次 | 已领用次数 | 开始领取时间 | 结束领取时间 | 修改时间 | 过期时间 |

| 活动       |          |          |          |          |          |              |          |          |          |          |          |
| ---------- | -------- | -------- | -------- | -------- | -------- | ------------ | -------- | -------- | -------- | -------- | -------- |
| 活动信息表 |          |          |          |          |          | 活动规则表   |          |          |          |          |          |
| 活动编号   | 活动名称 | 活动类型 | 开始时间 | 结束时间 | 创建时间 | 活动规则编号 | 满减金额 | 满减件数 | 优惠金额 | 优惠折扣 | 优惠级别 |

| 地区     |
| -------- |
| 地区表   |
| 地区名称 |

| 省份   |          |          |                            |                              |        |
| ------ | -------- | -------- | -------------------------- | ---------------------------- | ------ |
| 省份表 |          |          |                            |                              |        |
| id     | 省份名称 | 地区编码 | ISO-3166编码，供可视化使用 | IOS-3166-2编码，供可视化使用 | 地区id |

| 时间 |      |      |              |        |          |      |              |        |
| ---- | ---- | ---- | ------------ | ------ | -------- | ---- | ------------ | ------ |
| 日id | 周id | 周几 | 每月的第几天 | 第几月 | 第几季度 | 年   | 是否为工作日 | 节假日 |

| 用户       |          |          |          |          |      |          |      |      |          |          |          |          |
| ---------- | -------- | -------- | -------- | -------- | ---- | -------- | ---- | ---- | -------- | -------- | -------- | -------- |
| 用户信息表 |          |          |          |          |      |          |      |      |          |          |          |          |
| 用户id     | 用户名称 | 用户昵称 | 用户姓名 | 手机号码 | 邮箱 | 用户等级 | 生日 | 性别 | 创建时间 | 操作时间 | 开始日期 | 结束日期 |

### 1.2.1 商品维度表

```hive
--商品维度表
drop table if exists dim_sku_info;
create external table dim_sku_info
(
    sku_id               string COMMENT "商品id",
    sku_name             string COMMENT "商品名称",
    sku_price            decimal(16, 2) COMMENT "商品价格",
    sku_desc             string COMMENT "商品描述",
    weight               decimal(16, 2) COMMENT "重量",
    is_sale              boolean comment "是否在售",
    spu_id               string COMMENT "品类id",
    spu_name             string COMMENT "品类名称",
    category1_id         string COMMENT "一级分类id",
    category1_name       string COMMENT "一级分类名称",
    category2_id         string COMMENT "二级分类id",
    category2_name       string COMMENT "二级分类名称",
    category3_id         string COMMENT "三级分类id",
    category3_name       string COMMENT "三级分类名称",
    tm_id                string COMMENT "品牌id",
    tm_name              string COMMENT "品牌名称",
    sku_attr_values      ARRAY<STRUCT<attr_id :STRING,value_id :STRING,attr_name :STRING,value_name
                                      :STRING>> COMMENT '平台属性',
    sku_sale_attr_values ARRAY<STRUCT<sale_attr_id :STRING,sale_attr_value_id :STRING,sale_attr_name :STRING,sale_attr_value_name
                                      :STRING>> COMMENT '销售属性',
    create_time          STRING COMMENT '创建时间'
) COMMENT '商品维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dim/dim_sku_info/'
    TBLPROPERTIES ("parquet.compression" = "gzip");
```



```hive
--优惠卷维度表
drop table if exists dim_coupon_info;
create external table dim_coupon_info
(
    coupon_id        string COMMENT "购物卷编号",
    coupon_name      string COMMENT "购物卷名称",
    coupon_type      string COMMENT "购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券",
    condition_amount decimal(16, 2) COMMENT "满减额",
    condition_num    bigint COMMENT "满减数",
    activity_id      string COMMENT "活动编号",
    benefit_amount   decimal(16, 2) COMMENT "减金额",
    benefit_discount decimal(16, 2) COMMENT "折扣",
    create_time      string COMMENT "创建时间",
    create_type      string COMMENT "范围类型 1、商品 2、品类 3、品牌",
    limit_num        bigint COMMENT "最多领用几次",
    taken_count      bigint COMMENT "已领取次数",
    start_time       string COMMENT "领取开始时间",
    end_time         string COMMENT "领取结束时间",
    operate_time     string COMMENT "修改时间",
    expire_time      string COMMENT "过期时间"
) COMMENT "优惠卷维度表"
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dim/dim_coupon_info/'
    tblproperties ("parquet.compression" = "gzip");
```

### 1.2.2 活动维度表

```hive
--活动维度表
drop table if exists dim_activity_rule_info;
create external table dim_activity_rule_info
(
    activity_rule_id string COMMENT "活动规则id",
    activity_id      string COMMENT "活动编号",
    activity_name    string COMMENT "活动名称",
    activity_type    string COMMENT "活动类型",
    start_time       string COMMENT "开始时间",
    end_time         string COMMENT "结束时间",
    create_time      string COMMENT "创建时间",
    condition_amount decimal(16, 2) COMMENT "满减金额",
    condition_num    bigint COMMENT "满减件数",
    benefit_amount   decimal(16, 2) COMMENT "优惠金额",
    benefit_discount decimal(16, 2) COMMENT "优惠折扣",
    benefit_level    string COMMENT "优惠级别"
) COMMENT "活动维度表"
    partitioned by (dt string)
    stored as parquet
    location '/warehouse/gmall/dim/dim_activity_rule_info/'
    tblproperties ("parquet.compression" = "gzip");
```

### 1.2.3 地区维度表

```hive
--地区维度表
drop table if exists dim_base_province;
create external table dim_base_province
(
    id            string COMMENT "id",
    province_name string COMMENT "省份名称",
    area_code     string COMMENT "地区编码",
    iso_code      string COMMENT "ISO-3166编码，供可视化使用",
    iso_3166_2    string COMMENT "IOS-3166-2编码，供可视化使用",
    region_id     string COMMENT "地区id",
    region_name   string COMMENT "地区名称"
) COMMENT "地区维度表"
    stored as parquet
    location '/warehouse/gmall/dim/dim_base_province/'
    tblproperties ("parquet.compression" = "gzip");

insert overwrite table dim_base_province
select bp.id,
       bp.name,
       bp.area_code,
       bp.iso_code,
       bp.iso_3166_2,
       bp.region_id,
       br.region_name
from gmall.ods_base_province bp
         join gmall.ods_base_region br
              on bp.region_id = br.id;
```

### 1.2.4 时间维度表

```hive
--时间维度表
drop table if exists dim_date_info;
create external table dim_date_info
(
    date_id    string COMMENT "日id",
    week_id    string COMMENT "周id",
    week_day   string COMMENT "周几",
    day        string COMMENT "每月的第几天",
    month      string COMMENT "第几月",
    quarter    string COMMENT "第几季度",
    year       string COMMENT "年",
    is_workday string COMMENT "是否是工作日",
    holiday_id string COMMENT "节假日"
) COMMENT "时间维度表"
    stored as parquet
    location '/warehouse/gmall/dim/dim_date_info/'
    tblproperties ("parquet.compression" = "gzip");
```

### 1.2.5 用户维度表

```hive
--用户维度表
DROP TABLE IF EXISTS dim_user_info;
CREATE EXTERNAL TABLE dim_user_info
(
    `id`           STRING COMMENT '用户id',
    `login_name`   STRING COMMENT '用户名称',
    `nick_name`    STRING COMMENT '用户昵称',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dim/dim_user_info/'
    TBLPROPERTIES ("parquet.compression" = "gzip");
```



## 1.3、电商系统表构建（dwd层数据构建）

### **1.3.1启动日志表**

```hive
-- 启动日志表
drop table if exists dwd_start_log;
create external table dwd_start_log(
    area_code string COMMENT '地区编号',
    barnd string COMMENT '手机品牌',
    channel string COMMENT '渠道',
    is_new string COMMENT '是否首次启动',
    model string COMMENT '手机信号',
    mid_id string COMMENT '设备id',
    os string COMMENT '操作系统',
    user_id string COMMENT '用户id',
    version_code string COMMENT 'app版本号',
    entry string COMMENT 'icon手机图标  notice通知  install 安装后启动',
    loading_time bigint COMMENT '启动加载时间',
    open_ad_id string COMMENT '广告页id',
    open_ad_ms bigint COMMENT '广告总播放时长',
    open_ad_skip_ms bigint COMMENT '用户跳过广告时点',
    ts bigint COMMENT '时间'
)COMMENT '启动日志表'
partitioned by (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_start_log'
tblproperties ('parquet.compression'='gzip');
```



### **1.3.2、页面日志表**

```hive
--页面日志表
drop table if exists dwd_page_log;
create external table dwd_page_log(
    area_code string COMMENT '地区编码',
    brand string COMMENT '手机编号',
    channel string COMMENT '渠道',
    is_new string COMMENT '是否首次启动',
    model string COMMENT '手机型号',
    mid_id string COMMENT '设备id',
    os string COMMENT '操作系统',
    user_id string COMMENT '会员id',
    version_code string COMMENT 'app版本号',
    during_time bigint COMMENT '持续时间毫秒',
    page_item string COMMENT '目标id',
    page_item_type string COMMENT '目标类型',
    last_page_id string COMMENT '上页类型',
    page_id string COMMENT '页面id',
    source_type string COMMENT '来源类型',
    ts bigint COMMENT '时间'
)COMMENT '页面日志表'
partitioned by (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_page_log'
tblproperties ('parquet.compression'='gzip');
```

### **1.3.3、动作日志表**

```hive
DROP TABLE IF EXISTS dwd_action_log;
CREATE EXTERNAL TABLE dwd_action_log(
    `area_code` STRING COMMENT '地区编码',
    `brand` STRING COMMENT '手机品牌',
    `channel` STRING COMMENT '渠道',
    `is_new` STRING COMMENT '是否首次启动',
    `model` STRING COMMENT '手机型号',
    `mid_id` STRING COMMENT '设备id',
    `os` STRING COMMENT '操作系统',
    `user_id` STRING COMMENT '会员id',
    `version_code` STRING COMMENT 'app版本号',
    `during_time` BIGINT COMMENT '持续时间毫秒',
    `page_item` STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id` STRING COMMENT '上页类型',
    `page_id` STRING COMMENT '页面id ',
    `source_type` STRING COMMENT '来源类型',
    `action_id` STRING COMMENT '动作id',
    `item` STRING COMMENT '目标id ',
    `item_type` STRING COMMENT '目标类型',
    `ts` BIGINT COMMENT '时间'
) COMMENT '动作日志表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_action_log'
TBLPROPERTIES('parquet.compression'='gzip');
```

### **1.3.4、 曝光日志表**

```hive
-- 曝光日志表
DROP TABLE IF EXISTS dwd_displas_log;
CREATE EXTERNAL TABLE dwd_displas_log(
    `area_code` STRING COMMENT '地区编码',
    `brand` STRING COMMENT '手机品牌',
    `channel` STRING COMMENT '渠道',
    `is_new` STRING COMMENT '是否首次启动',
    `model` STRING COMMENT '手机型号',
    `mid_id` STRING COMMENT '设备id',
    `os` STRING COMMENT '操作系统',
    `user_id` STRING COMMENT '会员id',
    `version_code` STRING COMMENT 'app版本号',
    `during_time` BIGINT COMMENT '持续时间毫秒',
    `page_item` STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id` STRING COMMENT '上页类型',
    `page_id` STRING COMMENT '页面id ',
    `source_type` STRING COMMENT '来源类型',
    display_type string COMMENT '曝光类型',
    item string COMMENT '曝光对象id',
    item_type string COMMENT '曝光类型',
    `order` bigint COMMENT '曝光顺序',
    pos_id bigint COMMENT '曝光位置',
    `ts` BIGINT COMMENT '时间'
) COMMENT '曝光日志表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_displas_log'
TBLPROPERTIES('parquet.compression'='gzip');
```

### **1.3.5、错误日志表**

```hive
-- 错误日志表
DROP TABLE IF EXISTS dwd_error_log;
CREATE EXTERNAL TABLE dwd_error_log(
    `area_code` STRING COMMENT '地区编码',
    `brand` STRING COMMENT '手机品牌',
    `channel` STRING COMMENT '渠道',
    `is_new` STRING COMMENT '是否首次启动',
    `model` STRING COMMENT '手机型号',
    `mid_id` STRING COMMENT '设备id',
    `os` STRING COMMENT '操作系统',
    `user_id` STRING COMMENT '会员id',
    `version_code` STRING COMMENT 'app版本号',
    `page_item` STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id` STRING COMMENT '上页类型',
    `page_id` STRING COMMENT '页面id ',
    `source_type` STRING COMMENT '来源类型',
    entry string COMMENT 'icon手机图标  notice通知  install 安装后启动',
    loading_time bigint COMMENT '启动加载时间',
    open_ad_id string COMMENT '广告页id',
    open_ad_ms bigint COMMENT '广告总播放时长',
    open_ad_skip_ms bigint COMMENT '用户跳过广告时点',
    actions string COMMENT '动作',
    displays string COMMENT '曝光',
    `ts` BIGINT COMMENT '时间',
    error_code string COMMENT '错误码',
    msg string COMMENT '错误信息'
) COMMENT '错误日志表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_error_log'
TBLPROPERTIES('parquet.compression'='gzip');
```

### **1.3.6、评价事实表（事务型事实表）**

```hive
--评价事实表（事务型事实表）
drop table if exists dwd_comment_info;
create external table dwd_comment_info(
    id string COMMENT '编号',
    user_id string COMMENT '用户id',
    sku_id string COMMENT '商品sku',
    spu_id string COMMENT '商品spu',
    order_id string COMMENT '订单id',
    appraise string COMMENT '评价(好评、中评、差评、默认评价)',
    create_time string COMMENT '评价时间'
)COMMENT '评价事实表'
partitioned by (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_comment_info'
TBLPROPERTIES('parquet.compression'='gzip');
```

### **1.3.7、订单明细事实表（事务型事实表）**

```hive
-- 订单明细事实表（事务型事实表）
drop table if exists dwd_order_detail;
create external table dwd_order_detail(
    id string COMMENT '订单编号',
    order_id string COMMENT '订单号',
    user_id string COMMENT '用户id',
    sku_id string COMMENT 'sku用户id',
    province_id string COMMENT '省份id',
    activity_id string COMMENT '活动id',
    activity_rule string COMMENT '活动规则id',
    coupon_id string COMMENT '优惠卷id',
    create_time string COMMENT '创建时间',
    source_type string COMMENT '来源类型',
    source_id string COMMENT '来源编号',
    sku_num bigint COMMENT '商品数量',
    original_amount decimal(16,2) COMMENT '原始价格',
    split_activity_amount decimal(16,2) COMMENT '活动优惠分摊',
    split_coupon_amount decimal(16,2) COMMENT '优惠卷优惠分摊',
    split_final_amount decimal(16,2) COMMENT '最终价格分摊'
)COMMENT '订单明细事实表'
partitioned by (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_order_detail'
TBLPROPERTIES('parquet.compression'='gzip');
```

### **1.3.8、退单事实表**

```hive
--退单事实表
DROP TABLE IF EXISTS dwd_order_refund_info;
CREATE EXTERNAL TABLE dwd_order_refund_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `order_id` STRING COMMENT '订单ID',
    `sku_id` STRING COMMENT '商品ID',
    `province_id` STRING COMMENT '地区ID',
    `refund_type` STRING COMMENT '退单类型',
    `refund_num` BIGINT COMMENT '退单件数',
    `refund_amount` DECIMAL(16,2) COMMENT '退单金额',
    `refund_reason_type` STRING COMMENT '退单原因类型',
    `create_time` STRING COMMENT '退单时间'
) COMMENT '退单事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_order_refund_info/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### **1.3.9、 加购事实表（周期型快照事实表，每日快照）**

```hive
-- 加购事实表（周期型快照事实表，每日快照）
DROP TABLE IF EXISTS dwd_cart_info;
CREATE EXTERNAL TABLE dwd_cart_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `sku_id` STRING COMMENT '商品ID',
    `source_type` STRING COMMENT '来源类型',
    `source_id` STRING COMMENT '来源编号',
    `cart_price` DECIMAL(16,2) COMMENT '加入购物车时的价格',
    `is_ordered` STRING COMMENT '是否已下单',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `order_time` STRING COMMENT '下单时间',
    `sku_num` BIGINT COMMENT '加购数量'
) COMMENT '加购事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_cart_info/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### **1.3.10、收藏事实表**

```hive
--收藏事实表
DROP TABLE IF EXISTS dwd_favor_info;
CREATE EXTERNAL TABLE dwd_favor_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING  COMMENT '用户id',
    `sku_id` STRING  COMMENT 'skuid',
    `spu_id` STRING  COMMENT 'spuid',
    `is_cancel` STRING  COMMENT '是否取消',
    `create_time` STRING  COMMENT '收藏时间',
    `cancel_time` STRING  COMMENT '取消时间'
) COMMENT '收藏事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_favor_info/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### **1.3.11、优惠券领用事实表（累积型快照事实表）**

```hive
-- 优惠券领用事实表（累积型快照事实表）
DROP TABLE IF EXISTS dwd_coupon_use;
CREATE EXTERNAL TABLE dwd_coupon_use(
    `id` STRING COMMENT '编号',
    `coupon_id` STRING  COMMENT '优惠券ID',
    `user_id` STRING  COMMENT 'userid',
    `order_id` STRING  COMMENT '订单id',
    `coupon_status` STRING  COMMENT '优惠券状态',
    `get_time` STRING  COMMENT '领取时间',
    `using_time` STRING  COMMENT '使用时间(下单)',
    `used_time` STRING  COMMENT '使用时间(支付)',
    `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券领用事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_coupon_use/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### **1.3.12、 支付事实表（累积型快照事实表）**

```hive
-- 支付事实表（累积型快照事实表）
DROP TABLE IF EXISTS dwd_payment_info;
CREATE EXTERNAL TABLE dwd_payment_info (
    `id` STRING COMMENT '编号',
    `order_id` STRING COMMENT '订单编号',
    `user_id` STRING COMMENT '用户编号',
    `province_id` STRING COMMENT '地区ID',
    `trade_no` STRING COMMENT '交易编号',
    `out_trade_no` STRING COMMENT '对外交易编号',
    `payment_type` STRING COMMENT '支付类型',
    `payment_amount` DECIMAL(16,2) COMMENT '支付金额',
    `payment_status` STRING COMMENT '支付状态',
    `create_time` STRING COMMENT '创建时间',--调用第三方支付接口的时间
    `callback_time` STRING COMMENT '完成时间'--支付完成时间，即支付成功回调时间
) COMMENT '支付事实表表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_payment_info/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### **1.3.13、退款事实表**

```hive
-- 退款事实表
DROP TABLE IF EXISTS dwd_refund_payment;
CREATE EXTERNAL TABLE dwd_refund_payment (
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `order_id` STRING COMMENT '订单编号',
    `sku_id` STRING COMMENT 'SKU编号',
    `province_id` STRING COMMENT '地区ID',
    `trade_no` STRING COMMENT '交易编号',
    `out_trade_no` STRING COMMENT '对外交易编号',
    `payment_type` STRING COMMENT '支付类型',
    `refund_amount` DECIMAL(16,2) COMMENT '退款金额',
    `refund_status` STRING COMMENT '退款状态',
    `create_time` STRING COMMENT '创建时间',--调用第三方支付接口的时间
    `callback_time` STRING COMMENT '回调时间'--支付接口回调时间，即支付成功时间
) COMMENT '退款事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_refund_payment/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### **1.3.14、订单事实表（累积型快照事实表）**

```hive
-- 订单事实表（累积型快照事实表）
DROP TABLE IF EXISTS dwd_order_info;
CREATE EXTERNAL TABLE dwd_order_info(
    `id` STRING COMMENT '编号',
    `order_status` STRING COMMENT '订单状态',
    `user_id` STRING COMMENT '用户ID',
    `province_id` STRING COMMENT '地区ID',
    `payment_way` STRING COMMENT '支付方式',
    `delivery_address` STRING COMMENT '邮寄地址',
    `out_trade_no` STRING COMMENT '对外交易编号',
    `tracking_no` STRING COMMENT '物流单号',
    `create_time` STRING COMMENT '创建时间(未支付状态)',
    `payment_time` STRING COMMENT '支付时间(已支付状态)',
    `cancel_time` STRING COMMENT '取消时间(已取消状态)',
    `finish_time` STRING COMMENT '完成时间(已完成状态)',
    `refund_time` STRING COMMENT '退款时间(退款中状态)',
    `refund_finish_time` STRING COMMENT '退款完成时间(退款完成状态)',
    `expire_time` STRING COMMENT '过期时间',
    `feight_fee` DECIMAL(16,2) COMMENT '运费',
    `feight_fee_reduce` DECIMAL(16,2) COMMENT '运费减免',
    `activity_reduce_amount` DECIMAL(16,2) COMMENT '活动减免',
    `coupon_reduce_amount` DECIMAL(16,2) COMMENT '优惠券减免',
    `original_amount` DECIMAL(16,2) COMMENT '订单原始价格',
    `final_amount` DECIMAL(16,2) COMMENT '订单最终价格'
) COMMENT '订单事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_order_info/'
TBLPROPERTIES ("parquet.compression"="gzip");
```



## 1.4、电商系统表构建（dws层数据构建）

### 1.4.1、用户主题表

```hive
-- 用户主题
DROP TABLE IF EXISTS dws_user_action_daycount;
CREATE EXTERNAL TABLE dws_user_action_daycount
(
    `user_id`                      STRING COMMENT '用户id',
    `login_count`                  BIGINT COMMENT '登录次数',
    `cart_count`                   BIGINT COMMENT '加入购物车次数',
    `favor_count`                  BIGINT COMMENT '收藏次数',
    `order_count`                  BIGINT COMMENT '下单次数',
    `order_activity_count`         BIGINT COMMENT '订单参与活动次数',
    `order_activity_reduce_amount` DECIMAL(16, 2) COMMENT '订单减免金额(活动)',
    `order_coupon_count`           BIGINT COMMENT '订单用券次数',
    `order_coupon_reduce_amount`   DECIMAL(16, 2) COMMENT '订单减免金额(优惠券)',
    `order_original_amount`        DECIMAL(16, 2) COMMENT '订单单原始金额',
    `order_final_amount`           DECIMAL(16, 2) COMMENT '订单总金额',
    `payment_count`                BIGINT COMMENT '支付次数',
    `payment_amount`               DECIMAL(16, 2) COMMENT '支付金额',
    `refund_order_count`           BIGINT COMMENT '退单次数',
    `refund_order_num`             BIGINT COMMENT '退单件数',
    `refund_order_amount`          DECIMAL(16, 2) COMMENT '退单金额',
    `refund_payment_count`         BIGINT COMMENT '退款次数',
    `refund_payment_num`           BIGINT COMMENT '退款件数',
    `refund_payment_amount`        DECIMAL(16, 2) COMMENT '退款金额',
    `coupon_get_count`             BIGINT COMMENT '优惠券领取次数',
    `coupon_using_count`           BIGINT COMMENT '优惠券使用(下单)次数',
    `coupon_used_count`            BIGINT COMMENT '优惠券使用(支付)次数',
    `appraise_good_count`          BIGINT COMMENT '好评数',
    `appraise_mid_count`           BIGINT COMMENT '中评数',
    `appraise_bad_count`           BIGINT COMMENT '差评数',
    `appraise_default_count`       BIGINT COMMENT '默认评价数',
    `order_detail_stats`           array<struct<sku_id :string,sku_num :bigint,order_count :bigint,activity_reduce_amount
                                                :decimal(16, 2),coupon_reduce_amount :decimal(16, 2),original_amount
                                                :decimal(16, 2),final_amount :decimal(16, 2)>> COMMENT '下单明细统计'
) COMMENT '每日用户行为'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_user_action_daycount/'
    TBLPROPERTIES ("parquet.compression" = "gzip");
```

### 1.4.2、访客主题表

```hive
DROP TABLE IF EXISTS dws_visitor_action_daycount;
CREATE EXTERNAL TABLE dws_visitor_action_daycount
(
    `mid_id`       STRING COMMENT '设备id',
    `brand`        STRING COMMENT '设备品牌',
    `model`        STRING COMMENT '设备型号',
    `is_new`       STRING COMMENT '是否首次访问',
    `channel`      ARRAY<STRING> COMMENT '渠道',
    `os`           ARRAY<STRING> COMMENT '操作系统',
    `area_code`    ARRAY<STRING> COMMENT '地区ID',
    `version_code` ARRAY<STRING> COMMENT '应用版本',
    `visit_count`  BIGINT COMMENT '访问次数',
    `page_stats`   ARRAY<STRUCT<page_id:STRING,page_count:BIGINT,during_time:BIGINT>> COMMENT '页面访问统计'
) COMMENT '每日设备行为表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_visitor_action_daycount'
    TBLPROPERTIES ("parquet.compression" = "gzip");
```

### 1.4.3、商品主题表

```hive
DROP TABLE IF EXISTS dws_sku_action_daycount;
CREATE EXTERNAL TABLE dws_sku_action_daycount
(
    `sku_id` STRING COMMENT 'sku_id',
    `order_count` BIGINT COMMENT '被下单次数',
    `order_num` BIGINT COMMENT '被下单件数',
    `order_activity_count` BIGINT COMMENT '参与活动被下单次数',
    `order_coupon_count` BIGINT COMMENT '使用优惠券被下单次数',
    `order_activity_reduce_amount` DECIMAL(16,2) COMMENT '优惠金额(活动)',
    `order_coupon_reduce_amount` DECIMAL(16,2) COMMENT '优惠金额(优惠券)',
    `order_original_amount` DECIMAL(16,2) COMMENT '被下单原价金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '被下单最终金额',
    `payment_count` BIGINT COMMENT '被支付次数',
    `payment_num` BIGINT COMMENT '被支付件数',
    `payment_amount` DECIMAL(16,2) COMMENT '被支付金额',
    `refund_order_count` BIGINT  COMMENT '被退单次数',
    `refund_order_num` BIGINT COMMENT '被退单件数',
    `refund_order_amount` DECIMAL(16,2) COMMENT '被退单金额',
    `refund_payment_count` BIGINT  COMMENT '被退款次数',
    `refund_payment_num` BIGINT COMMENT '被退款件数',
    `refund_payment_amount` DECIMAL(16,2) COMMENT '被退款金额',
    `cart_count` BIGINT COMMENT '被加入购物车次数',
    `favor_count` BIGINT COMMENT '被收藏次数',
    `appraise_good_count` BIGINT COMMENT '好评数',
    `appraise_mid_count` BIGINT COMMENT '中评数',
    `appraise_bad_count` BIGINT COMMENT '差评数',
    `appraise_default_count` BIGINT COMMENT '默认评价数'
) COMMENT '每日商品行为'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dws/dws_sku_action_daycount/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### 1.4.4、**优惠卷主题表**

```hive
DROP TABLE IF EXISTS dws_coupon_info_daycount;
CREATE EXTERNAL TABLE dws_coupon_info_daycount(
    `coupon_id` STRING COMMENT '优惠券ID',
    `get_count` BIGINT COMMENT '被领取次数',
    `order_count` BIGINT COMMENT '被使用(下单)次数',
    `order_reduce_amount` DECIMAL(16,2) COMMENT '用券下单优惠金额',
    `order_original_amount` DECIMAL(16,2) COMMENT '用券订单原价金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '用券下单最终金额',
    `payment_count` BIGINT COMMENT '被使用(支付)次数',
    `payment_reduce_amount` DECIMAL(16,2) COMMENT '用券支付优惠金额',
    `payment_amount` DECIMAL(16,2) COMMENT '用券支付总金额',
    `expire_count` BIGINT COMMENT '过期次数'
) COMMENT '每日活动统计'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dws/dws_coupon_info_daycount/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### 1.4.5、活动主题表

```hive
DROP TABLE IF EXISTS dws_activity_info_daycount;
CREATE EXTERNAL TABLE dws_activity_info_daycount(
    `activity_rule_id` STRING COMMENT '活动规则ID',
    `activity_id` STRING COMMENT '活动ID',
    `order_count` BIGINT COMMENT '参与某活动某规则下单次数',    `order_reduce_amount` DECIMAL(16,2) COMMENT '参与某活动某规则下单减免金额',
    `order_original_amount` DECIMAL(16,2) COMMENT '参与某活动某规则下单原始金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '参与某活动某规则下单最终金额',
    `payment_count` BIGINT COMMENT '参与某活动某规则支付次数',
   `payment_reduce_amount` DECIMAL(16,2) COMMENT '参与某活动某规则支付减免金额',
    `payment_amount` DECIMAL(16,2) COMMENT '参与某活动某规则支付金额'
) COMMENT '每日活动统计'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dws/dws_activity_info_daycount/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### 1.4.6、地区主题表

```hive
DROP TABLE IF EXISTS dws_area_stats_daycount;
CREATE EXTERNAL TABLE dws_area_stats_daycount(
    `province_id` STRING COMMENT '地区编号',
    `visit_count` BIGINT COMMENT '访问次数',
    `login_count` BIGINT COMMENT '登录次数',
    `visitor_count` BIGINT COMMENT '访客人数',
    `user_count` BIGINT COMMENT '用户人数',
    `order_count` BIGINT COMMENT '下单次数',
    `order_original_amount` DECIMAL(16,2) COMMENT '下单原始金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '下单最终金额',
    `payment_count` BIGINT COMMENT '支付次数',
    `payment_amount` DECIMAL(16,2) COMMENT '支付金额',
    `refund_order_count` BIGINT COMMENT '退单次数',
    `refund_order_amount` DECIMAL(16,2) COMMENT '退单金额',
    `refund_payment_count` BIGINT COMMENT '退款次数',
    `refund_payment_amount` DECIMAL(16,2) COMMENT '退款金额'
) COMMENT '每日地区统计表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dws/dws_area_stats_daycount/'
TBLPROPERTIES ("parquet.compression"="gzip");
```



## 1.5、电商系统构建表（dwt层数据构建）

### 1.5.1、设备主题宽表

```hive
-- 设备主题宽表
DROP TABLE IF EXISTS dwt_visitor_topic;
CREATE EXTERNAL TABLE dwt_visitor_topic
(
    `mid_id`                   STRING COMMENT '设备id',
    `brand`                    STRING COMMENT '手机品牌',
    `model`                    STRING COMMENT '手机型号',
    `channel`                  ARRAY<STRING> COMMENT '渠道',
    `os`                       ARRAY<STRING> COMMENT '操作系统',
    `area_code`                ARRAY<STRING> COMMENT '地区ID',
    `version_code`             ARRAY<STRING> COMMENT '应用版本',
    `visit_date_first`         STRING COMMENT '首次访问时间',
    `visit_date_last`          STRING COMMENT '末次访问时间',
    `visit_last_1d_count`      BIGINT COMMENT '最近1日访问次数',
    `visit_last_1d_day_count`  BIGINT COMMENT '最近1日访问天数',
    `visit_last_7d_count`      BIGINT COMMENT '最近7日访问次数',
    `visit_last_7d_day_count`  BIGINT COMMENT '最近7日访问天数',
    `visit_last_30d_count`     BIGINT COMMENT '最近30日访问次数',
    `visit_last_30d_day_count` BIGINT COMMENT '最近30日访问天数',
    `visit_count`              BIGINT COMMENT '累积访问次数',
    `visit_day_count`          BIGINT COMMENT '累积访问天数'
) COMMENT '设备主题宽表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwt/dwt_visitor_topic'
    TBLPROPERTIES ("parquet.compression" = "gzip");
```

### 1.5.2、会员主题宽表

```hive
-- 会员主题宽表
DROP TABLE IF EXISTS dwt_user_topic;
CREATE EXTERNAL TABLE dwt_user_topic
(
    `user_id` STRING  COMMENT '用户id',
    `login_date_first` STRING COMMENT '首次活跃日期',
    `login_date_last` STRING COMMENT '末次活跃日期',
    `login_date_1d_count` STRING COMMENT '最近1日登录次数',
    `login_last_1d_day_count` BIGINT COMMENT '最近1日登录天数',
    `login_last_7d_count` BIGINT COMMENT '最近7日登录次数',
    `login_last_7d_day_count` BIGINT COMMENT '最近7日登录天数',
    `login_last_30d_count` BIGINT COMMENT '最近30日登录次数',
    `login_last_30d_day_count` BIGINT COMMENT '最近30日登录天数',
    `login_count` BIGINT COMMENT '累积登录次数',
    `login_day_count` BIGINT COMMENT '累积登录天数',
    `order_date_first` STRING COMMENT '首次下单时间',
    `order_date_last` STRING COMMENT '末次下单时间',
    `order_last_1d_count` BIGINT COMMENT '最近1日下单次数',
    `order_activity_last_1d_count` BIGINT COMMENT '最近1日订单参与活动次数',
    `order_activity_reduce_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日订单减免金额(活动)',
    `order_coupon_last_1d_count` BIGINT COMMENT '最近1日下单用券次数',
    `order_coupon_reduce_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日订单减免金额(优惠券)',
    `order_last_1d_original_amount` DECIMAL(16,2) COMMENT '最近1日原始下单金额',
    `order_last_1d_final_amount` DECIMAL(16,2) COMMENT '最近1日最终下单金额',
    `order_last_7d_count` BIGINT COMMENT '最近7日下单次数',
    `order_activity_last_7d_count` BIGINT COMMENT '最近7日订单参与活动次数',
    `order_activity_reduce_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日订单减免金额(活动)',
    `order_coupon_last_7d_count` BIGINT COMMENT '最近7日下单用券次数',
    `order_coupon_reduce_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日订单减免金额(优惠券)',
    `order_last_7d_original_amount` DECIMAL(16,2) COMMENT '最近7日原始下单金额',
    `order_last_7d_final_amount` DECIMAL(16,2) COMMENT '最近7日最终下单金额',
    `order_last_30d_count` BIGINT COMMENT '最近30日下单次数',
    `order_activity_last_30d_count` BIGINT COMMENT '最近30日订单参与活动次数',
    `order_activity_reduce_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日订单减免金额(活动)',
    `order_coupon_last_30d_count` BIGINT COMMENT '最近30日下单用券次数',
    `order_coupon_reduce_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日订单减免金额(优惠券)',
    `order_last_30d_original_amount` DECIMAL(16,2) COMMENT '最近30日原始下单金额',
    `order_last_30d_final_amount` DECIMAL(16,2) COMMENT '最近30日最终下单金额',
    `order_count` BIGINT COMMENT '累积下单次数',
    `order_activity_count` BIGINT COMMENT '累积订单参与活动次数',
    `order_activity_reduce_amount` DECIMAL(16,2) COMMENT '累积订单减免金额(活动)',
    `order_coupon_count` BIGINT COMMENT '累积下单用券次数',
    `order_coupon_reduce_amount` DECIMAL(16,2) COMMENT '累积订单减免金额(优惠券)',
    `order_original_amount` DECIMAL(16,2) COMMENT '累积原始下单金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '累积最终下单金额',
    `payment_date_first` STRING COMMENT '首次支付时间',
    `payment_date_last` STRING COMMENT '末次支付时间',
    `payment_last_1d_count` BIGINT COMMENT '最近1日支付次数',
    `payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日支付金额',
    `payment_last_7d_count` BIGINT COMMENT '最近7日支付次数',
    `payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日支付金额',
    `payment_last_30d_count` BIGINT COMMENT '最近30日支付次数',
    `payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日支付金额',
    `payment_count` BIGINT COMMENT '累积支付次数',
    `payment_amount` DECIMAL(16,2) COMMENT '累积支付金额',
    `refund_order_last_1d_count` BIGINT COMMENT '最近1日退单次数',
    `refund_order_last_1d_num` BIGINT COMMENT '最近1日退单件数',
    `refund_order_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日退单金额',
    `refund_order_last_7d_count` BIGINT COMMENT '最近7日退单次数',
    `refund_order_last_7d_num` BIGINT COMMENT '最近7日退单件数',
    `refund_order_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日退单金额',
    `refund_order_last_30d_count` BIGINT COMMENT '最近30日退单次数',
    `refund_order_last_30d_num` BIGINT COMMENT '最近30日退单件数',
    `refund_order_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日退单金额',
    `refund_order_count` BIGINT COMMENT '累积退单次数',
    `refund_order_num` BIGINT COMMENT '累积退单件数',
    `refund_order_amount` DECIMAL(16,2) COMMENT '累积退单金额',
    `refund_payment_last_1d_count` BIGINT COMMENT '最近1日退款次数',
    `refund_payment_last_1d_num` BIGINT COMMENT '最近1日退款件数',
    `refund_payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日退款金额',
    `refund_payment_last_7d_count` BIGINT COMMENT '最近7日退款次数',
    `refund_payment_last_7d_num` BIGINT COMMENT '最近7日退款件数',
    `refund_payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日退款金额',
    `refund_payment_last_30d_count` BIGINT COMMENT '最近30日退款次数',
    `refund_payment_last_30d_num` BIGINT COMMENT '最近30日退款件数',
    `refund_payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日退款金额',
    `refund_payment_count` BIGINT COMMENT '累积退款次数',
    `refund_payment_num` BIGINT COMMENT '累积退款件数',
    `refund_payment_amount` DECIMAL(16,2) COMMENT '累积退款金额',
    `cart_last_1d_count` BIGINT COMMENT '最近1日加入购物车次数',
    `cart_last_7d_count` BIGINT COMMENT '最近7日加入购物车次数',
    `cart_last_30d_count` BIGINT COMMENT '最近30日加入购物车次数',
    `cart_count` BIGINT COMMENT '累积加入购物车次数',
    `favor_last_1d_count` BIGINT COMMENT '最近1日收藏次数',
    `favor_last_7d_count` BIGINT COMMENT '最近7日收藏次数',
    `favor_last_30d_count` BIGINT COMMENT '最近30日收藏次数',
    `favor_count` BIGINT COMMENT '累积收藏次数',
    `coupon_last_1d_get_count` BIGINT COMMENT '最近1日领券次数',
    `coupon_last_1d_using_count` BIGINT COMMENT '最近1日用券(下单)次数',
    `coupon_last_1d_used_count` BIGINT COMMENT '最近1日用券(支付)次数',
    `coupon_last_7d_get_count` BIGINT COMMENT '最近7日领券次数',
    `coupon_last_7d_using_count` BIGINT COMMENT '最近7日用券(下单)次数',
    `coupon_last_7d_used_count` BIGINT COMMENT '最近7日用券(支付)次数',
    `coupon_last_30d_get_count` BIGINT COMMENT '最近30日领券次数',
    `coupon_last_30d_using_count` BIGINT COMMENT '最近30日用券(下单)次数',
    `coupon_last_30d_used_count` BIGINT COMMENT '最近30日用券(支付)次数',
    `coupon_get_count` BIGINT COMMENT '累积领券次数',
    `coupon_using_count` BIGINT COMMENT '累积用券(下单)次数',
    `coupon_used_count` BIGINT COMMENT '累积用券(支付)次数',
    `appraise_last_1d_good_count` BIGINT COMMENT '最近1日好评次数',
    `appraise_last_1d_mid_count` BIGINT COMMENT '最近1日中评次数',
    `appraise_last_1d_bad_count` BIGINT COMMENT '最近1日差评次数',
    `appraise_last_1d_default_count` BIGINT COMMENT '最近1日默认评价次数',
    `appraise_last_7d_good_count` BIGINT COMMENT '最近7日好评次数',
    `appraise_last_7d_mid_count` BIGINT COMMENT '最近7日中评次数',
    `appraise_last_7d_bad_count` BIGINT COMMENT '最近7日差评次数',
    `appraise_last_7d_default_count` BIGINT COMMENT '最近7日默认评价次数',
    `appraise_last_30d_good_count` BIGINT COMMENT '最近30日好评次数',
    `appraise_last_30d_mid_count` BIGINT COMMENT '最近30日中评次数',
    `appraise_last_30d_bad_count` BIGINT COMMENT '最近30日差评次数',
    `appraise_last_30d_default_count` BIGINT COMMENT '最近30日默认评价次数',
    `appraise_good_count` BIGINT COMMENT '累积好评次数',
    `appraise_mid_count` BIGINT COMMENT '累积中评次数',
    `appraise_bad_count` BIGINT COMMENT '累积差评次数',
    `appraise_default_count` BIGINT COMMENT '累积默认评价次数'
)COMMENT '会员主题宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwt/dwt_user_topic/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### **1.5.3、商品主题宽表**

```hive
DROP TABLE IF EXISTS dwt_sku_topic;
CREATE EXTERNAL TABLE dwt_sku_topic
(
    `sku_id` STRING COMMENT 'sku_id',
    `order_last_1d_count` BIGINT COMMENT '最近1日被下单次数',
    `order_last_1d_num` BIGINT COMMENT '最近1日被下单件数',
    `order_activity_last_1d_count` BIGINT COMMENT '最近1日参与活动被下单次数',
    `order_coupon_last_1d_count` BIGINT COMMENT '最近1日使用优惠券被下单次数',
    `order_activity_reduce_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日优惠金额(活动)',
    `order_coupon_reduce_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日优惠金额(优惠券)',
    `order_last_1d_original_amount` DECIMAL(16,2) COMMENT '最近1日被下单原始金额',
    `order_last_1d_final_amount` DECIMAL(16,2) COMMENT '最近1日被下单最终金额',
    `order_last_7d_count` BIGINT COMMENT '最近7日被下单次数',
    `order_last_7d_num` BIGINT COMMENT '最近7日被下单件数',
    `order_activity_last_7d_count` BIGINT COMMENT '最近7日参与活动被下单次数',
    `order_coupon_last_7d_count` BIGINT COMMENT '最近7日使用优惠券被下单次数',
    `order_activity_reduce_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日优惠金额(活动)',
    `order_coupon_reduce_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日优惠金额(优惠券)',
    `order_last_7d_original_amount` DECIMAL(16,2) COMMENT '最近7日被下单原始金额',
    `order_last_7d_final_amount` DECIMAL(16,2) COMMENT '最近7日被下单最终金额',
    `order_last_30d_count` BIGINT COMMENT '最近30日被下单次数',
    `order_last_30d_num` BIGINT COMMENT '最近30日被下单件数',
    `order_activity_last_30d_count` BIGINT COMMENT '最近30日参与活动被下单次数',
    `order_coupon_last_30d_count` BIGINT COMMENT '最近30日使用优惠券被下单次数',
    `order_activity_reduce_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日优惠金额(活动)',
    `order_coupon_reduce_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日优惠金额(优惠券)',
    `order_last_30d_original_amount` DECIMAL(16,2) COMMENT '最近30日被下单原始金额',
    `order_last_30d_final_amount` DECIMAL(16,2) COMMENT '最近30日被下单最终金额',
    `order_count` BIGINT COMMENT '累积被下单次数',
    `order_num` BIGINT COMMENT '累积被下单件数',
    `order_activity_count` BIGINT COMMENT '累积参与活动被下单次数',
    `order_coupon_count` BIGINT COMMENT '累积使用优惠券被下单次数',
    `order_activity_reduce_amount` DECIMAL(16,2) COMMENT '累积优惠金额(活动)',
    `order_coupon_reduce_amount` DECIMAL(16,2) COMMENT '累积优惠金额(优惠券)',
    `order_original_amount` DECIMAL(16,2) COMMENT '累积被下单原始金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '累积被下单最终金额',
    `payment_last_1d_count` BIGINT COMMENT '最近1日被支付次数',
    `payment_last_1d_num` BIGINT COMMENT '最近1日被支付件数',
    `payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日被支付金额',
    `payment_last_7d_count` BIGINT COMMENT '最近7日被支付次数',
    `payment_last_7d_num` BIGINT COMMENT '最近7日被支付件数',
    `payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日被支付金额',
    `payment_last_30d_count` BIGINT COMMENT '最近30日被支付次数',
    `payment_last_30d_num` BIGINT COMMENT '最近30日被支付件数',
    `payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日被支付金额',
    `payment_count` BIGINT COMMENT '累积被支付次数',
    `payment_num` BIGINT COMMENT '累积被支付件数',
    `payment_amount` DECIMAL(16,2) COMMENT '累积被支付金额',
    `refund_order_last_1d_count` BIGINT COMMENT '最近1日退单次数',
    `refund_order_last_1d_num` BIGINT COMMENT '最近1日退单件数',
    `refund_order_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日退单金额',
    `refund_order_last_7d_count` BIGINT COMMENT '最近7日退单次数',
    `refund_order_last_7d_num` BIGINT COMMENT '最近7日退单件数',
    `refund_order_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日退单金额',
    `refund_order_last_30d_count` BIGINT COMMENT '最近30日退单次数',
    `refund_order_last_30d_num` BIGINT COMMENT '最近30日退单件数',
    `refund_order_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日退单金额',
    `refund_order_count` BIGINT COMMENT '累积退单次数',
    `refund_order_num` BIGINT COMMENT '累积退单件数',
    `refund_order_amount` DECIMAL(16,2) COMMENT '累积退单金额',
    `refund_payment_last_1d_count` BIGINT COMMENT '最近1日退款次数',
    `refund_payment_last_1d_num` BIGINT COMMENT '最近1日退款件数',
    `refund_payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日退款金额',
    `refund_payment_last_7d_count` BIGINT COMMENT '最近7日退款次数',
    `refund_payment_last_7d_num` BIGINT COMMENT '最近7日退款件数',
    `refund_payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日退款金额',
    `refund_payment_last_30d_count` BIGINT COMMENT '最近30日退款次数',
    `refund_payment_last_30d_num` BIGINT COMMENT '最近30日退款件数',
    `refund_payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日退款金额',
    `refund_payment_count` BIGINT COMMENT '累积退款次数',
    `refund_payment_num` BIGINT COMMENT '累积退款件数',
    `refund_payment_amount` DECIMAL(16,2) COMMENT '累积退款金额',
    `cart_last_1d_count` BIGINT COMMENT '最近1日被加入购物车次数',
    `cart_last_7d_count` BIGINT COMMENT '最近7日被加入购物车次数',
    `cart_last_30d_count` BIGINT COMMENT '最近30日被加入购物车次数',
    `cart_count` BIGINT COMMENT '累积被加入购物车次数',
    `favor_last_1d_count` BIGINT COMMENT '最近1日被收藏次数',
    `favor_last_7d_count` BIGINT COMMENT '最近7日被收藏次数',
    `favor_last_30d_count` BIGINT COMMENT '最近30日被收藏次数',
    `favor_count` BIGINT COMMENT '累积被收藏次数',
    `appraise_last_1d_good_count` BIGINT COMMENT '最近1日好评数',
    `appraise_last_1d_mid_count` BIGINT COMMENT '最近1日中评数',
    `appraise_last_1d_bad_count` BIGINT COMMENT '最近1日差评数',
    `appraise_last_1d_default_count` BIGINT COMMENT '最近1日默认评价数',
    `appraise_last_7d_good_count` BIGINT COMMENT '最近7日好评数',
    `appraise_last_7d_mid_count` BIGINT COMMENT '最近7日中评数',
    `appraise_last_7d_bad_count` BIGINT COMMENT '最近7日差评数',
    `appraise_last_7d_default_count` BIGINT COMMENT '最近7日默认评价数',
    `appraise_last_30d_good_count` BIGINT COMMENT '最近30日好评数',
    `appraise_last_30d_mid_count` BIGINT COMMENT '最近30日中评数',
    `appraise_last_30d_bad_count` BIGINT COMMENT '最近30日差评数',
    `appraise_last_30d_default_count` BIGINT COMMENT '最近30日默认评价数',
    `appraise_good_count` BIGINT COMMENT '累积好评数',
    `appraise_mid_count` BIGINT COMMENT '累积中评数',
    `appraise_bad_count` BIGINT COMMENT '累积差评数',
    `appraise_default_count` BIGINT COMMENT '累积默认评价数'
 )COMMENT '商品主题宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwt/dwt_sku_topic/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### 1.5.4、优惠券主题表

```hive
DROP TABLE IF EXISTS dwt_coupon_topic;
CREATE EXTERNAL TABLE dwt_coupon_topic(
    `coupon_id` STRING COMMENT '优惠券ID',
    `get_last_1d_count` BIGINT COMMENT '最近1日领取次数',
    `get_last_7d_count` BIGINT COMMENT '最近7日领取次数',
    `get_last_30d_count` BIGINT COMMENT '最近30日领取次数',
    `get_count` BIGINT COMMENT '累积领取次数',
    `order_last_1d_count` BIGINT COMMENT '最近1日使用某券下单次数',
    `order_last_1d_reduce_amount` DECIMAL(16,2) COMMENT '最近1日使用某券下单优惠金额',
    `order_last_1d_original_amount` DECIMAL(16,2) COMMENT '最近1日使用某券下单原始金额',
    `order_last_1d_final_amount` DECIMAL(16,2) COMMENT '最近1日使用某券下单最终金额',
    `order_last_7d_count` BIGINT COMMENT '最近7日使用某券下单次数',
    `order_last_7d_reduce_amount` DECIMAL(16,2) COMMENT '最近7日使用某券下单优惠金额',
    `order_last_7d_original_amount` DECIMAL(16,2) COMMENT '最近7日使用某券下单原始金额',
    `order_last_7d_final_amount` DECIMAL(16,2) COMMENT '最近7日使用某券下单最终金额',
    `order_last_30d_count` BIGINT COMMENT '最近30日使用某券下单次数',
    `order_last_30d_reduce_amount` DECIMAL(16,2) COMMENT '最近30日使用某券下单优惠金额',
    `order_last_30d_original_amount` DECIMAL(16,2) COMMENT '最近30日使用某券下单原始金额',
    `order_last_30d_final_amount` DECIMAL(16,2) COMMENT '最近30日使用某券下单最终金额',
    `order_count` BIGINT COMMENT '累积使用(下单)次数',
    `order_reduce_amount` DECIMAL(16,2) COMMENT '使用某券累积下单优惠金额',
    `order_original_amount` DECIMAL(16,2) COMMENT '使用某券累积下单原始金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '使用某券累积下单最终金额',
    `payment_last_1d_count` BIGINT COMMENT '最近1日使用某券支付次数',
    `payment_last_1d_reduce_amount` DECIMAL(16,2) COMMENT '最近1日使用某券优惠金额',
    `payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日使用某券支付金额',
    `payment_last_7d_count` BIGINT COMMENT '最近7日使用某券支付次数',
    `payment_last_7d_reduce_amount` DECIMAL(16,2) COMMENT '最近7日使用某券优惠金额',
    `payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日使用某券支付金额',
    `payment_last_30d_count` BIGINT COMMENT '最近30日使用某券支付次数',
    `payment_last_30d_reduce_amount` DECIMAL(16,2) COMMENT '最近30日使用某券优惠金额',
    `payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日使用某券支付金额',
    `payment_count` BIGINT COMMENT '累积使用(支付)次数',
    `payment_reduce_amount` DECIMAL(16,2) COMMENT '使用某券累积优惠金额',
    `payment_amount` DECIMAL(16,2) COMMENT '使用某券累积支付金额',
    `expire_last_1d_count` BIGINT COMMENT '最近1日过期次数',
    `expire_last_7d_count` BIGINT COMMENT '最近7日过期次数',
    `expire_last_30d_count` BIGINT COMMENT '最近30日过期次数',
    `expire_count` BIGINT COMMENT '累积过期次数'
)comment '优惠券主题表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwt/dwt_coupon_topic/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### 1.5.5、活动主题宽表

```hive
DROP TABLE IF EXISTS dwt_activity_topic;
CREATE EXTERNAL TABLE dwt_activity_topic(
    `activity_rule_id` STRING COMMENT '活动规则ID',
    `activity_id` STRING  COMMENT '活动ID',
    `order_last_1d_count` BIGINT COMMENT '最近1日参与某活动某规则下单次数',
    `order_last_1d_reduce_amount` DECIMAL(16,2) COMMENT '最近1日参与某活动某规则下单优惠金额',
    `order_last_1d_original_amount` DECIMAL(16,2) COMMENT '最近1日参与某活动某规则下单原始金额',
    `order_last_1d_final_amount` DECIMAL(16,2) COMMENT '最近1日参与某活动某规则下单最终金额',
    `order_count` BIGINT COMMENT '参与某活动某规则累积下单次数',
    `order_reduce_amount` DECIMAL(16,2) COMMENT '参与某活动某规则累积下单优惠金额',
    `order_original_amount` DECIMAL(16,2) COMMENT '参与某活动某规则累积下单原始金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '参与某活动某规则累积下单最终金额',
    `payment_last_1d_count` BIGINT COMMENT '最近1日参与某活动某规则支付次数',
    `payment_last_1d_reduce_amount` DECIMAL(16,2) COMMENT '最近1日参与某活动某规则支付优惠金额',
    `payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日参与某活动某规则支付金额',
    `payment_count` BIGINT COMMENT '参与某活动某规则累积支付次数',
    `payment_reduce_amount` DECIMAL(16,2) COMMENT '参与某活动某规则累积支付优惠金额',
    `payment_amount` DECIMAL(16,2) COMMENT '参与某活动某规则累积支付金额'
) COMMENT '活动主题宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwt/dwt_activity_topic/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

### **1.5.6、地区主题宽表**

```hive
DROP TABLE IF EXISTS dwt_area_topic;
CREATE EXTERNAL TABLE dwt_area_topic(
    `province_id` STRING COMMENT '编号',
    `visit_last_1d_count` BIGINT COMMENT '最近1日访客访问次数',
    `login_last_1d_count` BIGINT COMMENT '最近1日用户访问次数',
    `visit_last_7d_count` BIGINT COMMENT '最近7访客访问次数',
    `login_last_7d_count` BIGINT COMMENT '最近7日用户访问次数',
    `visit_last_30d_count` BIGINT COMMENT '最近30日访客访问次数',
    `login_last_30d_count` BIGINT COMMENT '最近30日用户访问次数',
    `visit_count` BIGINT COMMENT '累积访客访问次数',
    `login_count` BIGINT COMMENT '累积用户访问次数',
    `order_last_1d_count` BIGINT COMMENT '最近1天下单次数',
    `order_last_1d_original_amount` DECIMAL(16,2) COMMENT '最近1天下单原始金额',
    `order_last_1d_final_amount` DECIMAL(16,2) COMMENT '最近1天下单最终金额',
    `order_last_7d_count` BIGINT COMMENT '最近7天下单次数',
    `order_last_7d_original_amount` DECIMAL(16,2) COMMENT '最近7天下单原始金额',
    `order_last_7d_final_amount` DECIMAL(16,2) COMMENT '最近7天下单最终金额',
    `order_last_30d_count` BIGINT COMMENT '最近30天下单次数',
    `order_last_30d_original_amount` DECIMAL(16,2) COMMENT '最近30天下单原始金额',
    `order_last_30d_final_amount` DECIMAL(16,2) COMMENT '最近30天下单最终金额',
    `order_count` BIGINT COMMENT '累积下单次数',
    `order_original_amount` DECIMAL(16,2) COMMENT '累积下单原始金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '累积下单最终金额',
    `payment_last_1d_count` BIGINT COMMENT '最近1天支付次数',
    `payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1天支付金额',
    `payment_last_7d_count` BIGINT COMMENT '最近7天支付次数',
    `payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7天支付金额',
    `payment_last_30d_count` BIGINT COMMENT '最近30天支付次数',
    `payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30天支付金额',
    `payment_count` BIGINT COMMENT '累积支付次数',
    `payment_amount` DECIMAL(16,2) COMMENT '累积支付金额',
    `refund_order_last_1d_count` BIGINT COMMENT '最近1天退单次数',
    `refund_order_last_1d_amount` DECIMAL(16,2) COMMENT '最近1天退单金额',
    `refund_order_last_7d_count` BIGINT COMMENT '最近7天退单次数',
    `refund_order_last_7d_amount` DECIMAL(16,2) COMMENT '最近7天退单金额',
    `refund_order_last_30d_count` BIGINT COMMENT '最近30天退单次数',
    `refund_order_last_30d_amount` DECIMAL(16,2) COMMENT '最近30天退单金额',
    `refund_order_count` BIGINT COMMENT '累积退单次数',
    `refund_order_amount` DECIMAL(16,2) COMMENT '累积退单金额',
    `refund_payment_last_1d_count` BIGINT COMMENT '最近1天退款次数',
    `refund_payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1天退款金额',
    `refund_payment_last_7d_count` BIGINT COMMENT '最近7天退款次数',
    `refund_payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7天退款金额',
    `refund_payment_last_30d_count` BIGINT COMMENT '最近30天退款次数',
    `refund_payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30天退款金额',
    `refund_payment_count` BIGINT COMMENT '累积退款次数',
    `refund_payment_amount` DECIMAL(16,2) COMMENT '累积退款金额'
) COMMENT '地区主题宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwt/dwt_area_topic/'
TBLPROPERTIES ("parquet.compression"="gzip");
```

## 1.6、电商系统构建表（ADS层数据构建）



























​	