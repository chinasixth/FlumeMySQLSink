版本：Flume-1.8.0
环境：Centos 6.5
JDK：1.8_171

需求：将具有一定格式的数据（有特定分隔符），由Flume采集，sink到MySQL（仅支持MySQL）

com.qinglianyun.flume.mysqlsink.MySQLSinkByBatch
说明：可以将数据批量写入MySQL
缺点：只能静态指定数据库表名
配置参数说明：
hostname	: MySQL服务主机名
port    	：访问MySQL的端口
user    	: 用户名
password	: 密码
database	: 数据库名，数据库必须已经存在
tableName	: 表名，数据库中的表需要手动创建
columnName	: 列名，多个列之间使用","分割，仅支持静态指定列名
separate	: 数据分隔符，默认是"\\^A"


com.qinglianyun.flume.mysqlsink.MySQLSinkByOne
说明： 将数据按条写入MySQL，可以动态指定表名
缺点： 当数据量很大时，一条一条写入数据，将会造成很大开销
配置参数说明：
同上