//> using dep "org.flinkextended::flink-scala-api:1.17.1_1.1.0"

//> using dep "org.apache.flink:flink-clients:1.17.1"
//> using dep "org.apache.flink:flink-connector-files:1.17.1"
//> using dep "org.apache.flink:flink-connector-base:1.17.1"
//> using dep "org.apache.flink:flink-table-runtime:1.17.1"
//> using dep "org.apache.flink:flink-table-planner-loader:1.17.1"
//> using dep "org.apache.flink:flink-runtime-web:1.17.1"

//> using dep "org.slf4j:slf4j-log4j12:1.7.15"

// Paimon Deps
//> using dep "org.apache.flink:flink-shaded-hadoop-2-uber:2.8.3-10.0"
//> using dep "org.apache.paimon:paimon-flink-1.17:0.6.0-incubating"

// CDC Deps
//> using dep "com.ververica:flink-sql-connector-mysql-cdc:2.4.2"
//> using dep "com.mysql:mysql-connector-j:8.2.0"

import org.apache.flink.configuration.{Configuration, ConfigConstants}
import org.apache.flink.configuration.RestOptions.BIND_PORT
import org.apache.flinkx.api.*

import org.apache.paimon.flink.action.cdc.mysql.MySqlSyncTableAction

import scala.jdk.CollectionConverters._

val warehouse = "s3a://vvc-stage-streamhouse/datalake"
val catalog = "datalake"
val database = "default"
val tableName = "customers"
val mysqlConfig = Map(
  "hostname" -> "stage-blogpost-rds-vvcrdsclusterwriter0555f50e-ap4atqaod1py.clrjoknilssk.us-west-1.rds.amazonaws.com",
  "port" -> "3306",
  "username" -> "user",
  "password" -> "password",
  "database-name" -> "demo",
  "table-name" -> tableName
).asJava
val catalogConfig = Map(
  "metastore" -> "filesystem",
  "warehouse" -> warehouse
).asJava
val tableConfig = Map(
  "changelog-producer" -> "input",
  "sink.parallelism" -> "2"
).asJava

val action = new MySqlSyncTableAction(
  warehouse,
  database,
  tableName,
  catalogConfig,
  mysqlConfig
)
  .withTableConfig(tableConfig)
  .withPrimaryKeys("id")

val env = StreamExecutionEnvironment.getExecutionEnvironment
action
  .withStreamExecutionEnvironment(env.getJavaEnv)
  .build()