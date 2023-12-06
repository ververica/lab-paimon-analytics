import $ivy.`org.flinkextended::flink-scala-api:1.17.1_1.1.0`
import $ivy.`org.apache.flink:flink-table-api-java:1.17.1`
import $ivy.`org.apache.flink:flink-table-api-java-bridge:1.17.1`
import $ivy.`org.apache.flink:flink-table-planner-loader:1.17.1`
import $ivy.`org.apache.flink:flink-table-runtime:1.17.1`
import $ivy.`org.apache.flink:flink-connector-base:1.17.1`
import $ivy.`org.apache.flink:flink-clients:1.17.1`
import $ivy.`org.apache.flink:flink-runtime-web:1.17.1`
import $ivy.`org.slf4j:slf4j-log4j12:1.7.15`

// Paimon Deps
import $ivy.`org.apache.flink:flink-shaded-hadoop-2-uber:2.8.3-10.0`
import $cp.lib.`paimon-flink-1.17-0.7-20231204.002054-14.jar`

// CDC Deps
import $ivy.`com.ververica:flink-sql-connector-mysql-cdc:2.4.2`
import $ivy.`com.mysql:mysql-connector-j:8.2.0`

import org.apache.flink.configuration.{Configuration, ConfigConstants}
import org.apache.flink.configuration.RestOptions.BIND_PORT
import org.apache.flinkx.api._

import org.apache.paimon.flink.action.cdc.mysql.MySqlSyncTableAction

import scala.jdk.CollectionConverters._

val warehouse = "file:/tmp/paimon"
val catalog = "my_catalog"
val database = "default"
val tableName = "customers"
val mysqlConfig = Map(
  "hostname" -> "localhost",
  "port" -> "3306",
  "username" -> "root",
  "password" -> "root",
  "database-name" -> "my_app_db",
  "table-name" -> tableName
).asJava
val catalogConfig = Map(
  "metastore" -> "filesystem",
  "warehouse" -> warehouse
).asJava
val tableConfig = Map(
  "changelog-producer" -> "lookup",
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

val config = Configuration.fromMap(
  Map(
    ConfigConstants.LOCAL_START_WEBSERVER -> "true",
    BIND_PORT.key -> "8082",
    "execution.checkpointing.interval" -> "3s"
  ).asJava
)

val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
action
  .withStreamExecutionEnvironment(env.getJavaEnv)
  .build()

env.execute("Sync MySQL Table: customers")
