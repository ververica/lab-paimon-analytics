import $ivy.`org.apache.flink:flink-table-runtime:1.17.1`
import $ivy.`org.apache.flink:flink-table-planner-loader:1.17.1`
import $ivy.`org.apache.flink:flink-connector-base:1.17.1`
import $ivy.`org.apache.flink:flink-connector-files:1.17.1`
import $ivy.`org.apache.flink:flink-clients:1.17.1`
import $ivy.`org.slf4j:slf4j-log4j12:1.7.15`

import $ivy.`org.apache.flink:flink-shaded-hadoop-2-uber:2.8.3-10.0`
import $ivy.`org.apache.paimon:paimon-flink-1.17:0.5.0-incubating`

import org.apache.paimon.flink.action.MergeIntoAction

val warehouse = "file:/tmp/paimon"
val catalog = "my_catalog"
val database = "default"
new MergeIntoAction(warehouse, database, "country_sales")
  .withTargetAlias("cs")
  .withSourceTable("S")
  .withSourceSqls(
    s"""CREATE CATALOG $catalog WITH (
       'type'='paimon',
       'warehouse'='$warehouse'
    );""",
    s"USE CATALOG $catalog",
    s"CREATE TEMPORARY VIEW S AS SELECT distinct country from `$database`.customers"
  )
  .withMergeCondition("cs.country = S.country")
  .withNotMatchedInsert(null, "S.country, 0, 0")
  .withNotMatchedBySourceDelete(null)
  .run()