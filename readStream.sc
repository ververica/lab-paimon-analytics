import $ivy.`org.apache.paimon:paimon-bundle:0.6.0-incubating`
import $ivy.`org.apache.paimon:paimon-s3:0.6.0-incubating`
import $ivy.`org.apache.flink:flink-shaded-hadoop-2-uber:2.8.3-10.0`
import $ivy.`org.apache.hadoop:hadoop-aws:2.8.3`

import $ivy.`org.slf4j:slf4j-log4j12:1.7.15`

import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.table.Table
import org.apache.paimon.table.source.AbstractInnerTableScan
import org.apache.paimon.types.{DataTypes, RowType}
import org.apache.paimon.catalog.{
  Catalog,
  Identifier,
  CatalogContext,
  CatalogFactory
}
import org.apache.paimon.fs.Path
import org.apache.paimon.options.CatalogOptions.WAREHOUSE
import org.apache.paimon.options.Options

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.concurrent.duration._

val warehouse = "s3://vvc-stage-streamhouse/datalake"
val database = "default"

def createFilesystemCatalog(s3Keys: Map[String, String]) = {
  val options =
    Options.fromMap((s3Keys + (WAREHOUSE.key() -> warehouse)).asJava)
  val context = CatalogContext.create(options)
  CatalogFactory.createCatalog(context)
}

def getTable(s3Keys: Map[String, String]) = {
  val identifier = Identifier.create(database, "country_sales")
  val catalog = createFilesystemCatalog(s3Keys)
  catalog.getTable(identifier) -> catalog
}

val s3Access = List(
  sys.env.get("S3_ACCESS_KEY").map(v => "s3.access-key" -> v),
  sys.env.get("S3_SECRET_KEY").map(v => "s3.secret-key" -> v)
).flatten.toMap
val (table, catalog) = getTable(s3Access)

val builder = new PredicateBuilder(
  RowType.of(DataTypes.STRING(), DataTypes.INT(), DataTypes.INT())
)

val projection = Array(0, 1, 2)
val readBuilder = table
  .copy(Map("consumer-id" -> "myId").asJava)
  .newReadBuilder()
  .withProjection(projection)

val columnWidth = 25

def formatRow(row: Any*) =
  row.map(_.toString().padTo(columnWidth, ' ').take(columnWidth)).mkString

def printReport(table: List[(String, String)], values: Iterable[String]) = {
  print("\u001b[2J\u001b[;H")
  println(
    s"last read size: ${table.size}, last update: ${new java.util.Date()}"
  )
  values.foreach(println)
}

val scan = readBuilder.newStreamScan()
var checkpoint = Option.empty[Long]
val report = mutable.SortedMap[String, String]()
val scanInterval = 2.seconds

while (true)
  try {
    val splits = scan.plan().splits()
    checkpoint = Some(scan.checkpoint())
    val read = readBuilder.newRead()
    val reader = read.createReader(splits)

    val table =
      (for row <- reader.toCloseableIterator.asScala
      yield row.getString(0).toString ->
        formatRow(
          row.getRowKind(),
          row.getString(0),
          row.getInt(1),
          row.getInt(2)
        )).toList
    report ++= table
    printReport(table, report.values)
    Thread.sleep(scanInterval.toMillis)
  } catch
    case e: Exception =>
      e.printStackTrace()
      checkpoint.foreach(c => scan.restore(c.toLong))
