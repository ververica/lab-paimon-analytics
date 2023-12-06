import $ivy.`org.apache.flink:flink-shaded-hadoop-2-uber:2.8.3-10.0`
import $cp.lib.`paimon-bundle-0.7-20231201.002224-11.jar`

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

import scala.jdk.CollectionConverters._
import scala.collection.mutable

val warehouse = "file:/tmp/paimon"
val database = "default"

def createFilesystemCatalog() = {
  val context = CatalogContext.create(new Path(warehouse))
  CatalogFactory.createCatalog(context)
}

def getTable() = {
  val identifier = Identifier.create(database, "country_sales")
  val catalog = createFilesystemCatalog()
  catalog.getTable(identifier) -> catalog
}

val (table, catalog) = getTable()

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
  println(
    s"\u001b[2Jlast read size: ${table.size}, last update: ${new java.util.Date()}"
  )
  values.foreach(println)
}

val scan = readBuilder.newStreamScan()
println(s"scan = ${scan.asInstanceOf[AbstractInnerTableScan].options().toMap}")
var checkpoint = Option.empty[Long]
val report = mutable.SortedMap[String, String]()

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
    Thread.sleep(2000)
  } catch
    case e: Exception =>
      e.printStackTrace()
      checkpoint.foreach(c => scan.restore(c.toLong))
