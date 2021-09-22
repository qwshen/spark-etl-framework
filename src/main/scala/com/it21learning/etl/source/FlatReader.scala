package com.it21learning.etl.source

import com.it21learning.etl.common.{FlatReadActor, ExecutionContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * To load a text file.
 *
 * The output dataframe has the following columns:
 *   - row_value: the content of each row
 *   - row_no: the sequence number of each row.
 */
final class FlatReader extends FlatReadActor[FlatReader] {
  /**
   * Load the flat-file
   *
   * @param ctx - the execution context
   * @param session - the spark-session
   */
  def run(ctx: ExecutionContext)(implicit session: SparkSession): Option[DataFrame] = for {
    uri <- this._fileUri
  } yield Try {
    import session.implicits._
    val rdd = session.sparkContext.textFile(uri)
    this._format match {
      case Seq(_, _ @ _*) =>
        val rawRdd = rdd.map(r => this._format.map(f => r.substring(f.startPos - 1, f.startPos + f.length - 1))).map(r => Row.fromSeq(r))
        val rawSchema = StructType(this._schema.get.fields.map(field => StructField(field.name, StringType, field.nullable)))
        session.createDataFrame(rawRdd, rawSchema)
          .select(this._schema.get.fields.map(field => col(field.name).cast(field.dataType)): _*)
      case _ => rdd.zipWithIndex.toDF("row_value", "row_no")
    }
  } match {
    case Success(df) => df
    case Failure(ex) => throw new RuntimeException(s"Cannot load the flat file - $uri", ex)
  }
}
