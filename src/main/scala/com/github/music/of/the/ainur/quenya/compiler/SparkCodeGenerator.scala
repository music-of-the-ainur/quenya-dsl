package com.github.music.of.the.ainur.quenya.compiler

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

private[quenya] trait SparkCodeGenerator extends LazyLogging {
  def execute(syntaxTree: List[Statement], df: DataFrame): DataFrame = {
    syntaxTree.foldLeft(df)({
      (df, s) =>
        Try(df.withColumn(s.alias, selectField(df, s))) match {
          case Success(d) => d
          case Failure(f) => {
            logger.warn(f.getMessage())
            if (s.operator == DOLLAR)
              withColumnNull(df, s)
            else 
              validateStruct(df, s)
          }
        }
    }).select(syntaxTree.filter(_.operator == DOLLAR).map(s => col(s.alias)): _*)
  }

  /*
   * If it's a "$" get the desired value or "explode" if it's a "@".
   */
  private def selectField(df: DataFrame, statement: Statement): Column =
    statement.operator match {
      case DOLLAR =>
        statement.col.element match {
          case Some(item) => col(statement.col.name).getItem(item.toInt).cast(statement.dataType.getOrElse(throw NullDataType()))
          case None => col(statement.col.name).cast(statement.dataType.getOrElse(throw NullDataType()))
        }
      case AT => explode_outer(col(statement.col.name))
    }

  /*
   * When you are reading a specific data structure individually, some records can be an array
   * while other records could be a single value struct. To keep the DSL consistent if the explode fails
   * it tries to select it.
   * <foo><bar>1</bar></foo>
   * <foo><bar>1</bar>><bar>1</bar></foo>
   * It's not need to cast the "null" value, since it's AT()
   */
  private def validateStruct(df: DataFrame, statement: Statement): DataFrame =
    Try(df.withColumn(statement.alias, col(statement.col.name))) match {
      case Success(d) => d
      case Failure(f) => df.withColumn(statement.alias, lit(null))
    }
  

  /*
   * Creates a "withColumn" statement with the correct "null" type i,e String,Long, Int etc
   * If the fiend is "@" i.e AT means you don't need to cast because it's not a Hive column
   * it's just an result of "explode" function. Fields from Hive table are of the type "$"
   * i.e Dollar
   */
  private def withColumnNull(df: DataFrame, statement: Statement): DataFrame =
    df.withColumn(statement.alias, lit(null).cast(statement.dataType.getOrElse(throw NullDataType())))

}
