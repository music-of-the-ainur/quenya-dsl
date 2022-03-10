package com.github.music.of.the.ainur.quenya

import com.github.music.of.the.ainur.quenya.compiler.{CombinatorParser, SparkCodeGenerator}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer

object QuenyaDSL extends CombinatorParser with SparkCodeGenerator with Serializable {

  def printDsl(df:DataFrame,shortName:Boolean = true): Unit = {
    println(getDsl(df,shortName))
  }

  def getDsl(df:DataFrame,shortName:Boolean = true): String = {
    var dslBuilder = ListBuffer[String]()
    df.schema.fields.foreach( f => {
      generator(f.name,f.dataType,List(f.name),shortName = shortName,dslBuilder = dslBuilder)
    })
    dslBuilder.mkString("\n")
  }

  private def generator(
    name:String,
    dataType:DataType,
    fields:List[String],
    precedence:Int = 0,
    shortName:Boolean = true,
    dslBuilder:ListBuffer[String]): Unit =
    { 
      dataType match {
        case  fieldType @ (BinaryType | FloatType | ByteType 
| IntegerType | LongType | BooleanType | StringType |
            TimestampType | DoubleType | ShortType) => dslBuilder += s"""${fieldGen(fields,precedence)}$$${aliasGen(fields,shortName)}:${fieldType.toString}"""
        case ArrayType(dt,_) => {
          val alias = aliasGen(fields,shortName)
          dslBuilder += s"""${fieldGen(fields,precedence)}@$alias"""
          generator(name,dt, List(alias), precedence + 1,dslBuilder = dslBuilder)
        }
        case StructType(fieldsStruct) => fieldsStruct.map(fd => generator(fd.name,fd.dataType,fields :+ fd.name,precedence,dslBuilder = dslBuilder))
      }
    }

  private def aliasGen(fields: List[String],shortName:Boolean): String = 
    if(shortName)
      fields.last
    else
      fields.mkString("_")

  private def fieldGen(fields: List[String],precedence: Int): String =
    (0 until precedence).map(_ => "\t").mkString + fields.mkString(".")
}
