package com.github.music.of.the.ainur.quenya

import com.github.music.of.the.ainur.quenya.compiler.{CombinatorParser, SparkCodeGenerator}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object QuenyaDSL extends CombinatorParser with SparkCodeGenerator with Serializable {

  private var enableShortName:Boolean = true
  def printDsl(df: DataFrame,shortName: Boolean = enableShortName): Unit = {
    enableShortName = shortName
    df.schema.fields.foreach( f => {
      generator(f.name,f.dataType,List(f.name))
    })
  }

  def getDslStr(df: DataFrame,shortName: Boolean = enableShortName): String = {
    enableShortName = shortName
    var dslStr= ""
    df.schema.fields.foreach(f => {
      val colVal = strGenerator(f.name, f.dataType, List(f.name))
      dslStr = dslStr + colVal + "\n"
    })
    dslStr.stripMargin
  }


  private def generator(
    name: String,
    dataType: DataType,
    fields:List[String],
    precedence:Int = 0): Unit = 
    { 
      dataType match {
        case  fieldType @ (BinaryType | FloatType | ByteType 
| IntegerType | LongType | BooleanType | StringType |
            TimestampType | DoubleType | ShortType) => println(s"""${fieldGen(fields,precedence)}$$${aliasGen(fields)}:${fieldType.toString}""")
        case ArrayType(dt,_) => {
          val alias = aliasGen(fields)
          println(s"""${fieldGen(fields,precedence)}@$alias""")
          generator(name,dt, List(alias), precedence + 1)
        }
        case StructType(fieldsStruct) => fieldsStruct.map(fd => generator(fd.name,fd.dataType,fields :+ fd.name,precedence))
      }
    }

  private def strGenerator(name: String, dataType: DataType,fields: List[String], precedence: Int = 0): String = {
    dataType match {
      case fieldType @ (BinaryType | FloatType | ByteType | IntegerType |
                        LongType | BooleanType | StringType | TimestampType | DoubleType |
                        ShortType) => {
        val s = s"""${fieldGen(fields, precedence)}$$${aliasGen(
          fields
        )}:${fieldType.toString}"""
        s
      }
      case ArrayType(dt, _) => {
        val alias = aliasGen(fields)
        var arrElement = s"""${fieldGen(fields, precedence)}@$alias"""
        var arrDSL: String = strGenerator(name, dt, List(alias), precedence + 1)
        arrElement + "\n" + arrDSL
      }
      case StructType(fieldsStruct) => {
        var structDsl = ""
        fieldsStruct.map(fd => {
          val structEle = {
            strGenerator(fd.name, fd.dataType, fields :+ fd.name, precedence)
          }
          if(structDsl.isEmpty){structDsl = structEle}
          else {structDsl=structDsl+ "\n"+structEle}
        })
        structDsl
      }
    }
  }

  private def aliasGen(fields: List[String]): String = 
    if(enableShortName)
      fields.last
    else
      fields.mkString("_")

  private def fieldGen(fields: List[String],precedence: Int): String =
    (0 until precedence).map(_ => "\t").mkString + fields.mkString(".")
}
