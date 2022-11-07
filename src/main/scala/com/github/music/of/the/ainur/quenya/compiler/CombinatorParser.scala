package com.github.music.of.the.ainur.quenya.compiler

import org.apache.spark.sql.types._
import org.apache.spark.sql.types.AbstractDataType
import scala.util.parsing.combinator._

/*
 Spark DSL Backus-Naur form.
 <dsl> ::= \{"[\r\n]*".r <precedence> <col> <operator> \}
 <precedence> ::= "[\s\t]*".r
 <col> ::= "a-zA-Z0-9_.".r  [ element ]
 <element> ::= "[" "\d".r "]"
 <operator> ::= <@> | <$>
 <@> ::= @ <alias>
 <$> ::= $ <alias> : <datatype>
 <alias> ::= "0-9a-zA-Z_".r
 <datatype> ::= BinaryType | BooleanType | StringType | TimestampType | DecimalType
 | DoubleType | FloatType | ByteType | IntegerType | LongType | ShortType
 */
private[quenya] trait CombinatorParser {
  val parser = ParserQuenyaDsl

  def compile(dsl: String):List[Statement] =
    parser.parseAll(parser.dsl,dsl) match {
      case parser.Success(r,n) => r
      case parser.Failure(msg,n) => throw ParserError(msg)
      case parser.Error(msg,n) => throw ParserError(msg)
    }
}

trait ParserUtil {
  def removeLiteral(content: String, literal: String): String = {
    if (content.head.toString == literal && content.last.toString == literal)
      content.substring(1, content.length - 1)
    else content
  }
}
object ParserQuenyaDsl extends JavaTokenParsers with ParserUtil {
  override val skipWhitespace = false

  def dsl: Parser[List[Statement]] = repsep(expression,"""[\n\r]*""".r) ^^ (List() ++ _ )
  def expression: Parser[Statement] = precedence ~ col ~ operator ^^ {
    case prec ~ cl ~ op =>
      op match {
        case al ~ Some(semicolumn) ~ dt => Statement(prec,cl,DOLLAR,al.toString(),dt.asInstanceOf[Option[DataType]])
        // neet to decompose "alias" otherwise it will like "foo~None" as alias.
        case al ~ None => al match {
          case a ~ None => Statement(prec,cl,DOLLAR,a.toString(),None)
        }
        case al:String => Statement(prec,cl,AT,al)
      }
  }
  def precedence: Parser[Int] = """^[\t\s]*""".r ^^ (prec => prec.replaceAll(" ","\t").count(_ == '\t'))
  def col: Parser[StateSelect] = """[\w.]+|`[\w. \-:;$]+`""".r ~ opt(element) ^^ {
    case a ~ Some(b) => createStateSelect(a,b)
    case a ~ None => createStateSelect(a,None)
  }
  private def createStateSelect(name: String, element: Option[String]): StateSelect =
    StateSelect(removeLiteral(name,"`"),element)

  def element: Parser[Option[String]] = "[" ~> opt("""\d+""".r) <~ "]"
  def operator: Parser[Any] = at | dollar
  def at: Parser[String] = "@" ~> alias
  def dollar : Parser[Any] = "$" ~> alias ~ opt(":") ~ datatype
  def alias : Parser[String] = """\w+|`[\w \-:;$]+`""".r ^^ {
    alias => removeLiteral(alias,"`")
  }
  def datatype : Parser[Option[DataType]] = ("BinaryType" ^^ (dt => Some(BinaryType))
    | "FloatType" ^^ (dt => Some(FloatType))
    | "ByteType" ^^ (dt => Some(ByteType))
    | "IntegerType" ^^ (dt => Some(IntegerType))
    | "LongType" ^^ (dt => Some(LongType))
    | "BooleanType" ^^ (dt => Some(BooleanType))
    | "StringType" ^^ (dt => Some(StringType))
    | "TimestampType" ^^ (dt => Some(TimestampType))
    | "DecimalType" ^^ (dt => Some(DecimalType(0,10)))
    | "DoubleType" ^^ (dt => Some(DoubleType))
    | "ShortType" ^^ (dt => Some(ShortType))
    | "" ^^ (dt => None))
}