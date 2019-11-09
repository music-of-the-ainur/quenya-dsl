package com.github.music.of.the.ainur.quenya.compiler

import org.apache.spark.sql.types._

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

object ParserQuenyaDsl extends JavaTokenParsers {
  override val skipWhitespace = false
 
  def dsl: Parser[List[Statement]] = repsep(expression,"""[\n\r]*""".r) ^^ (List() ++ _ )
  def expression: Parser[Statement] = precedence ~ col ~ operator ^^ {
    case prec ~ cl ~ op =>
      op match {
        case al ~ ":" ~ dt => Statement(prec,cl,DOLLAR,al.toString(),Some(dt.asInstanceOf[DataType]))
        case al:String => Statement(prec,cl,AT,al)
      }
  }
  def precedence: Parser[Int] = """[\t\s]*""".r ^^ (prec => prec.replaceAll(" ","""\t""").count(_ == '\t'))
  def col: Parser[StateSelect] = """[0-9A-Za-z._]+""".r ~ opt(element) ^^ {
    case a ~ Some(b) => StateSelect(a,b)
    case a ~ None => StateSelect(a,None)
  }
  def element: Parser[Option[String]] = "[" ~> opt("""\d+""".r) <~ "]"
  def operator: Parser[Any] = at | dollar
  def at: Parser[String] = "@" ~> alias
  def dollar : Parser[Any] = "$" ~> alias ~ ":" ~ datatype
  def alias : Parser[String] = "[0-9a-zA-Z_]+".r
  def datatype : Parser[DataType] = ("BinaryType" ^^ (dt => BinaryType)
    | "FloatType" ^^ (dt => FloatType)
    | "ByteType" ^^ (dt => ByteType)
    | "IntegerType" ^^ (dt => IntegerType)
    | "LongType" ^^ (dt => LongType)
    | "BooleanType" ^^ (dt => BooleanType)
    | "StringType" ^^ (dt => StringType)
    | "TimestampType" ^^ (dt => TimestampType)
    | "DecimalType" ^^ (dt => DecimalType(0,10))
    | "DoubleType" ^^ (dt => DoubleType)
    | "ShortType" ^^ (dt => ShortType))
}
