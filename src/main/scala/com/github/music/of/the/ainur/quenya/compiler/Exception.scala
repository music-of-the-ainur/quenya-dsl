package com.github.music.of.the.ainur.quenya.compiler

sealed trait DSLException {
  self: Throwable =>
  val details: String
}

case class ParserError(expression: String) extends Exception(expression) with DSLException {
  override val details: String = "invalid expression: {$expression}"
}

case class NullDataType() extends Exception() with DSLException {
  override val details: String = """DataType is mandatory for "$""""
}

