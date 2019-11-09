package com.github.music.of.the.ainur.quenya.compiler

import org.apache.spark.sql.types.DataType

private[quenya] sealed trait Operator
case object DOLLAR extends Operator
case object AT extends Operator

private[quenya] case class StateSelect(name: String, element: Option[String])

private[quenya] case class Statement(
  val precedence: Int,
  val col: StateSelect,
  val operator: Operator,
  val alias: String,
  val dataType:Option[DataType] = None
)
