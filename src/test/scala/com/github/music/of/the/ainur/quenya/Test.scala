package com.github.music.of.the.ainur.quenya

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.spark.xml._

class Test extends FunSuite with BeforeAndAfter {
  val spark:SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val data = Seq(
"""{"name": {"nameOne": "Mithrandir","LastName": "Olórin","nickNames": ["Gandalf the Grey","Gandalf the White"]}, "race": "Maiar","age": 3500,"weapon": ["Glamdring", "Narya", "Wizard Staff"]}""",
"""{"name": {"nameOne": "Ilmarë","LastName": null, "nickNames": null}, "race": "Ainur","age": 4500,"weapon": ["Powers of the Ainur"]}""",
"""{"name": {"nameOne": "Morgoth","LastName": null, "nickNames": ["Bauglir","Belegurth","Belegûr","The Great Enemy","The Black Foe"]}, "race": "Ainur","age": 3500,"weapon": ["Powers of the Ainur","Grond","Mace","Sword"]}""",
"""{"name": {"nameOne": "Manwë","LastName": null, "nickNames": ["King of Arda,","Lord of the Breath of Arda","King of the Valar"]}, "race": "Ainur","age": 3500,"weapon": ["Powers of the Ainur"]}""")

  import spark.implicits._

  val df = spark.read.json(spark.sparkContext.parallelize(data).toDS())

  val quenyaDsl = QuenyaDSL

  val dsl = quenyaDsl.compile("""
 |age$age:LongType
 |name.LastName$LastName:StringType
 |name.nameOne$nameOne:StringType
 |name.nickNames[0]$nickNames:StringType
 |race$race:StringType
 |weapon@weapon
 |  weapon$weapon:StringType""".stripMargin)

  val dslDf = quenyaDsl.execute(dsl,df)
  val csvDf = spark.read.option("header","true").csv("src/test/resources/data.csv")
 
  val dslCount = dslDf.count()
  val csvCount = csvDf.count()

  test("number of records should match") {
    assert(dslCount == csvCount)
  }

  val diff = dslDf.as("dsl").join(csvDf.as("csv"),
    $"dsl.age" <=> $"csv.age" &&
    $"dsl.LastName" <=> $"csv.LastName" &&
    $"dsl.nameOne" <=> $"csv.nameOne"  &&
    $"dsl.nickNames" <=> $"csv.nickNames" &&
    $"dsl.race" <=> $"csv.race" &&
    $"dsl.weapon" <=> $"csv.weapon","leftanti").count()

  test("data should be exactly the same") {
    assert(diff == 0)
  }


  val xmlDf1 = xmlToDf("<foo><bar>1</bar><bar>1</bar></foo>")
  val xmlDf2 = xmlToDf("<foo><bar>1</bar></foo>")



  val xmlDsl = quenyaDsl.compile("""
      |bar@ba
      | ba$bar:LongType""".stripMargin)

  val xmlDslDf1 = quenyaDsl.execute(xmlDsl, xmlDf1)
  val xmlDslDf2 = quenyaDsl.execute(xmlDsl, xmlDf2)
  val xmlDslDf1Count = xmlDslDf1.count()
  val xmlDslDf2Count = xmlDslDf2.count()


  // test to check the number of elements in the df from first xml string are exactly 2
  test("number of records should be 2"){
    assert(xmlDslDf1Count == 2)
  }

  // test to check the number of elements in the df from second xml string are exactly 1
  test("number of records should be 1"){
    assert(xmlDslDf2Count == 1)
  }

  val checkXml1 = checkData(xmlDslDf1)
  val checkXml2 = checkData(xmlDslDf2)
  // test to check the data of df from first xml
  test("checking data for first xml"){
    assert(checkXml1)
  }

  // test to check the data of df from second xml
  test("checking data for second xml"){
    assert(checkXml2)
  }
  after {
    spark.stop()
  }

  // returns true if all the elements in the bar column of df are equal to 1
  def checkData(df:DataFrame): Boolean = {
    df.select("bar").as[Long].collect().forall(i=>i==1)
  }


//  function to convert xml string to df
  def xmlToDf(xml: String): DataFrame = {
    new XmlReader().xmlRdd(spark, spark.sparkContext.parallelize(Seq(xml)))
  }
}
