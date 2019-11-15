package com.github.music.of.the.ainur.quenya

import org.scalatest.{FunSuite, BeforeAndAfter}
import org.apache.spark.sql.SparkSession
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
  val xmlDf1 = spark.read
    .option("rowTag", "foo").xml("src/test/resources/xmldata1.xml")

  val xmlDf2 = spark.read
    .option("rowTag", "foo").xml("src/test/resources/xmldata2.xml")
  val xmlDf3 = spark.read
    .option("rowTag", "foo").xml("src/test/resources/xmldata3.xml")


  val xmlDsl = quenyaDsl.compile("""
      |bar@ba
      | ba$bar:LongType""".stripMargin)

  val xmlDslDf1 = quenyaDsl.execute(xmlDsl, xmlDf1)
  val xmlDslDf2 = quenyaDsl.execute(xmlDsl, xmlDf2)
  val xmlDslDf3 = quenyaDsl.execute(xmlDsl, xmlDf3)
  val xmlDslDf = xmlDslDf1.union(xmlDslDf2)
  val xmlDslDfCount = xmlDslDf.count()

  test("number of records should be 3"){
    assert(xmlDslDfCount == 3)
  }
  val dif = xmlDslDf.as("xml1").join(xmlDslDf3.as("xml2"),
    $"xml1.bar" <=> $"xml2.bar","leftanti").count()
  test("dif should be 0"){
    assert(dif==0)
  }
  after {
    spark.stop()
  }
}
