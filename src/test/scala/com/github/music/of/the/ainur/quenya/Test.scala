package com.github.music.of.the.ainur.quenya

import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class Test extends AnyFunSuite with BeforeAndAfter {
  val spark = SparkSession.builder()
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Test case for DSL

  val data = Seq(
    """{"name": {"nameOne": "Mithrandir","LastName": "Olórin","nickNames": ["Gandalf the Grey","Gandalf the White"]}, "race": "Maiar","age": 3500,"weapon": ["Glamdring", "Narya", "Wizard Staff"]}""",
    """{"name": {"nameOne": "Ilmarë","LastName": null, "nickNames": null}, "race": "Ainur","age": 4500,"weapon": ["Powers of the Ainur"]}""",
    """{"name": {"nameOne": "Morgoth","LastName": null, "nickNames": ["Bauglir","Belegurth","Belegûr","The Great Enemy","The Black Foe"]}, "race": "Ainur","age": 3500,"weapon": ["Powers of the Ainur","Grond","Mace","Sword"]}""",
    """{"name": {"nameOne": "Manwë","LastName": null, "nickNames": ["King of Arda,","Lord of the Breath of Arda","King of the Valar"]}, "race": "Ainur","age": 3500,"weapon": ["Powers of the Ainur"]}""")

  import spark.implicits._

  val df = spark.read.json(spark.sparkContext.parallelize(data).toDS())

  val quenyaDsl = QuenyaDSL

  val dsl = quenyaDsl.compile(
    """
      |age$age:LongType
      |name.LastName$LastName:StringType
      |name.nameOne$nameOne:StringType
      |name.nickNames[0]$nickNames:StringType
      |race$race:StringType
      |weapon@weapon
      |  weapon$weapon:StringType""".stripMargin)

  val dslDf = quenyaDsl.execute(dsl, df)
  val csvDf = spark.read.option("header", "true").csv("src/test/resources/data.csv")

  test(dslDf, csvDf, "Test for Dsl")

  // Test case for getDsl method
  val dslMatch =
    """age$age:LongType
name.LastName$LastName:StringType
name.nameOne$nameOne:StringType
name.nickNames@nickNames
	nickNames$nickNames:StringType
race$race:StringType
weapon@weapon
	weapon$weapon:StringType"""

  test("Test for DSL Generate") {
    assert(dslMatch == quenyaDsl.getDsl(df))
  }


  //Test case for Dsl containing special characters
  val complexJsonDf = spark.read.option("multiLine", "true").json("src/test/resources/data.json")

  // For columns containing special characters like ;,:,- and spaces etc , need to include them in ``

  val complexDsl = quenyaDsl.compile(
    """Coffee.country.company$`coffee-company`:StringType
      |Coffee.country.id$`coffee-country-id`:LongType
      |`Coffee.sub region`@`sub:region`
      |        `sub region.full name`$`sub_region_full-name`:StringType
      |        `sub region.id`$`sub_region-id`:LongType
      |        `sub region.name`$`sub$region-name`:StringType
      |`brewing.sub-region`@`sub-region`
      |        `sub-region.id`$`brewing_sub_region:id`:LongType
      |        `sub-region.name`$`brewing:sub-region:name`:StringType
      |`brewing.world:country.company`$company1:StringType
      |`brewing.world:country.id`$id1:LongType
      |`brewing2.sub;region`@`sub;region`
      |        `sub;region.id2`$id2:LongType
      |        `sub;region.name`$name2:StringType
      |`brewing2.world;country.company2`$company:StringType
      |`brewing2.world;country.id`$`id$f`:LongType""".stripMargin)

  val complexJsonData = quenyaDsl.execute(complexDsl, complexJsonDf)
  complexJsonData.coalesce(1).write.mode("overwrite").parquet("src/test/resources/data/complexData.parquet")

  val complexDataParquet = spark.read.parquet("src/test/resources/data/complexData.parquet")
  test(complexJsonData, complexDataParquet, "Test for Complex Dsl with Special Characters")

  def test(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    testCount(df1, df2, name)
    testCompare(df1, df2, name)
  }

  def testCount(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    val count1 = df1.count()
    val count2 = df2.count()
    val count3 = spark.emptyDataFrame.count()
    test(s"Count Test:$name should match of count: ${df1.count()}") {
      assert(count1 == count2)
    }
    test(s"Count Test:$name should not match") {
      assert(count1 != count3)
    }
  }

  // Doesn't support nested type and we don't need it :)
  def testCompare(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    val diff = compare(df1, df2)
    test(s"Compare Test:$name should be zero") {
      assert(diff == 0)
    }
    test(s"Compare Test:$name, should not be able to join") {
      assertThrows[AnalysisException] {
        compare(df2, spark.emptyDataFrame)
      }
    }
  }

  private def compare(df1: DataFrame, df2: DataFrame): Long = {
    val diff = df1.as("df1").join(df2.as("df2"), joinExpression(df1), "leftanti")
    diff.show(false)
    diff.count
  }

  private def joinExpression(df1: DataFrame): Column =
    df1.schema.fields
      .map(field => col(s"df1.${field.name}") <=> col(s"df2.${field.name}"))
      .reduce((col1, col2) => col1.and(col2))

  after {
    spark.stop()
  }

}