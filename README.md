# Quenya-DSL

[![Build-Status](https://github.com/music-of-the-ainur/quenya-dsl/actions/workflows/quenya-dsl-githubactions.yml/badge.svg)](https://github.com/music-of-the-ainur/quenya-dsl/actions/workflows/quenya-dsl-githubactions.yml)

Adding Quenya-DSL dependency to your sbt build:

```
libraryDependencies += "com.github.music-of-the-ainur" %% "quenya-dsl" % "1.2.2-$SPARK_VERSION"
```

To run in spark-shell:

```
spark-shell --packages "com.github.music-of-the-ainur:quenya-dsl_2.12:1.2.0-$SPARK_VERSION"
```


Quenya-Dsl is available in [Maven Central](https://mvnrepository.com/artifact/com.github.music-of-the-ainur)
repository. 

| versions                   | Connector Artifact                                        |
|----------------------------|-----------------------------------------------------------|
| Spark 3.3.x and scala 2.13 | `com.github.music-of-the-ainur:quenya-dsl_2.13:1.2.2-3.3` |
| Spark 3.3.x and scala 2.12 | `com.github.music-of-the-ainur:quenya-dsl_2.12:1.2.2-3.3` |
| Spark 3.2.x and scala 2.12 | `com.github.music-of-the-ainur:quenya-dsl_2.12:1.2.2-3.2` |
| Spark 3.1.x and scala 2.12 | `com.github.music-of-the-ainur:quenya-dsl_2.12:1.2.2-3.1` |
| Spark 2.4.x and scala 2.12 | `com.github.music-of-the-ainur:quenya-dsl_2.12:1.2.2-2.4` |
| Spark 2.4.x and scala 2.11 | `com.github.music-of-the-ainur:quenya-dsl_2.11:1.2.2-2.4` |

## Introduction
Quenya-DSL(Domain Specific Language) is a language that simplifies the task to parser complex semi-structured data.

```scala

val inputDf: DataFrame = ...

val quenyaDsl = QuenyaDSL
val dsl = quenyaDsl.compile("""
    |uuid$id:StringType
    |id$id:LongType
    |code$area_code:LongType
    |names@name
    |	name.firstName$first_name:StringType
    |	name.secondName$second_name:StringType
    |	name.lastName$last_name:StringType
    |source_id$source_id:LongType
    |address[3]$zipcode:StringType""".stripMargin)
val df:DataFrame = quenyaDsl.execute(dsl,inputDf)
df.show(false)
```

## Operators

### $ i.e DOLLAR
Operator **$** i.e **dollar** is used to select.

Example:

DSL
```
name.nameOne$firstName:StringType
name.nickNames[0]$firstNickName:StringType
```

JSON
```json
{ 
   "name":{ 
      "nameOne":"Mithrandir",
      "LastName":"Olórin",
      "nickNames":[ 
         "Gandalf the Grey",
         "Gandalf the White"
      ]
   },
   "race":"Maiar",
   "age":"immortal",
   "weapons":[ 
      "Glamdring",
      "Narya",
      "Wizard Staff"
   ]
}
```

Output:

```
+----------+----------------+
|firstName |firstNickName   |
+----------+----------------+
|Mithrandir|Gandalf the Grey|
+----------+----------------+
```
### @ i.e AT
Operator **@** i.e **at** is used to explode arrays, "space" or "tab" is used to define the precedence.

Example:

DSL
```
weapons@weapon
    weapon$weapon:StringType
```

JSON
```json
{ 
   "name":{ 
      "nameOne":"Mithrandir",
      "LastName":"Olórin",
      "nickNames":[ 
         "Gandalf the Grey",
         "Gandalf the White"
      ]
   },
   "race":"Maiar",
   "age":"immortal",
   "weapons":[ 
      "Glamdring",
      "Narya",
      "Wizard Staff"
   ]
}
```

Output:

```
+------------+
|weapon      |
+------------+
|Glamdring   |
|Narya       |
|Wizard Staff|
+------------+
```
## Supported Types

* FloatType
* BinaryType
* ByteType
* BooleanType
* StringType
* TimestampType
* DecimalType
* DoubleType
* IntegerType
* LongType
* ShortType

## DSL Generator

You can generate the DSL from an existing DataFrame:

```scala
import com.github.music.of.the.ainur.quenya.QuenyaDSL

val df:DataFrame = ...
val quenyaDsl = QuenyaDSL
quenyaDsl.printDsl(df)
```

### getDsl
You can generate and asssign a DSL to variable based on a DataFrame:

```scala
import com.github.music.of.the.ainur.quenya.QuenyaDSL

val df:DataFrame = ...
val quenyaDsl = QuenyaDSL
val dsl = quenyaDsl.getDsl(df)
```


json:
```
{ 
   "name":{ 
      "nameOne":"Mithrandir",
      "LastName":"Olórin",
      "nickNames":[ 
         "Gandalf the Grey",
         "Gandalf the White"
      ]
   },
   "race":"Maiar",
   "age":"immortal",
   "weapon":[ 
      "Glamdring",
      "Narya",
      "Wizard Staff"
   ]
}
```

output:

```
age$age:StringType
name.LastName$name_LastName:StringType
name.nameOne$name_nameOne:StringType
name.nickNames@name_nickNames
        name_nickNames$name_nickNames:StringType
race$race:StringType
weapon@weapon
        weapon$weapon:StringType
```

You can _alias_ using the fully qualified name using ```printDsl(df,true)```, you should turn on in case of name conflict.

## How to Handle Special Characters



Use the literal backtick **``**  to handle special characters like space,semicolon,hyphen and colon.
Example:



json:
```
{
   "name":{
      "name One":"Mithrandir",
      "Last-Name":"Olórin",
      "nick:Names":[
         "Gandalf the Grey",
         "Gandalf the White"
      ]
   },
   "race":"Maiar",
   "age":"immortal",
   "weapon;name":[
      "Glamdring",
      "Narya",
      "Wizard Staff"
   ]
}
```



DSL:
```
age$age:StringType
`name.Last-Name`$`Last-Name`:StringType
`name.name One`$`name-One`:StringType
`name.nick:Names`@`nick:Names`
        `nick:Names`$`nick:Names`:StringType
race$race:StringType
`weapon;name`@`weapon;name`
        `weapon;name`$`weapon_name`:StringType
```

## Backus–Naur form

```
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
```

## Author
Daniel Mantovani [daniel.mantovani@modak.com](mailto:daniel.mantovani@modak.com)

## Sponsor
[![Modak Analytics](/docs/img/modak_analytics.png)](http://www.modak.com)
