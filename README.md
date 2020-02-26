# Quenya DSL

[![Build Status](https://travis-ci.org/music-of-the-ainur/quenya-dsl.svg?branch=master)](https://travis-ci.org/music-of-the-ainur/quenya-dsl)
[![Gitter Community](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/music-of-the-ainur/community)

Adding Quenya DSL dependency to your sbt build:

```
libraryDependencies += "com.github.music-of-the-ainur" %% "quenya-dsl" % "1.0.2-2.2"
```

## Introduction
Quenya DSL(Domain Specific Language) is a language that simplifies the task to parser complex semi-structured data.

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
   "weapon":[ 
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
weapon@weapon
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
   "weapon":[ 
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

You can generate a DSL based on a DataFrame:

```scala
import com.github.music.of.the.ainur.quenya.QuenyaDSL

val df:DataFrame = ...
val quenyaDsl = QuenyaDSL
quenyaDsl.printDsl(df)
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

## Requirements

| Software     | Version |
|--------------|---------|
| Java         | 8       |
| Scala        | 2.11    |
| Apache Spark | 2.2     |

## Author
Daniel Mantovani [daniel.mantovani@modakanalytics.com](mailto:daniel.mantovani@modakanalytics.com)

## Sponsor
[![Modak Analytics](/docs/img/modak_analytics.png)](http://www.modakanalytics.com)
