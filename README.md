# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch04

# Environment
- Java 11
- Scala 2.13.8
- Spark 3.2.1

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch4_2.13-1.0.jar` (the same as name property in sbt file)

## 2, submit jar file, in project root dir
You can change the times of union. I just use 3 for my laptop(Mac, 4 core, 8G).
```
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.MainApp \
  target/scala-2.13/main-scala-ch4_2.13-1.0.jar noop | col | full
```

## 3, print

### Case: TransformationAndAction noop
```
# init records .................. 40781
1. Creating a session ........... 3493
2. Loading initial dataset ...... 7730
3. Building full dataset ........ 1062
4. Clean-up ..................... 20
5. Transformations .............. 0
6. Final action ................. 4449
# of records .................... 163124
```

### Case: TransformationAndAction col
```
# init records .................. 40781
1. Creating a session ........... 4074
2. Loading initial dataset ...... 9105
3. Building full dataset ........ 1384
4. Clean-up ..................... 20
5. Transformations .............. 142
6. Final action ................. 4800
# of records .................... 163124
```

### Case: TransformationAndAction full
```
# init records .................. 40781
1. Creating a session ........... 3908
2. Loading initial dataset ...... 10015
3. Building full dataset ........ 1239
4. Clean-up ..................... 18
5. Transformations .............. 278
6. Final action ................. 4158
# of records .................... 163124
```

### Case: TransformationExplain
```
== Physical Plan ==
Union
:- *(1) Project [Year#16, State#17, County#18, State FIPS Code#19, County FIPS Code#20, Combined FIPS Code#21, Birth Rate#22, Lower Confidence Limit#23 AS lcl#52, Upper Confidence Limit#24 AS ucl#62, ((cast(Lower Confidence Limit#23 as double) + cast(Upper Confidence Limit#24 as double)) / 2.0) AS avg#72, Lower Confidence Limit#23 AS lcl2#83, Upper Confidence Limit#24 AS ucl2#95]

:  +- FileScan csv [Year#16,State#17,County#18,State FIPS Code#19,County FIPS Code#20,Combined FIPS Code#21,Birth Rate#22,Lower Confidence Limit#23,Upper Confidence Limit#24] 
      Batched: false, 
      DataFilters: [], 
      Format: CSV, 
      Location: InMemoryFileIndex(1 paths)[file:/Users/changzhi/Spark/in-action/net.jgp.books.spark.ch04/data/NCH..., 
      PartitionFilters: [], 
      PushedFilters: [], 
      ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...
      
+- *(2) Project [Year#34, State#35, County#36, State FIPS Code#37, County FIPS Code#38, Combined FIPS Code#39, Birth Rate#40, Lower Confidence Limit#41 AS lcl#108, Upper Confidence Limit#42 AS ucl#109, ((cast(Lower Confidence Limit#41 as double) + cast(Upper Confidence Limit#42 as double)) / 2.0) AS avg#110, Lower Confidence Limit#41 AS lcl2#111, Upper Confidence Limit#42 AS ucl2#112]

   +- FileScan csv [Year#34,State#35,County#36,State FIPS Code#37,County FIPS Code#38,Combined FIPS Code#39,Birth Rate#40,Lower Confidence Limit#41,Upper Confidence Limit#42] 
      Batched: false, 
      DataFilters: [], 
      Format: CSV, 
      Location: InMemoryFileIndex(1 paths)[file:/Users/changzhi/Spark/in-action/net.jgp.books.spark.ch04/data/NCH..., 
      PartitionFilters: [], 
      PushedFilters: [], 
      ReadSchema: struct<Year:string,State:string,County:string,State FIPS Code:string,County FIPS Code:string,Comb...

```

## 4, Some diffcult case

### Inmutable value's scope problem, for example if block
I need to use if/else, otherwise var(not val)
```
    val cleanDF = bigDF.withColumnRenamed("Lower Confidence Limit", "lcl").
      withColumnRenamed("Upper Confidence Limit", "ucl")

    if (!mode.contains("noop")) {
      val colDF = cleanDF.
        withColumn("avg", ($"lcl" + $"ucl") / 2).
        withColumn("lcl2", $"lcl").
        withColumn("ucl2", $"ucl")

      if (mode.contains("full")) {
        val dropDF = colDF.
          drop($"avg").
          drop($"lcl2").
          drop($"ucl2")

        dropDF.collect()
      } else {
        colDF.collect()
      }
    } else {
      cleanDF.collect()
    }

    // can't get colDF, must use var ?
```
