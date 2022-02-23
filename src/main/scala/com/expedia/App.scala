package com.expedia

import org.apache.spark.sql.functions.{array_contains, col}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author ${user.name}
 */
object App {
  

  def main(args : Array[String]) {

    val spark = SparkSession.builder().master("local").appName("Email_Generator")
      .getOrCreate()

    val data = Seq(
      Row("James,,Smith",List("Java","Scala","C++"),"CA"),
      Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
      Row("Robert,,Williams",null,"NV")
    )

    val schema = new StructType()
      .add("name",StringType)
      .add("languagesAtSchool", ArrayType(StringType))
      .add("currentState", StringType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),schema)
    //where canbe used instead of withColumn
    val df2 = df.withColumn("Java Present", array_contains(col("languagesAtSchool"), "Java"))
    df2.show(false)

    val df3 = spark.sql("SELECT cardinality(array('b', 'd', 'c', 'a'))")
    df3.show()
    val df4 = spark.sql("SELECT array_contains(array(1, 2, 3), 2);")
    df4.show()
    val df5 = spark.sql("SELECT collect_set(col) FROM VALUES (1), (2), (1) AS tab(col);")
    df5.show()
    val df6 = spark.sql("SELECT collect_list(col) FROM VALUES (1), (2), (1) AS tab(col);")
    df6.show()

    val df7 = spark.sql("SELECT count(DISTINCT col) FROM VALUES (NULL), (5), (5), (10) AS tab(col);")
    df7.show()
    val df8 = spark.sql("SELECT date_add('2016-07-30', 1);")
    df8.show()
    val df9 = spark.sql("SELECT a, b, dense_rank(b) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);")
    df9.show()
    val df10 = spark.sql("SELECT format_number(12332.123456, 4);")
    df10.show()

    val df11 = spark.sql("SELECT map(1.0, '2', 3.0, '4');")
    df11.show()
    val df12 = spark.sql("SELECT map_from_arrays(array(1.0, 3.0), array('2', '4'));")
    df12.show()
    val df13 = spark.sql("SELECT map_keys(map(1, 'a', 2, 'b'));")
    df13.show()
    val df14 = spark.sql("SELECT to_date('2009-07-30 04:17:52') < to_date('2009-07-30 04:17:52');")
    df14.show()

    val df15 = spark.sql("SELECT to_unix_timestamp('2016-04-08', 'yyyy-MM-dd');")
    df15.show()
    val df16 = spark.sql("SELECT unix_timestamp('2016-04-08', 'yyyy-MM-dd');")
    df16.show()

  }

}
