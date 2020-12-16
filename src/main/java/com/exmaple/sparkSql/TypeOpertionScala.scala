package com.exmaple.sparkSql

import com.exmaple.common.{Comm, CommSparkSessionScala}
import org.apache.spark.sql.SparkSession

object TypeOpertionScala {

  //case class Person(name : String , age: Option[Long])
  case class Person(name: String, age: Long)

  case class Employee(name1: String, salary: Long)


  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession();
    //    coaleceseAndRepartiton(spark)
    //    distinctAndDropDuplicates(spark)
    //    filter(spark)
    //    mapAndFlatMap(spark)
    //    joinWith(spark)
//    sort(spark)
    sample(spark)
  }


  /**
   * coalesce 和repatition 操作
   * 都是用来进行重分区的
   * 区别在于： coalesce 只能用于减少分区的数据，而且可以选择不发生shuffle
   * repartition 可以增加分区的数据，也可以减少分区的数据，必须会发生shuffle,相当于进行了一次重分区操作
   *
   */
  def coaleceseAndRepartiton(spark: SparkSession): Unit = {
    val personDF = spark.read.json(Comm.fileDirPath + "people.json");

    val personDFRepartition = personDF.repartition(3)

    println(personDFRepartition.rdd.partitions.size)

    val personDFCoalecese = personDFRepartition.coalesce(2);

    println(personDFCoalecese.rdd.partitions.size)
  }


  /**
   * distinct 是根据每一条数据进行完整内容的比对和去重
   * DropDuplicates : 可以根据指定的字段进行去重
   *
   * @param spark
   */
  def distinctAndDropDuplicates(spark: SparkSession): Unit = {

    val personDF = spark.read.json(Comm.fileDirPath + "people.json");

    personDF.distinct().show()

    personDF.dropDuplicates(Seq("name")).show()

  }

  /**
   * filter ： 根据我们自己的业务逻辑，如果返回true，就保留该元素，如果是false，就不保留该元素
   * except :  获取在当前的dataset中有，但是在另一个dataset中没有的元素
   * intersect : 获取两个dataset中的交集
   *
   * @param spark
   */
  def filter(spark: SparkSession): Unit = {
    import spark.implicits._
    val personDS1 = spark.read.json(Comm.fileDirPath + "people.json").as[Person]
    val personDS2 = spark.read.json(Comm.fileDirPath + "people1.json").as[Person]
    //val personDF  = spark.read.json(Comm.fileDirPath + "people.json");
    //personDS.filter(line => line.age != None).show()
    //val rdd = personDF.rdd.filter(line => line.getAs("age") != null);
    //    for (elem <- rdd.collect()) {
    //      System.out.println(elem)
    //    }
    // personDS1.except(personDS2).show()
    personDS1.intersect(personDS2).show()
  }


  /**
   * map : 将数据集中的每一条数据都做一个映射，返回一条数据
   * flatMap ：数据集中的每条数据都可以返回多条数据
   * mapPartition : 一次性对一个partition中的数据进行处理
   *
   * @param spark
   */
  def mapAndFlatMap(spark: SparkSession): Unit = {
    import spark.implicits._

    val personDS = spark.read.json(Comm.fileDirPath + "people.json").as[Person]

    personDS.filter("age is not null").map(line => (line.name, line.age + 10)).show()

    personDS.filter("age is not null").flatMap(line => {
      Seq(Person(line.name + "_1", line.age + 1000), Person(line.name + "_2", line.age + 1000))
    }).show()


    personDS.filter("age is not null").mapPartitions(line => {
      val result = scala.collection.mutable.ArrayBuffer[(String, Long)]()
      while (line.hasNext) {
        var person = line.next()
        result += ((person.name, person.age + 1000))
      }
      result.iterator
    }).show()

  }

  def joinWith(spark: SparkSession): Unit = {

    val personPath = Comm.fileDirPath + "people.json";
    val employeesPath = Comm.fileDirPath + "employees.json";
    import spark.implicits._
    val personDS = spark.read.json(personPath).as[Person];
    val employeesDS = spark.read.json(employeesPath).as[Employee];
    personDS.joinWith(employeesDS, $"name" === $"name1").show();
  }

  def sort(spark: SparkSession): Unit = {
    import spark.implicits._
    val employeePath = Comm.fileDirPath + "employees.json"

    val employeesDS = spark.read.json(employeePath).as[Employee]

    //employeesDS.sort($"salary".desc).show()

    employeesDS.toDF().createOrReplaceTempView("employee")

    val result = spark.sql("select * from employee a where 1=1 order by salary desc")

    result.show();
  }

  /**
   * randomSplit : 根据Array中元素的数量进行切分，然后再给定每个元素的值来保证对切分数据的权重
   * sample : 根据我们设定的比例进行随机抽取
   *
   * @param spark
   */
  def sample(spark: SparkSession): Unit = {
    import spark.implicits._

    val employeePath = Comm.fileDirPath + "employees.json"

    val employeesDS = spark.read.json(employeePath).as[Employee]

    //employeesDS.randomSplit(Array(2,5,1)).foreach(ds => ds.show())

    employeesDS.sample(false, 0.8).show()


  }

}
