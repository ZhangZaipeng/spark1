package com.exmaple.sparkSql;

import com.exmaple.common.Comm;
import com.exmaple.common.CommSparkSessionJava;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RDDToDFReflectionJava {

  public static void main(String[] args) {
    SparkSession spark = CommSparkSessionJava.getSparkSession();

    String filePath = Comm.fileDirPath + "people.txt";

    JavaRDD<Person> personRdd = spark.read().textFile(filePath).javaRDD().map(line -> {
      String[] _lines = line.split(",");
      Person person = new Person();
      person.setName(_lines[0]);
      person.setAge(Long.valueOf(_lines[1].trim()));
      return person;
    });

    Dataset<Row> personDF = spark.createDataFrame(personRdd, Person.class);

    personDF.createOrReplaceTempView("person");
    Dataset<Row> resultDF = spark.sql("select * from person a where a.age > 20");

    JavaRDD<Person> resulttRDD = resultDF.javaRDD().map(line -> {
      Person person = new Person();
      person.setName(line.getAs("name"));
      person.setAge(line.getAs("age"));
      return person;
    });

    for (Person person : resulttRDD.collect()) {
      System.out.println(person.getName() + " : " + person.getAge());
    }

  }
}
