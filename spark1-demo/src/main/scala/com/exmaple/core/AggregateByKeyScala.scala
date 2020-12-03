package com.exmaple.core

import com.exmaple.common.CommSparkContextSca

object AggregateByKeyScala {
  def main(args: Array[String]): Unit = {

    val sc = CommSparkContextSca.getsc()

    val list = Array("haddop spark", "spark hive")

    val lines = sc.parallelize(list);

    val words = lines.flatMap(line => line.split(" "))

    val word = words.map(word => (word, 1));

    //val wordcount = word.aggregateByKey(0)((x1,y1) =>(x1 + y1),(x1,y1) =>(x1 + y1));
    val wordcount = word.aggregateByKey(0)(_ + _, _ + _);


    wordcount.foreach(_wordcount => (System.out.println(_wordcount._1 + "  :  " + _wordcount._2)));

  }


}
