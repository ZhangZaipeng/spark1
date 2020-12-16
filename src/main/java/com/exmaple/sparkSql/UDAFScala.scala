package com.exmaple.sparkSql

import com.exmaple.common.{Comm, CommSparkSessionScala}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

object UDAFScala {


  def main(args: Array[String]): Unit = {
    val spark = CommSparkSessionScala.getSparkSession()
    val userPath = Comm.fileDirPath + "user.json"

    spark.read.json(userPath).createOrReplaceTempView("user")

    spark.udf.register("count", new MyCount)
    spark.sql("select count(deptName),deptName from user group by deptName").show()
  }

  class MyCount extends UserDefinedAggregateFunction {
    /**
     * 指的就是输入数据的类型
     *
     * @return
     */
    override def inputSchema: StructType = {
      StructType(Array(StructField("str", StringType, true)));
    }

    /**
     * 中间进行聚合的时候所处理的数据类型
     *
     * @return
     */
    override def bufferSchema: StructType = {
      StructType(Array(StructField("count", IntegerType, true)));
    }

    /**
     * 返回类型
     *
     * @return
     */
    override def dataType: DataType = {
      IntegerType
    }


    /**
     * 校验返回值
     *
     * @return
     */
    override def deterministic: Boolean = {
      true
    }


    /**
     * 为每个分组的数据执行初始化操作
     *
     * @param buffer
     */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0;
    }

    /**
     * 每个分组有新的数据进来的时候，如何进行分组对应的聚合值得计算
     *
     * @param buffer
     * @param input
     */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getAs[Int](0) + 1;
    }

    /**
     * 在每个节点上的聚合值要进行最后的合并mege
     *
     * @param buffer1
     * @param buffer2
     */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
    }

    /**
     * 返回最终的结果
     *
     * @param buffer
     * @return
     */
    override def evaluate(buffer: Row): Any = {
      buffer.getAs[Int](0);
    }
  }

}
