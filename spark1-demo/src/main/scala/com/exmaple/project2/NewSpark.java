package com.exmaple.project2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class NewSpark {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .master("local")
        .appName("project2")
        .enableHiveSupport()
        .getOrCreate();

    String yesterday = Tools.getYesterday();
    Dataset<Row> pagePV = calculatePagePV(yesterday, spark);
//        Dataset<Row> pageUV = calculatePageUV(yesterday,spark);
//        double regis = calculateNewUserRegis(yesterday,spark);
//        Dataset<Row> sectionSort =  calculateSectionSort(yesterday,spark);

    writeData(pagePV, "stu_pagePV");
    //writeData(pageUV,"stu_pageUV");

  }

  /**
   * 计算每个每个页面的PV并且对其排序 排序的好处：排序后插入mysql，java web系统要查询每天pv top10的页面，直接查询mysql表limit10就可以
   * 如果我们这里不做排序的话，那么java web系统就要做排序，这样返回会影响java web系统的性能，影响用户访问的响应时间
   */
  private static Dataset<Row> calculatePagePV(String date, SparkSession spark) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("select cdate,pageid,pv_count from ");
    stringBuilder.append(" ( ");
    stringBuilder.append(" select cdate,pageid,count(1) pv_count from hivespark.news_access ");
    stringBuilder.append(" where action='view' and cdate='" + date + "' group by cdate,pageid ");
    stringBuilder.append(" ) ");
    stringBuilder.append(" t order by pv_count desc ");
    return spark.sql(stringBuilder.toString());
  }

  /**
   * 计算每天每个页面的uv及排序
   */
  private static Dataset<Row> calculatePageUV(String date, SparkSession spark) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("select cdate,pageid,uv_count from ");
    stringBuilder.append(" ( ");
    stringBuilder.append(" select cdate,pageid,count(userid) uv_count from hivespark.news_access ");
    stringBuilder
        .append(" where action='view' and cdate='" + date + "' group by cdate,pageid,userid ");
    stringBuilder.append(" ) ");
    stringBuilder.append(" t order by uv_count desc ");
    return spark.sql(stringBuilder.toString());
  }


  /**
   * 计算每天的新用户注册比例 1.先获取昨天所有访问行为中新用户的访问总数，既where cdate = 昨天 and userid is null 2.获取昨天总的注册用户数
   * 3.新用户的注册比例 = 昨天注册用户数   /   新用户的访问总数
   */
  private static double calculateNewUserRegis(String date, SparkSession spark) {

    String sql1 = "select count(1) allView from hivespark.news_access " +
        " where action ='view' and cdate = '" + date + "' and userid = 'null'";

    String sql2 = "select count(1) allRegis from hivespark.news_access " +
        " where action='register' and cdate = '" + date + "'";

    long allView = (long) spark.sql(sql1).collectAsList().get(0).get(0);
    long allRegis = (long) spark.sql(sql2).collectAsList().get(0).get(0);

    double rate = (double) allRegis / (double) allView;

    double res = Tools.formatDouble(rate, 2);
    return res;

  }


  private static Dataset<Row> calculateSectionSort(String date, SparkSession spark) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("select cdate,section,se_count from ");
    stringBuilder.append(" ( ");
    stringBuilder.append(" select cdate,section,count(1) se_count from hivespark.news_access ");
    stringBuilder.append(" where action='view' and cdate='" + date + "' group by cdate,section ");
    stringBuilder.append(" ) ");
    stringBuilder.append(" t order by se_count desc ");
    return spark.sql(stringBuilder.toString());
  }


  private static void writeData(Dataset<Row> dataFrame, String tableName) {
    dataFrame.write()
        .format("jdbc")
        .option("url", "jdbc:mysql://bigdata-pro-m03.kfk.com/spark")
        .option("dbtable", tableName)
        .option("user", "root")
        .option("password", "123456")
        .save();
  }


}
