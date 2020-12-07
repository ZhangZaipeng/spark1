package com.exmaple.sparkSql;

import java.io.Serializable;

public class Person implements Serializable {

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getAge() {
    return age;
  }

  public void setAge(long age) {
    this.age = age;
  }

  private String name;
  private long age;
}
