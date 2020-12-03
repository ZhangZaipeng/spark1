package com.exmaple.core

class SecondSortKeyScala(val first : String,val second : Int)
  extends Ordered[SecondSortKeyScala] with Serializable {
  override def compare(that: SecondSortKeyScala)  : Int = {
        val comp = this.first.compareTo(that.first);
    if(comp == 0){
        return this.second.compareTo(that.second);
    }
        return comp;
  }
}
