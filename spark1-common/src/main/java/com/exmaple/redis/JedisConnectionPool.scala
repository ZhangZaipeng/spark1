package com.exmaple.redis

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @Description ：
 * @Tauthor ZhangZaipeng
 * @Tdata 2020/8/21   11:24
 */
object JedisConnectionPool {

  val config = new JedisPoolConfig()
  //最大分配对象
  config.setMaxTotal(1024)
  // 连接池中的最小空闲连接
  config.setMinIdle(1)
  //最大能保持 idel状态的对象
  config.setMaxIdle(10)
  //当池内没有返回对象时，最大等待时间
  config.setMaxWaitMillis(-1)
  //当调用borrow Object方法时，是否进行有效性检查
  config.setTestOnBorrow(false)
  //当调用return Object方法时，是否进行有效性检查
  config.setTestOnReturn(false)

  //5000代表超时时间（5秒）
  val pool = new JedisPool(config, "192.172.1.40", 46379, 5000, "123456")

  private def getJedis: Jedis = {
    pool.getResource
  }

  private def closeJedis(jedis: Jedis): Unit = {
    if (jedis != null) jedis.close()
  }

  def exists(cacheKey: String): Boolean = {
    var jedis : Jedis  = null
    try {
      jedis = getJedis
      return jedis.exists(cacheKey)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally closeJedis(jedis)
    false;
  }

  def get(cacheKey: String): String = {
    var jedis : Jedis  = null
    try {
      jedis = getJedis
      return jedis.get(cacheKey)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally closeJedis(jedis)
    null
  }

  def set(cacheKey: String, cacheValue: String): Boolean = {
    var jedis : Jedis  = null
    try {
      jedis = getJedis
      return jedis.set(cacheKey, cacheValue) != null
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally closeJedis(jedis)
    false
  }

  def incr(cacheKey: String) : Long = {
    var jedis : Jedis  = null
    try {
      jedis = getJedis
      return jedis.incr(cacheKey)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally closeJedis(jedis)
    0
  }

  def incrBy(cacheKey:String, num:Long) : Long = {
    var jedis : Jedis  = null
    try {
      jedis = getJedis
      return jedis.incrBy(cacheKey, num)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally closeJedis(jedis)
    0
  }

  def main(args: Array[String]) {

    val jedis = JedisConnectionPool.getJedis
    jedis.set("income", "1000")
    val r1 = jedis.get("xiaoyang")
    println(r1)
    jedis.incrBy("xiaoyang", -50)
    val r2 = jedis.get("xiaoyang")
    println(r2)
    closeJedis(jedis)

    /*val r = conn.keys("*")
    import scala.collection.JavaConversions._
    for (p <- r) {
      println(p + " : " + conn.get(p))
    }*/
  }
}
