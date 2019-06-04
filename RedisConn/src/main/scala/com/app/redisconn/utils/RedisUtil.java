/**
 * @ClassName RedisUtil
 * @Description TODO
 * @Author zy
 * @Date 2019/6/1 18:00
 * @Version 1.0
 **/
package com.app.redisconn.utils;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    private static JedisPool pool = null;

    static {
        if (pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxIdle(25);
            config.setMaxWaitMillis(1000 * 100);
            config.setTestOnBorrow(true);
            pool = new JedisPool(config, "Redis IP", 6379);
        }
    }

    public static Jedis getConnection(){
        return pool.getResource();
    }

    public static void closeConnection(Jedis jedis, Boolean exceptionExist){
        if (jedis != null) {
            if(exceptionExist) {
                pool.returnBrokenResource(jedis);
            }else {
                pool.returnResource(jedis);
            }
        }
    }
}

