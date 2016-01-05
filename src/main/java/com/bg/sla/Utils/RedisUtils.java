package com.bg.sla.Utils;


import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtils {
	JedisPool jedisPool = null;
	public RedisUtils(){
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxIdle(10);
		poolConfig.setMaxTotal(100);
		poolConfig.setMaxWaitMillis(10000);
		poolConfig.setTestOnBorrow(true);
		
		jedisPool = new JedisPool(poolConfig, Constant.REDIS_URL, Constant.REDIS_PORT);
	}
	
	public List<String> lrange(String key,int start,int end){
		Jedis resource = jedisPool.getResource();
		
		List<String> list = resource.lrange(key, start, end);
		jedisPool.returnResource(resource);
		return list;
		
	}
	
	public void add(String lowKey, String url) {
		Jedis resource = jedisPool.getResource();
		resource.lpush(lowKey, url);
		jedisPool.returnResource(resource);
	}
	public String poll(String key) {
		Jedis resource = jedisPool.getResource();
		String result = resource.lpop(key);
		jedisPool.returnResource(resource);
		return result;
	}
	public  String getVal(String key) {
		Jedis resource = jedisPool.getResource();
		String result = resource.get(key);
		jedisPool.returnResource(resource);
		return result;
	}
	public  void putVal(String key,String value) {
		Jedis resource = jedisPool.getResource();
		resource.set(key, value);
		jedisPool.returnResource(resource);
	}
	
	
	
	

}
