package edu.ncsu.spiderz;

import redis.clients.jedis.*;

public class RedisCountMin {
	
	Jedis jedis;
	int width;
	int height;
	int numHash;
	
	public int countMinGet(String key) {
		// TODO - implement count min
		String value = jedis.hget(key, "count");
		if(value == null)
			return 0;
		
		return Integer.parseInt(value);
	}
	
	public void countMinInc(String key) {
		// TODO - implement count min
		jedis.hincrBy(key, "count", 1);
	}
	
	public RedisCountMin(String ipPort, int width, int height, int numHash) {
		jedis = new Jedis(ipPort);
		
		this.width = width;
		this.height = height;
		this.numHash = numHash;
	}
	
	public RedisCountMin() {
		jedis = new Jedis("localhost");
		
		this.width = 1024;
		this.height = 8;
		this.numHash = 3;
	}	
}