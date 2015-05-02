package edu.ncsu.spiderz;

import redis.clients.jedis.*;

public class RedisQueue {

	Jedis jedis;
	String qName;
	
	public void queue(String value) {
		jedis.rpush(qName, value);
	}
	
	public String dequeue() {
		return jedis.lpop(qName);
		
	}
	
	public RedisQueue(String ipPort, String name) {
		jedis = new Jedis(ipPort);
		qName = name;
	}
	
	public RedisQueue(String name) {
		jedis = new Jedis("localhost");
		qName = name;
	}	
}