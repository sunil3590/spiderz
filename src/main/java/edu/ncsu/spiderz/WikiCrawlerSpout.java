package edu.ncsu.spiderz;

import java.util.Map;

import redis.clients.jedis.Jedis;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class WikiCrawlerSpout implements IRichSpout {

	private SpoutOutputCollector _collector;
	
	private int noInputCounter = 0;
	
	private String queueId = "unexploredQueue";
	
	// connection to redis
	Jedis jedis;

	// redis ip and port
	String redisIp = null;
	String redisPort = null;
	
	// constructor to get redis IP and port
	public WikiCrawlerSpout(String redisIp, String redisPort) {
		
		System.out.println("\n>>>>>>> SPOUT : redis - " + redisIp + ":" + 
				redisPort + "\n");
		
		this.redisIp = redisIp;
		this.redisPort = redisPort;
	}
	
	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		// add title to redis queue of unexplored titles
		String topic = jedis.lpop(queueId);

		if (topic == null) {
			try {
				Thread.sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			// notify if queue has been empty for some time
			noInputCounter++;
			if (noInputCounter > 2000) {
				System.out.println("\n>>>> SPOUT - No title in redis "
						+ "queue from quite some time! May be you haven't " +
						"added a starting title to crawl from\n");
				noInputCounter = 0;
			}

			return;
		}

		noInputCounter = 0;

		_collector.emit(new Values(topic));
	}

	@Override
	public void open(Map arg0, TopologyContext topCtx,
			SpoutOutputCollector collector) {
		_collector = collector;

		// connect to redis to access queue which 
		// contains unexplored titles
		jedis = new Jedis(this.redisIp, Integer.parseInt(this.redisPort));
		
		// set the starting point to crawl if already not set
		if(jedis.llen(queueId) == 0) {
			// give a variety of links to start crawling from
			jedis.rpush(queueId, "Computer science");
			jedis.rpush(queueId, "Botany");
			jedis.rpush(queueId, "Physics");
			jedis.rpush(queueId, "Mathematics");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("title"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}