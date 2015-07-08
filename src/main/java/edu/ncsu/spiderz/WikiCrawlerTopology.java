package edu.ncsu.spiderz;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import storm.trident.TridentTopology;
import storm.trident.testing.Split;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class WikiCrawlerTopology {

	private static int numWorkers = 1; // how many machines do we have?
	private static int numCores = 2; // how many cores on each machine?
	public static StormTopology buildTopology(String redisIp, String redisPort) {
		// topology to build
		TopologyBuilder topology = new TopologyBuilder();

		// create a spout
		WikiCrawlerSpout wikiSpout = new WikiCrawlerSpout(redisIp, redisPort);

		// create a bolt
		WikiCrawlerExplorerBolt wikiBolt = new WikiCrawlerExplorerBolt(redisIp, redisPort);

		// set up the DAG
		// this spout always takes 1 task, it is light
		topology.setSpout("wikiSpout", wikiSpout, 1)
				.setNumTasks(2)
				.setMaxSpoutPending(5);
		// this bolt uses as many executors(threads) as the cores available
		topology.setBolt("wikiBolt", wikiBolt, numCores)
				.setNumTasks(numCores * 4) // 4 task per thread
				.shuffleGrouping("wikiSpout");

		return topology.createTopology();
	}

	public static void main(String args[]) throws Exception {
		
		// check validity of command line arguments
		if(args.length != 2) {
			System.out.println("Command line arguments missing\n");
			System.out.println("Pass redis IP and port\n");
			return;
		}
		
		// configure the topology
		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(numWorkers);

		LocalCluster cluster = new LocalCluster();
		StormTopology topology = buildTopology(args[0], args[1]);
		cluster.submitTopology("crawler", conf, topology);

		System.out.println("\n>>>> TOPOLOGY - STATUS OK\n");
	}
}