package edu.ncsu.spiderz;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.List;
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

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class WikiCrawlerTopology {

	public static StormTopology buildTopology(LocalDRPC drpc) {
		// topology to build
		TopologyBuilder topology = new TopologyBuilder();
		
		// create a spout
		WikiCrawlerSpout wikiCrawler = new WikiCrawlerSpout();
		
		// set up the DAG
		topology.setSpout("titles", wikiCrawler);
		
		return topology.createTopology();
	}
	
	public static void main(String args[]) throws Exception{
		// configure the topolgy
		Config conf = new Config();
    	conf.setDebug(false);
    	conf.setMaxSpoutPending(10);

    	// configure a local cluster and submit
    	LocalCluster cluster = new LocalCluster();
    	LocalDRPC drpc = new LocalDRPC();
    	cluster.submitTopology("crawler", conf, buildTopology(drpc));
    	
    	// wait to crawl enough topics
    	Thread.sleep(10000);

    	// make drpc queries in a loop
    	for (int i = 0; i < 20; i++) {        		
        		// get the list of top k words with their counts
        		System.out.println("TOP K TOPICS : "+ drpc.execute("get_top_k", "whatever"));
        		
        		// wait for some time before querying again
        		Thread.sleep(5000);
    	}
   	
		System.out.println("STATUS: OK");
		
		// shutdown and terminate
		cluster.shutdown();
        drpc.shutdown();
	}
}