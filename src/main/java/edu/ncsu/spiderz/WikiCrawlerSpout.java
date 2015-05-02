package edu.ncsu.spiderz;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.storm.http.HttpEntity;
import org.apache.storm.http.HttpResponse;
import org.apache.storm.http.client.HttpClient;
import org.apache.storm.http.client.methods.HttpGet;
import org.apache.storm.http.impl.client.DefaultHttpClient;
import org.apache.storm.http.util.EntityUtils;

import org.apache.storm.guava.collect.Lists;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonArray;

import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;

import edu.ncsu.spiderz.RedisQueue;
import edu.ncsu.spiderz.RedisCountMin;

@SuppressWarnings("serial")
public class WikiCrawlerSpout implements IRichSpout {
	
	SpoutOutputCollector _collector;
	
	private String queryStr;
	
	// list to hold titles linked from one page
	private List<String> titles;
	
	// Q of titles not yet explored
	RedisQueue unexploredQ;
	
	// count of number of incoming links
	RedisCountMin linkCount;

	
	// get JSON from a string
	private JsonElement getJson(String response) {
		JsonElement root = null;
		
		try {
			root = new JsonParser().parse(response);
		} catch (Exception e) {
			System.out.println("Error while parsing the json response" + e);
		}
		
		return root;
	}

	// make a http request to the wiki api server
	private String getResponse(final String url) throws Exception {
		HttpGet method = new HttpGet(url);
		
		try {
			HttpClient http = new DefaultHttpClient();
			HttpResponse httpResponse = http.execute(method);
			HttpEntity entity = httpResponse.getEntity();
			if (entity != null) {
				String json = EntityUtils.toString(entity);
				return json;
			}
		} catch (IOException e) {
			throw e;
		} finally {
			//http.getConnectionManager().shutdown();
		}

		throw new Exception("Could not fetch url : " + url);
	}

	// helps find out if there are more links to be fetched
	private String fetchPlContinue(String response) {
		JsonElement elem = getJson(response).getAsJsonObject().get("query-continue");
		
		if (elem != null) {
			return elem.getAsJsonObject().get("links").getAsJsonObject()
					.get("plcontinue").getAsString();
		} else {
			return null;
		}
	}
	
	// add links from the response of url to list
	private String addLinks(String url, List<String> links) {
		// call wiki api and get response
		String response = null;
		try {
			response = getResponse(url);			
		} catch (Exception e) {
			System.out.println("Error in getting response from wiki : " + e);
			return null;
		}

		
		// extract links from response and add them to list 
		JsonElement root = getJson(response);
		JsonElement ele = root.getAsJsonObject().get("query").
							getAsJsonObject().get("pages");
		
		Set<Entry<String, JsonElement>> entrySet = ele.getAsJsonObject().entrySet();
		
		for (Entry<String, JsonElement> entry : entrySet) {
			JsonArray ll = ele.getAsJsonObject().get(entry.getKey()).
					getAsJsonObject().get("links").getAsJsonArray();
			
			for (JsonElement jsonElement : ll) {
				links.add(jsonElement.getAsJsonObject().get("title").getAsString());
				
				//TODO
				//System.out.println(jsonElement.getAsJsonObject().get("title").getAsString());
			}
		}
		
		// figure out if continutation of the api is required
		String plcontinue = fetchPlContinue(response);
		
		return plcontinue;
	}
	
	// explore the links in a wiki title
	private void explore(String title) {		
        try {
    		// build url to explore
    		StringBuilder url = new StringBuilder(queryStr);
            url.append("action=query&prop=links&pllimit=max&titles=");
            url.append(URLEncoder.encode(title, "UTF-8"));
            
        	// add first "pllimit" topics from page
			String plcontinue = addLinks(url.toString(), titles);
			
			// loop till we add all topics from page
			while(plcontinue != null) {
				plcontinue = addLinks(url.toString() + "&plcontinue=" + 
								URLEncoder.encode(plcontinue, "UTF-8"), titles);
			}
        } catch (Exception e) {
        	System.out.println("Error while exploring : " + title);
		}
	}
	
	// get next topic to be emitted by spout
	private String getNextTopic() {
		// if all titles in a particular page have been exhausted
		// fetch links from a new page
		while(titles.size() == 0) {
			// talk to redis, deque an unexplored title
			String nextTopic = unexploredQ.dequeue();
			
			// debug
			//System.out.println("Dequeued : " + nextTopic);
			
			// wiki api to get titles
			explore(nextTopic);
			
			// debug
			//System.out.println("Explored : " + titles.size());
			
			// loop through all titles in title just explored
			for (String title : titles) {
				// if title not explored
				if(linkCount.countMinGet(title) == 0) {
					// add title to redis queue
					unexploredQ.queue(title);
					
					// debug
					//System.out.println("Queued : " + title);
				}
				
				// increment count for title to reflect new incoming link
				linkCount.countMinInc(title);
			}
		}
		
		// send out the first unprocessed topic 
		return titles.remove(0);
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
		titles.clear();
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
		String topic = getNextTopic();
		_collector.emit(new Values(topic));
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		_collector = arg2;
		
		queryStr = "http://en.wikipedia.org/w/api.php?format=json&";
		titles = Lists.newArrayListWithExpectedSize(2000);
		
		// set the starting point to crawl TODO - do not hardcode
		unexploredQ = new RedisQueue("wikiCrawlerTopicQueue"); 
		unexploredQ.queue("Google");
		
		// link counter
		linkCount = new RedisCountMin();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}