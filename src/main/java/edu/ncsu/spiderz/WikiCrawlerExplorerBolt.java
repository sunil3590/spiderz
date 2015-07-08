package edu.ncsu.spiderz;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.storm.guava.collect.Lists;
import org.apache.storm.http.HttpEntity;
import org.apache.storm.http.HttpResponse;
import org.apache.storm.http.client.HttpClient;
import org.apache.storm.http.client.methods.HttpGet;
import org.apache.storm.http.impl.client.DefaultHttpClient;
import org.apache.storm.http.util.EntityUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonArray;

import redis.clients.jedis.Jedis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class WikiCrawlerExplorerBolt implements IRichBolt{

	OutputCollector _outputCollector;
	
	private String queryStr = "http://en.wikipedia.org/w/api.php?format=json&";
	private String invIdxPrefix = "RII_";
	private String linkCountPrefix = "RLC_";
	private String queueId = "unexploredQueue";
	
	// list to hold titles linked from one page
	private List<String> titles;
	
	// hash of stop words to be eliminated
	private HashMap<String, Boolean> stopWords = new HashMap<String, Boolean>();
	
	// connection to redis
	private Jedis jedis;
	
	// profiling
	private int numWordsIndexed = 0;
	private int numLinksSeen = 0;
	private int numLinksExplored = 0;
	
	// object ID
	private int id = 0;
	
	// redis ip and port
	String redisIp = null;
	String redisPort = null;
	
	// constructor to get redis IP and port
	public WikiCrawlerExplorerBolt(String redisIp, String redisPort) {
		
		System.out.println("\n>>>>>>> BOLT " + id + " : redis - " + redisIp + ":" + 
				redisPort + "\n");
		
		this.redisIp = redisIp;
		this.redisPort = redisPort;
	}
	
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
				String title = jsonElement.getAsJsonObject()
									.get("title").getAsString();
				int ns = Integer.parseInt(jsonElement.getAsJsonObject()
						.get("ns").getAsString());
				
				// add only topic links
				if(ns == 0)
					links.add(title);
			}
		}
		
		// figure out if continuation of the api is required
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
	
	// build the bloomfilter using the stopwords in the stop-words.txt file
	public void buildStopWordList() {
		
		// folder which contains all stop words - generated during build
		File folder = new File("stop-words");
		
		// if folder does not exist, notify and return
		if(folder.exists() == false) {
			System.out.println("\n>>>> BOLT " + id + " - Cound not find stop-words folder," +
					"\"mvn clean install\"");
			return;
		}
		
		// get all files in the folder
		File[] stopWordFiles = folder.listFiles();
		
		// read each file and build bloom filter full of stop words
		for(File file : stopWordFiles) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(file));
				String line;
				
				// read all words and add to bloom filter
				while ((line = br.readLine()) != null) {	
					stopWords.put(line.toLowerCase(), true);
				}
				
				br.close();
			} catch(Exception e) {	
				System.out.println("\n>>>> BOLT " + id + " - Error in stop word file read" + e);
			}
		}
		
		System.out.println("\n>>>> BOLT " + id + " - Stop words list built\n");
	}
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple tuple) {
		String tupleTitle = tuple.getStringByField("title");
		String token = null;
		
		System.out.println(">>>> BOLT " + id + " - Exploring " + tupleTitle);
		
		// explore the title
		explore(tupleTitle);
		
		// loop through all titles in title just explored
		for (String title : titles) {
			// copy of title
			String copyTitle = new String(title);
			
			// get link count for title from redis
			String linkCountStr = jedis.hget(linkCountPrefix + title, 
											 "count");
			// if title not explored
			if(linkCountStr == null) {
				// add title to redis queue of unexplored titles
				jedis.rpush(queueId, title);
				
				// replace special characters with space
				copyTitle.replaceAll("[$&+,:;=?@#|'<>.^*()%!-]", " ");
				
				// take the title to lower case
				copyTitle = copyTitle.toLowerCase();
				
				// tokenize the title and process each token
				StringTokenizer tokenizer = new StringTokenizer(copyTitle, " ");
				while(tokenizer.hasMoreTokens()) {
					token = tokenizer.nextToken();
					
					// add reverse index entry to redis if not stop word
					if(stopWords.containsKey(token) == false) {
						// add prefix to the token to easily identify in redis
						jedis.sadd(invIdxPrefix + token, title);
						
						numWordsIndexed++;
						if(numWordsIndexed % 10000 == 0)
							System.out.println(">>>> BOLT " + id + " - Indexed " + 
									numWordsIndexed + " keywords");
					}
				}
				
				numLinksSeen++;
				if(numLinksSeen % 5000 == 0)
					System.out.println(">>>> BOLT " + id + " - Seen " + 
							numLinksSeen + " titles");
			}
			
			// increment count for title to reflect new incoming link
			jedis.hincrBy(linkCountPrefix + title, "count", 1);
		}
		
		_outputCollector.ack(tuple);
		
		numLinksExplored++;
		if(numLinksExplored % 10 == 0)
			System.out.println(">>>> BOLT " + id + " - Explored " + 
					numLinksExplored + " titles");
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// generate random id for object
		id = (int) (Math.random() * 100000);
		
		// list that hold all the links in a topic that is being explored
		titles = Lists.newArrayListWithExpectedSize(2000);
		
		// connect to redis to access list of unexplored titles and
		// reverse index
		jedis = new Jedis(this.redisIp, Integer.parseInt(this.redisPort), 2000);
		
		// build a stop word list from text files
		buildStopWordList();
		
		_outputCollector = collector;
		
		System.out.println("\n>>>> BOLT " + id + " - Prepared\n");
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