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
	private String revIdxPrefix = "RII_";
	private String queueId = "unexploredQueue";
	
	// list to hold titles linked from one page
	private List<String> titles;
	
	// hash of stop words to be eliminated
	private HashMap<String, Boolean> stopWords = new HashMap<String, Boolean>();
	
	// count of number of incoming links
	private RedisCountMin linkCount;
	
	// connection to redis
	private Jedis jedis;
	
	// profiling
	private int numWordsIndexed = 0;
	private int numLinksSeen = 0;
	private int numLinksExplored = 0;
	
	// object ID
	private int id = 0;
	
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
		
		// folder which contains all stop words
		//URL resource = getClass().getResource("/resources/data/stop-words");
		URL resource = getClass().getClassLoader().getResource("stop-words");
		File folder = new File(resource.getFile());
		
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
			// if title not explored
			if(linkCount.countMinGet(title) == 0) {
				// add title to redis queue of unexplored titles
				jedis.rpush(queueId, title);
				
				// add title to inverted index
				// tokenize the title and remove stop words
				StringTokenizer tokenizer = new StringTokenizer(title, " ");
				while(tokenizer.hasMoreTokens()) {
					token = tokenizer.nextToken();
					token = token.toLowerCase();
					
					// eliminate braces around words
					if(token.charAt(0) == '(') {
						token = token.substring(1);
					} else if(token.charAt(token.length()-1) == ')') {
						token = token.substring(0, token.length()-1);
					}
					
					// add reverse index entry to redis if not stop word
					if(stopWords.containsKey(token)== false) {
						// add prefix to the token to easily identify in redis
						jedis.sadd(revIdxPrefix + token, title);
						
						numWordsIndexed++;
						if(numWordsIndexed % 2000 == 0)
							System.out.println(">>>> BOLT " + id + " - Indexed " + 
									numWordsIndexed + " keywords");
					}
				}
				
				numLinksSeen++;
				if(numLinksSeen % 1000 == 0)
					System.out.println(">>>> BOLT " + id + " - Seen " + 
							numLinksSeen + " titles");
			}
			
			// increment count for title to reflect new incoming link
			linkCount.countMinInc(title);
		}
		
		_outputCollector.ack(tuple);
		
		numLinksExplored++;
		if(numLinksExplored % 5 == 0)
			System.out.println(">>>> BOLT " + id + " - Explored " + 
					numLinksExplored + " titles");
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// generate random id for object
		id = (int) (Math.random() * 100000);
		
		// list that hold all the links in a topic that is being explored
		titles = Lists.newArrayListWithExpectedSize(2000);
		
		// link counter
		linkCount = new RedisCountMin();
		
		// connect to redis to access list of unexplored titles and
		// reverse index
		jedis = new Jedis("localhost");
		
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