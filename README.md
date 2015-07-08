# Spiderz
#### * Distributed crawling of Wikipedia using Apache Storm
#### * Store inverted index of keywords and link counts in Redis
#### * Handle search queries from web clients using Node.js

More details to come soon!

### To run
1. Update "conf/redis.conf" with the IP and port where redis should bind to
2. Start redis data store
sudo redis-server conf/redis.conf
3. Start storm crawler topology
storm jar target/spiderz-1.0-SNAPSHOT-jar-with-dependencies.jar edu.ncsu.spiderz.WikiCrawlerTopology ~redis IP~ ~redis port~
4. Start node search engine
nodejs wikiSearch/app.js ~redis IP~ ~redis port~

### TODO
1. Use first paragraph to index and search better
2. Make request for multiple topics at a time
3. Test in distributed mode