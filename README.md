# Spiderz
### Distributed crawling of Wikipedia using Apache Storm and Redis
#### Currently finding the top K pages with most incoming links 
(shhh, I know this is trivial)

More details to come soon!

### TODO
1. Use only topic links from the JSON response
2. Make request for mutiple topics at a time
3. Look at keeping the connection open (not sure if this "can be/is" done)
4. Implement count min sketch on Redis instead of a huge hash table
5. Test in distributed mode