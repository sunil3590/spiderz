package edu.ncsu.spiderz;

import java.nio.ByteBuffer;

import redis.clients.jedis.*;

public class RedisCountMin {

	Jedis jedis;
	// dimensions of count min sketch
	int width;
	int height;
	// prefix to be used with key in redis
	String keyPrefix = "RCM_";
	// seeds to hash - support up to 8 hashes and hence height of 8
	int[] seed = { 4962, 274836, 7527385, 321459, 9864761, 649, 176924826,
			57549862 };

	// ported Javascript from - https://github.com/perezd/node-murmurhash/
	// blob/master/murmurhash.js
	private static int murmur2mod(String str, int seed) {
		char[] data = str.toCharArray();
		int l = str.length();
		long h = seed ^ l;
		int i = 0;
		long k = 0;
		
		int mul = 1;
		
		while (l >= 4) {
			k = ((data[i] & 0xff)) | 
					((data[++i] & 0xff) << 8) | 
					((data[++i] & 0xff) << 16) | 
					((data[++i] & 0xff) << 24);

			k = (((k & 0xffff) * mul) + ((((k >>> 16) * mul) & 0xffff) << 16));
			k ^= k >>> 24;
			k = (((k & 0xffff) * mul) + ((((k >>> 16) * mul) & 0xffff) << 16));

			h = (((h & 0xffff) * mul) + ((((h >>> 16) * mul) & 0xffff) << 16))
					^ k;

			l -= 4;
			++i;
		}

		switch (l) {
		case 3:
			h ^= (data[i + 2] & 0xff) << 16;
		case 2:
			h ^= (data[i + 1] & 0xff) << 8;
		case 1:
			h ^= (data[i] & 0xff);
			h = (((h & 0xffff) * mul) + ((((h >>> 16) * mul) & 0xffff) << 16));
		}
		
		h ^= h >>> 13;
		h = (((h & 0xffff) * mul) + ((((h >>> 16) * mul) & 0xffff) << 16));
		h ^= h >>> 15;
		
		h = h >>> 0;
		
		// leave out the signed bit
		int h31 = (int)h & 0x7fffffff;
		
		return h31;
}

	private String getRedisCountMinKey(String key, int row) {
		int idx = murmur2mod(key, seed[row]) % width;

		// compute the redis key
		String cmKey = keyPrefix + Integer.toString(row) + "_"
				+ Integer.toString(idx);
		
		return cmKey;
	}

	public int countMinGet(String key) {
		int count = 0;
		int min = Integer.MAX_VALUE;

		for (int i = 0; i < height; i++) {
			// get redis key
			String cmKey = getRedisCountMinKey(key, i);

			// get value in redis for key
			String value = jedis.hget(cmKey, "count");

			// even if one entry is 0, we can stop looking further
			if (value == null)
				return 0;

			// hold the min value observed
			count = Integer.parseInt(value);
			if (count < min)
				min = count;
		}

		return min;
	}

	public void countMinInc(String key) {
		for (int i = 0; i < height; i++) {
			// get redis key
			String cmKey = getRedisCountMinKey(key, i);

			// increment for the key in redis
			jedis.hincrBy(cmKey, "count", 1);
		}
	}

	public RedisCountMin(String ipPort) throws Exception {
		jedis = new Jedis(ipPort);

		this.width = 1024 * 1024 * 32;
		this.height = 8;
	}

	public RedisCountMin() {
		jedis = new Jedis("localhost");

		this.width = 1024 * 1024 * 32;
		this.height = 8;
	}
}