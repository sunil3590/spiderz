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

	// https://github.com/addthis/stream-lib/blob/master/src/
	// main/java/com/clearspring/analytics/hash/MurmurHash.java
	private static long simpleHash(String key, int seed) {
		int m = 0x5bd1e995;
		int r = 24;

		byte[] data = key.getBytes();
		int length = data.length;

		int h = seed ^ length;

		int len_4 = length >> 2;

		for (int i = 0; i < len_4; i++) {
			int i_4 = i << 2;
			int k = data[i_4 + 3];
			k = k << 8;
			k = k | (data[i_4 + 2] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 1] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 0] & 0xff);
			k *= m;
			k ^= k >>> r;
			k *= m;
			h *= m;
			h ^= k;
		}

		// avoid calculating modulo
		int len_m = len_4 << 2;
		int left = length - len_m;

		if (left != 0) {
			if (left >= 3) {
				h ^= (int) data[length - 3] << 16;
			}
			if (left >= 2) {
				h ^= (int) data[length - 2] << 8;
			}
			if (left >= 1) {
				h ^= (int) data[length - 1];
			}

			h *= m;
		}

		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;

		return getUnsignedInt(h);
	}

	// http://stackoverflow.com/questions/9578639/
	// best-way-to-convert-a-signed-integer-to-an-unsigned-long
	private static final int BITS_PER_BYTE = 8;

	private static long getUnsignedInt(int x) {
		ByteBuffer buf = ByteBuffer.allocate(Long.SIZE / BITS_PER_BYTE);
		buf.putInt(Integer.SIZE / BITS_PER_BYTE, x);
		return buf.getLong(0);
	}

	private String getRedisCountMinKey(String key, int row) {
		long idx = simpleHash(key, seed[row]) % width;

		// compute the redis key
		String cmKey = keyPrefix + Integer.toString(row) + "_"
				+ Long.toString(idx);

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

	public RedisCountMin(String ipPort, int width, int height) throws Exception {
		if (height > 8 || height < 1 || width < 1)
			throw new Exception("Invalid arguments");

		jedis = new Jedis(ipPort);

		this.width = width;
		this.height = height;
	}

	public RedisCountMin() {
		jedis = new Jedis("localhost");

		this.width = 2048;
		this.height = 8;
	}
}