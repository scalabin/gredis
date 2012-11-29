package redis.client.gredis


import java.util.TreeMap;

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import redis.clients.jedis.BalancePool
import redis.clients.jedis.ConsistentJedisPool
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.JedisTemplate
import redis.clients.jedis.RedisCallback
import redis.clients.jedis.RedisClient
import redis.clients.jedis.exceptions.JedisException
import redis.clients.util.Hashing

import dsl.config.util.DslConfig


@Singleton
class Gredis {
	def static Logger log = LoggerFactory.getLogger(Gredis.class)
	def maxActive = DslConfig.instance.factory.redis.maxActive
	def MaxIdle = DslConfig.instance.factory.redis.MaxIdle
	def MaxWait =DslConfig.instance.factory.redis.MaxWait
	def expireTime = DslConfig.instance.factory.redis.expireTime
	def address =DslConfig.instance.factory.redis.addresses
	int numberOfReplicas = 500;

	RedisClient redisClient
	JedisTemplate jedisTemplate
	BalancePool balancePool
	

	Gredis(){
		initJedisTemplate()
		initRedisClient()
	}

	def initRedisClient(){
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(maxActive);
		config.setMaxIdle(MaxIdle);
		config.setMaxWait(MaxWait);
		config.setTestOnBorrow(true);

		def addressArr = address.split(",")
		List<String> addressList = Arrays.asList(addressArr)
		ConsistentJedisPool consistentJedisPool = new ConsistentJedisPool(addressList, numberOfReplicas, config, Hashing.MD5)
		redisClient = new RedisClient(consistentJedisPool);
	}


	def void initJedisTemplate() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(maxActive);
		config.setMaxIdle(MaxIdle);
		config.setMaxWait(MaxWait);
		config.setTestOnBorrow(true);

		def addressArr = address.split(",")
		List<String> addressList = Arrays.asList(addressArr)
		balancePool = new BalancePool(addressList, numberOfReplicas, config, Hashing.MD5)
		jedisTemplate = new JedisTemplate(balancePool);
	}
	
	def redisPersitenceCall(RedisCallback<Object> callback) {
		balancePool.poolArr.each { pool ->
			Jedis jedis = pool.getResource();
			try {
				def ret = callback.doInRedis(jedis);
				return ret;
			} catch (Throwable e) {
				if (jedis != null) {
					pool.returnBrokenResource(jedis);
				}
	
				throw new JedisException("Failed to call redis", e);
			} finally {
				if (jedis != null) {
					pool.returnResource(jedis);
				}
			}
		}
	}

}

