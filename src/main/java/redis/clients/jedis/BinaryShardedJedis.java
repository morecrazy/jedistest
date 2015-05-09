package redis.clients.jedis;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.Hashing;
import redis.clients.util.Sharded;

public class BinaryShardedJedis extends Sharded<Jedis, JedisShardInfo>
        implements BinaryJedisCommands {
    public BinaryShardedJedis(List<JedisShardInfo> shards) {
        super(shards);
    }

    public BinaryShardedJedis(List<JedisShardInfo> shards, Hashing algo) {
        super(shards, algo);
    }

    public BinaryShardedJedis(List<JedisShardInfo> shards, Pattern keyTagPattern) {
        super(shards, keyTagPattern);
    }

    public BinaryShardedJedis(List<JedisShardInfo> shards, Hashing algo,
            Pattern keyTagPattern) {
        super(shards, algo, keyTagPattern);
    }

    public void disconnect() throws IOException {
        for (Jedis jedis : getAllShards()) {
            jedis.disconnect();
        }
    }

    protected Jedis create(JedisShardInfo shard) {
        return new Jedis(shard);
    }

    public String set(byte[] key, byte[] value) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return setValue(j,key,value);
    }
    
    private String setValue(Jedis j,byte [] key,byte[] value)
    {
    	try
    	{
    		return j.set(key,value);
    	}
    	
    	catch(JedisConnectionException e)
    	{
    		System.out.println("error11:"+j.getClent().getHost()+":"+j.getClent().getPort()+"  "+e.getMessage());
    		e.printStackTrace();
    		if (j.getBakJedis()!=null)
    		{
    			
    			Client client = j.getClent();
    			Client clientBak = j.getBakJedis().getClent();
    			j.setClent(clientBak);
    			j.getBakJedis().setClent(client);
    			client.disconnect();
    			return  setValue(j,key,value);
    		}
    		else throw e;
    	}
    	catch(JedisDataException e)
    	{
    		System.out.println("error11:"+j.getClent().getHost()+":"+j.getClent().getPort()+"  "+e.getMessage());
    		e.printStackTrace();
    		if (j.getBakJedis()!=null)
    		{
    			
    			Client client = j.getClent();
    			Client clientBak = j.getBakJedis().getClent();
    			j.setClent(clientBak);
    			j.getBakJedis().setClent(client);
    			client.disconnect();
    			return  setValue(j,key,value);
    		}
    		else throw e;
    	}
    	
    }
    private byte[] getValue(Jedis j,byte [] key)
    {
    	try
    	{
    		return j.get(key);
    	}

    	catch(JedisConnectionException e)
    	{
    		System.out.println("error11:"+j.getClent().getHost()+":"+j.getClent().getPort()+"  "+e.getMessage());
    		e.printStackTrace();
    		if (j.getBakJedis()!=null)
    		{
    			
    			Client client = j.getClent();
    			Client clientBak = j.getBakJedis().getClent();
    			j.setClent(clientBak);
    			j.getBakJedis().setClent(client);
    			client.disconnect();
    			byte[] b = getValue(j, key);
    			return b;
    		}
    		else throw e;
    	}
    	catch(JedisDataException e)
    	{
    		System.out.println("error11:"+j.getClent().getHost()+":"+j.getClent().getPort()+"  "+e.getMessage());
    		e.printStackTrace();
    		if (j.getBakJedis()!=null)
    		{
    			
    			Client client = j.getClent();
    			Client clientBak = j.getBakJedis().getClent();
    			j.setClent(clientBak);
    			j.getBakJedis().setClent(client);
    			client.disconnect();
    			byte[] b = getValue(j, key);
    			return b;
    		}
    		else throw e;
    	}

    }

    public byte[] get(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        //return j.get(key);
        return getValue(j,key);
    }

    public Boolean exists(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.exists(key);
    }

    public String type(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.type(key);
    }

    public Long expire(byte[] key, int seconds) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.expire(key, seconds);
    }

    public Long expireAt(byte[] key, long unixTime) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.expireAt(key, unixTime);
    }

    public Long ttl(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.ttl(key);
    }

    public byte[] getSet(byte[] key, byte[] value) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.getSet(key, value);
    }

    public Long setnx(byte[] key, byte[] value) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.setnx(key, value);
    }

    public String setex(byte[] key, int seconds, byte[] value) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.setex(key, seconds, value);
    }

    public Long decrBy(byte[] key, long integer) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.decrBy(key, integer);
    }

    public Long decr(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.decr(key);
    }

    public Long incrBy(byte[] key, long integer) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.incrBy(key, integer);
    }

    public Long incr(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.incr(key);
    }

    public Long append(byte[] key, byte[] value) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.append(key, value);
    }

    public byte[] substr(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.substr(key, start, end);
    }

    public Long hset(byte[] key, byte[] field, byte[] value) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.hset(key, field, value);
    }

    public byte[] hget(byte[] key, byte[] field) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.hget(key, field);
    }

    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.hsetnx(key, field, value);
    }

    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.hmset(key, hash);
    }

    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.hmget(key, fields);
    }

    public Long hincrBy(byte[] key, byte[] field, long value) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.hincrBy(key, field, value);
    }

    public Boolean hexists(byte[] key, byte[] field) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.hexists(key, field);
    }

    public Long hdel(byte[] key, byte[] field) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.hdel(key, field);
    }

    public Long hlen(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.hlen(key);
    }

    public Set<byte[]> hkeys(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.hkeys(key);
    }

    public Collection<byte[]> hvals(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.hvals(key);
    }

    public Map<byte[], byte[]> hgetAll(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.hgetAll(key);
    }

    public Long rpush(byte[] key, byte[] string) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.rpush(key, string);
    }

    public Long lpush(byte[] key, byte[] string) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.lpush(key, string);
    }

    public Long llen(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.llen(key);
    }

    public List<byte[]> lrange(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.lrange(key, start, end);
    }

    public String ltrim(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.ltrim(key, start, end);
    }

    public byte[] lindex(byte[] key, int index) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.lindex(key, index);
    }

    public String lset(byte[] key, int index, byte[] value) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.lset(key, index, value);
    }

    public Long lrem(byte[] key, int count, byte[] value) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.lrem(key, count, value);
    }

    public byte[] lpop(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.lpop(key);
    }

    public byte[] rpop(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.rpop(key);
    }

    public Long sadd(byte[] key, byte[] member) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.sadd(key, member);
    }

    public Set<byte[]> smembers(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.smembers(key);
    }

    public Long srem(byte[] key, byte[] member) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.srem(key, member);
    }

    public byte[] spop(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.spop(key);
    }

    public Long scard(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.scard(key);
    }

    public Boolean sismember(byte[] key, byte[] member) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.sismember(key, member);
    }

    public byte[] srandmember(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.srandmember(key);
    }

    public Long zadd(byte[] key, double score, byte[] member) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zadd(key, score, member);
    }

    public Set<byte[]> zrange(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrange(key, start, end);
    }

    public Long zrem(byte[] key, byte[] member) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrem(key, member);
    }

    public Double zincrby(byte[] key, double score, byte[] member) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zincrby(key, score, member);
    }

    public Long zrank(byte[] key, byte[] member) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrank(key, member);
    }

    public Long zrevrank(byte[] key, byte[] member) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrevrank(key, member);
    }

    public Set<byte[]> zrevrange(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrevrange(key, start, end);
    }

    public Set<Tuple> zrangeWithScores(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrangeWithScores(key, start, end);
    }

    public Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrevrangeWithScores(key, start, end);
    }

    public Long zcard(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zcard(key);
    }

    public Double zscore(byte[] key, byte[] member) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zscore(key, member);
    }

    public List<byte[]> sort(byte[] key) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.sort(key);
    }

    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.sort(key, sortingParameters);
    }

    public Long zcount(byte[] key, double min, double max) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zcount(key, min, max);
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrangeByScore(key, min, max);
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max,
            int offset, int count) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrangeByScore(key, min, max, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min,
            double max, int offset, int count) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrevrangeByScore(key, max, min);
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min,
            int offset, int count) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrevrangeByScore(key, max, min, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrevrangeByScoreWithScores(key, max, min);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max,
            double min, int offset, int count) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Long zremrangeByRank(byte[] key, int start, int end) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zremrangeByRank(key, start, end);
    }

    public Long zremrangeByScore(byte[] key, double start, double end) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.zremrangeByScore(key, start, end);
    }

    public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot,
            byte[] value) {
        Jedis j = getShard(key);
        j.setHasConnected(true);
        return j.linsert(key, where, pivot, value);
    }

    public List<Object> pipelined(ShardedJedisPipeline shardedJedisPipeline) {
        shardedJedisPipeline.setShardedJedis(this);
        shardedJedisPipeline.execute();
        return shardedJedisPipeline.getResults();
    }
}