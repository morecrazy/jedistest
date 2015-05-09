package redis.clients.jedis;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.util.Hashing;
import redis.clients.util.Pool;

public class ShardedJedisPool extends Pool<ShardedJedis> {
    public ShardedJedisPool(final GenericObjectPool.Config poolConfig,
            List<JedisShardInfo> shards) {
        this(poolConfig, shards, Hashing.MURMUR_HASH);
    }

    public ShardedJedisPool(final GenericObjectPool.Config poolConfig,
            List<JedisShardInfo> shards, Hashing algo) {
        this(poolConfig, shards, algo, null);
    }

    public ShardedJedisPool(final GenericObjectPool.Config poolConfig,
            List<JedisShardInfo> shards, Pattern keyTagPattern) {
        this(poolConfig, shards, Hashing.MURMUR_HASH, keyTagPattern);
    }

    public ShardedJedisPool(final GenericObjectPool.Config poolConfig,
            List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern) {
        super(poolConfig, new ShardedJedisFactory(shards, algo, keyTagPattern));
    }

    /**
     * PoolableObjectFactory custom impl.
     */
    private static class ShardedJedisFactory extends BasePoolableObjectFactory {
        private List<JedisShardInfo> shards;
        private Hashing algo;
        private Pattern keyTagPattern;

        public ShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo,
                Pattern keyTagPattern) {
            this.shards = shards;
            this.algo = algo;
            this.keyTagPattern = keyTagPattern;
        }

        public Object makeObject() throws Exception {
            ShardedJedis jedis = new ShardedJedis(shards, algo, keyTagPattern);
            return jedis;
        }

        public void destroyObject(final Object obj) throws Exception {
            if ((obj != null) && (obj instanceof ShardedJedis)) {
                ShardedJedis shardedJedis = (ShardedJedis) obj;
                for (Jedis jedis : shardedJedis.getAllShards()) {
                	Jedis jedisTem = jedis;
                    
                   		try {
                   			if(jedis.isHasConnected())
                   			{
                   				jedis.quit();
                   			}
                        } catch (Exception e) {

                        }
               			while(jedisTem != null)
               			{
               				try {
               					jedisTem.disconnect();
                            } catch (Exception e) {

                            }
               				jedisTem = jedisTem.getBakJedis();
               			}

                }
            }
        }

        public boolean validateObject(final Object obj) {
        	
/*                ShardedJedis jedis = (ShardedJedis) obj;
                for (Jedis shard : jedis.getAllShards()) {
                	try {
                		shard.ping();
                    }
                	catch (Exception ex) 
                    {
                    	if(shard.getBakJedis()!=null)
                    	{
	            			Client client = shard.getClent();
	            			Client clientBak = shard.getBakJedis().getClent();
	            			shard.setClent(clientBak);
	            			shard.getBakJedis().setClent(client);
	            			try {
	            				shard.ping();
	            			}catch(Exception e)
	            			{
	            			}
                    	}
                    }
                }*/
                return true;

        }
    }
}