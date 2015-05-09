package com.mine.jedistest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mine.util.Utils;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class RedisShardPoolTest {
	static ShardedJedisPool pool;//切片连接池
    static{
        JedisPoolConfig config =new JedisPoolConfig();//Jedis池配置
        config.setMaxActive(300);//最大活动的对象个数
        config.setMaxIdle(1000 * 60);//对象最大空闲时间
        config.setMaxWait(1000 * 10);//获取对象时最大等待时间
        config.setTestOnBorrow(true);//；如果为true，则得到的jedis实例均是可用的；
        String hostA = "10.13.88.161";//服务器地址
        int portA = 6379;//redis端口号
        String hostB = "10.13.88.161";
        int portB = 6370;
        String hostC = "10.13.88.161";
        int portC = 6381;
        String hostD = "10.13.88.161";
        int portD = 6382;
        
        String hostE = "10.13.88.163";
        int portE = 6370;
        
        List<JedisShardInfo> jdsInfoList =new ArrayList<JedisShardInfo>(2);
        JedisShardInfo infoA = new JedisShardInfo(hostA, portA);
        JedisShardInfo infoB = new JedisShardInfo(hostB, portB);
        JedisShardInfo infoC = new JedisShardInfo(hostC, portC);
        JedisShardInfo infoD = new JedisShardInfo(hostD, portD);
        JedisShardInfo infoE = new JedisShardInfo(hostE, portE);
        
        /**
        jdsInfoList.add(infoA);
        jdsInfoList.add(infoB);
        jdsInfoList.add(infoC);
        jdsInfoList.add(infoD);
        */
        jdsInfoList.add(infoE);
        
        pool =new ShardedJedisPool(config, jdsInfoList);
     }
    public static void main(String[] args) {
        long s1=System.currentTimeMillis();   
        String key = "LINEITEM_BILL_MSG";
        for(int i=0; i<500; i++){
              ShardedJedis jds = null;//切片客户端连接
              jds = pool.getResource();
            try {               
                ImpressionVO impressionVO = new ImpressionVO();
                impressionVO.setId(i+"");
                impressionVO.setCost(1L);
                jds.rpush(key.getBytes(), Utils.convertObject2Byte(impressionVO));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                pool.returnResource(jds);
            }
        }
        long s2=System.currentTimeMillis();
        System.out.println(testTime(s2-s1));        
    }   
    public static String testTime(long ss){
        String aa=null;
        long zongmiaoshu = ss / 1000;
        long dangqianmiaoshu = zongmiaoshu % 60;
        long zongfenshu = zongmiaoshu /60;
        long dangqianfenshu = zongfenshu % 60;
        long zongshishu = zongfenshu / 60;
        long dangqianshishu = zongshishu % 24;    
        aa="当前时间：" + dangqianshishu + ":" + dangqianfenshu + ":" + dangqianmiaoshu;    
        return aa;            
    }
}
