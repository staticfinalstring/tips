package com.wilmar.apm.provider.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Redis可重入分布式锁实现(只考虑业务超时与线程重入)
 * 非完美方案。 当前线程锁超时后，其他线程会趁虚而入
 * @Description: Redis可重入分布式锁实现
 */
public class RedisReentrantLock {

    private ThreadLocal<Map<String, Integer>> locker = new ThreadLocal<>();

    /**
     * LUA 脚本实现原子锁释放
     * 防止场景：第一个请求超时，锁到期自动释放，第二个请求进行中时，被第一个请求将锁释放
     * 需要：每个请求单独的一个唯一值，根据值只释放自己的锁
     */
    private static String LUA_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
    
    private static final Long RELEASE_SUCCESS = 1L;

    private JedisPool jedisPool;

    public RedisReentrantLock(JedisPool jedisPool){
        this.jedisPool = jedisPool;
    }

    private Jedis getJedis(){
        return jedisPool.getResource();
    }

    private void releaseJedis(Jedis jedis){
        if (null != jedis){
            jedis.close();
        }
    }

    /**
     * 获取锁
     * @param key 锁的key
     * @param value 指定的value
     * @param timeout 超时时间
     * @return 是否成功获取锁
     */
    private boolean _lock(String key, String value, Long timeout){
        Jedis jedis = getJedis();
        String set;
        try {
            set = getJedis().set(key, value, "nx", "ex", timeout);
        } finally {
            releaseJedis(jedis);
        }
        return set != null;
    }

    /**
     * 指定性释放锁
     * @param key 锁的key
     * @param value 指定的value
     * @return 是否成功释放锁
     */
    private boolean _unLock(String key, String value){
        Jedis jedis = getJedis();
        Object result;
        try {
            result = jedis.eval(LUA_SCRIPT, Collections.singletonList(key), Collections.singletonList(value));
        } finally {
            releaseJedis(jedis);
        }
        return RELEASE_SUCCESS.equals(result);
    }

    private Map<String, Integer> currentLocker(){
        Map<String, Integer> currentLocker = locker.get();
        if (null != currentLocker){
            return currentLocker;
        }
        locker.set(new HashMap<String, Integer>(3));
        return locker.get();
    }

    public boolean lock(String key, String value, Long timeout){
        Map<String, Integer> lock = currentLocker();
        Integer lockCount = lock.get(key);
        if (null != lockCount){
            lock.put(key, lockCount + 1);
            return true;
        }
        boolean ok = this._lock(key, value, timeout);
        if (ok){
            lock.put(key, 1);
            return true;
        }
        return false;
    }

    public boolean unLock(String key, String value){
        Map<String, Integer> lock = currentLocker();
        Integer lockCount = lock.get(key);
        if (null == lockCount){
            return false;
        }
        lockCount -= 1;
        if (lockCount > 0){
            lock.put(key, lockCount);
            return true;
        } else {
            lock.remove(key);
            return this._unLock(key, value);
        }
    }

}
