package com.ermu.redis.service;

import com.alibaba.fastjson.JSON;
import com.ermu.redis.config.RedisTemplateConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.*;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * @author xusonglin
 */
@Service
public class RedisService {
    @Autowired
    private RedisTemplate redisTemplate;

    @Autowired
    private RedisTemplateConfig redisTemplateConfig;

    private static double size = Math.pow(2, 32);

    /**
     * 加入系统前缀
     *
     * @param key
     * @return
     * @date 2017年5月4日
     */
    private String addSys(String key) {
        String result = key;
        String sys = redisTemplateConfig.getSysName();
        if (key.startsWith(sys)){
            result = key;
        }else{
            result = sys + ":" + key;
        }
        return result;
    }

    /**
     * 将非字符串对象
     * 转化为json字符串
     * @param value
     * @return
     */
    private String toJsonStr(Object value){
        String realValue = "";
        if (value instanceof String) {
            realValue = value.toString();
        } else {
            realValue = JSON.toJSONString(value, false);
        }
        return realValue;
    }

    /**
     * 写入缓存
     * @param key
     * @param offset   位 8Bit=1Byte
     * @return
     */
    public boolean setBit(String key, long offset, boolean isShow) {
        boolean result = false;
        try {
            ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
            operations.setBit(addSys(key), offset, isShow);
            result = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 写入缓存
     * @param key
     * @param offset
     * @return
     */
    public boolean getBit(String key, long offset) {
        boolean result = false;
        try {
            ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
            result = operations.getBit(addSys(key), offset);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /********************************************** 通用操作 ************************************/
    /**
     * 批量删除对应的value
     *
     * @param keys
     */
    public void remove(final String... keys) {
        for (String key : keys) {
            remove(addSys(key));
        }
    }

    /**
     * 删除对应的value
     *
     * @param key
     */
    public void remove(final String key) {
        if (exists(addSys(key))) {
            redisTemplate.delete(addSys(key));
        }
    }

    /**
     * 判断缓存中是否有对应的value
     * @param key
     * @return
     */
    public boolean exists(final String key) {
        return redisTemplate.hasKey(addSys(key));
    }
    /********************************************** 通用操作 ************************************/

    /********************************************** String 操作 ************************************/

    /**
     * 写入缓存
     * @param key
     * @param value
     * @return
     */
    public boolean strSet(final String key, Object value) {
        boolean result = false;
        try {
            ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
            operations.set(addSys(key), toJsonStr(value));
            result = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
    /**
     * 写入缓存 redis 中 不存在当前key才能保存
     * @param key
     * @param value
     * @return
     */
    public boolean strSetIfAbsent(final String key, Object value) {
        boolean result = false;
        try {
            ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
            result = operations.setIfAbsent(addSys(key), toJsonStr(value));
            result = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 写入缓存设置时效时间
     * @param key
     * @param value
     * @return
     */
    public boolean strSetAndExpire(final String key, Object value, Long expireTime) {
        boolean result = false;
        try {
            ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
            operations.set(addSys(key), toJsonStr(value));
            redisTemplate.expire(addSys(key), expireTime, TimeUnit.SECONDS);
            result = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
    /**
     * 读取缓存
     *
     * @param key
     * @return
     */
    public Object strGet(final String key) {
        Object result = null;
        ValueOperations<Serializable, Object> operations = redisTemplate.opsForValue();
        result = operations.get(addSys(key));
        return result;
    }
    /********************************************** String 操作 ************************************/


    /********************************************** Hash 操作 ************************************/

    /**
     * 哈希 添加
     * @param key
     * @param hashKey
     * @param value
     */
    public void hmSet(String key, Object hashKey, Object value) {
        HashOperations<String, Object, Object> hash = redisTemplate.opsForHash();
        hash.put(addSys(key), toJsonStr(hashKey), toJsonStr(value));
    }

    /**
     * 哈希获取数据
     * @param key
     * @param hashKey
     * @return
     */
    public Object hmGet(String key, Object hashKey) {
        HashOperations<String, Object, Object> hash = redisTemplate.opsForHash();
        return hash.get(addSys(key),  toJsonStr(hashKey));
    }

    /**
     * 查询多条
     * @param key
     * @param hashKeys
     * @return
     */
    public List<Object> hmMultiGet(String key ,List<Object> hashKeys){
        HashOperations<String, Object, Object> hash = redisTemplate.opsForHash();
        for (Object hashKey : hashKeys) {
            hashKey = toJsonStr(hashKey);
        }
        return hash.multiGet(key,hashKeys);
    }

    /**
     * 哈希删除数据
     * @param key
     * @param hashKey
     * @return
     */
    public void hmDel(String key, Object hashKey) {
        HashOperations<String, Object, Object> hash = redisTemplate.opsForHash();
        hash.delete(addSys(key),  toJsonStr(hashKey));
    }

    /**
     * 所有值集合
     * @param key
     * @return
     */
    public List<Object> hmValues(String key){
        HashOperations<String, Object, Object> hash = redisTemplate.opsForHash();
        return hash.values(addSys(key));
    }

    /********************************************** Hash 操作 ************************************/

    /********************************************** List 操作 ************************************/
    /**
     * 列表添加
     * @param key
     * @param value
     */
    public void setPush(String key, Object value) {
        ListOperations<String, Object> list = redisTemplate.opsForList();
        list.rightPush(addSys(key), toJsonStr(value));
    }

    /**
     * 列表获取
     * @param key
     * @param l
     * @param l1
     * @return
     */
    public List<Object> lRange(String key, long l, long l1) {
        ListOperations<String, Object> list = redisTemplate.opsForList();
        return list.range(addSys(key), l, l1);
    }
    /********************************************** List 操作 ************************************/

    /********************************************** Set 操作 ************************************/
    /**
     * 集合添加
     * @param key
     * @param value
     */
    public void add(String key, Object value) {
        SetOperations<String, Object> set = redisTemplate.opsForSet();
        set.add(addSys(key), toJsonStr(value));
    }

    /**
     * 集合元素
     * @param key
     * @return
     */
    public Set<Object> setGetMembers(String key) {
        SetOperations<String, Object> set = redisTemplate.opsForSet();
        return set.members(addSys(key));
    }
    /********************************************** Set 操作 ************************************/

    /*************************************** 有序集合 ********************************************/
    /**
     * 有序集合添加
     *
     * @param key
     * @param value
     * @param score
     */
    public void zAdd(String key, Object value, double score) {
        ZSetOperations<String, Object> zset = redisTemplate.opsForZSet();
        zset.add(addSys(key), toJsonStr(value), score);
    }

    /**
     * 有序集合添加
     *
     * @param key
     */
    public void zAdd(String key,Set<ZSetOperations.TypedTuple<Object>> set) {
        ZSetOperations<String, Object> zset = redisTemplate.opsForZSet();
        zset.add(addSys(key),set);
    }

    /**
     * 获取排名
     * @param key 集合名称
     * @param value 值
     */
    public Long zGetRank(String key, Object value) {
        ZSetOperations<String, Object> zset = redisTemplate.opsForZSet();
        return  zset.reverseRank(addSys(key),toJsonStr(value)) + 1L ;
    }

    /**
     * 获取分数
     * @param key
     * @param value
     */
    public Double zGetScore(String key, Object value) {
        ZSetOperations<String, Object> zset = redisTemplate.opsForZSet();
        return zset.score(addSys(key),toJsonStr(value));
    }

    /**
     * 增加分数
     * @param key
     * @param value
     */
    public void incrementScore(String key, Object value, double score) {
        ZSetOperations<String, Object> zset = redisTemplate.opsForZSet();
        zset.incrementScore(addSys(key), toJsonStr(value), score);
    }

    /**
     * 获取分数区间个数
     * @param key
     */
    public Long countBetweenScore(String key, double score1,double score2) {
        ZSetOperations<String, Object> zset = redisTemplate.opsForZSet();
        Long ret = zset.count(addSys(key),score1,score2);
        return ret;
    }

    /**
     * 获取分数区间
     * @param key
     */
    public Set<ZSetOperations.TypedTuple<Object>> getBetweenScore(String key, double score1,double score2) {
        ZSetOperations<String, Object> zset = redisTemplate.opsForZSet();
        Set<ZSetOperations.TypedTuple<Object>> typedTuples = zset.reverseRangeByScoreWithScores(addSys(key), score1, score2);
        return typedTuples;
    }

    /**
     * 获取名次区间
     * @param key
     */
    public Set<ZSetOperations.TypedTuple<Object>> reverseZRankWithRank(String key, long start, long end) {
        ZSetOperations<String, Object> zset = redisTemplate.opsForZSet();
        Set<ZSetOperations.TypedTuple<Object>> ret = zset.reverseRangeWithScores(addSys(key), start, end);
        return ret;
    }

}