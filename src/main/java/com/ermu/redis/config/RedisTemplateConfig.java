package com.ermu.redis.config;

import com.ermu.redis.util.PropertiesLoaderUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;

/**
 * @author：xusonglin
 * ===============================
 * Created with IDEA.
 * Date：2019/2/18
 * Time：9:30
 * ================================
 */
@Configuration
public class RedisTemplateConfig {
    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory factory) {
        RedisTemplate template = new RedisTemplate();
        template.setConnectionFactory(factory);
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        template.setKeySerializer(stringRedisSerializer);
        template.setValueSerializer(stringRedisSerializer);
        template.setHashKeySerializer(stringRedisSerializer);
        template.setHashValueSerializer(stringRedisSerializer);
        template.afterPropertiesSet();
        return template;
    }

    @Autowired
    private Environment env;
    private String host;
    private String sysName;

    @PostConstruct
    public void  init(){
        PropertiesLoaderUtils prop = new PropertiesLoaderUtils("application.properties");
        host = prop.getProperty("redis.host");
        if(StringUtils.isEmpty(host)){
            sysName = env.getProperty("redis.sysName");
        } else{
            sysName = prop.getProperty("redis.sysName");
        }
    }
    public String getSysName() {
        return sysName;
    }

    public void setSysName(String sysName) {
        this.sysName = sysName;
    }
}
