package com.rideplatform.core.service;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;


@Configuration
@ComponentScan(basePackageClasses = TripService.class)
public class RedisConfig {
    @Bean
    public StringRedisTemplate redisTemplate(RedisConnectionFactory connectionFactory){
        return new StringRedisTemplate(connectionFactory);
    }
    // @Bean
    // public RedisCacheManager cacheManager(RedisConnectionFactory connectionFactory){
    //     RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig().entryTtl(Duration.ofMinutes(10)).disableCachingNullValues();
    //     return RedisCacheManager.builder(connectionFactory).cacheDefaults(config).build();
    // }
}
