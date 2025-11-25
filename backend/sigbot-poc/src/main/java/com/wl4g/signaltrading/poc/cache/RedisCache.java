package com.wl4g.signaltrading.poc.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Service
@RequiredArgsConstructor
public class RedisCache implements ICache {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public <T> T get(String key, Class<T> type) {
        requireKey(key);
        requireNonNull(type, "type");
        String raw = redisTemplate.opsForValue().get(key);
        if (raw == null) {
            return null;
        }
        return deserialize(raw, type);
    }

    @Override
    public void set(String key, Object value, Duration expire) {
        requireKey(key);
        String payload = serialize(value);
        if (expire != null) {
            redisTemplate.opsForValue().set(key, payload, expire);
        } else {
            redisTemplate.opsForValue().set(key, payload);
        }
    }

    @Override
    public boolean setNx(String key, Object value, Duration expire) {
        requireKey(key);
        String payload = serialize(value);
        Boolean applied = expire == null
                ? redisTemplate.opsForValue().setIfAbsent(key, payload)
                : redisTemplate.opsForValue().setIfAbsent(key, payload, expire);
        return Boolean.TRUE.equals(applied);
    }

    @Override
    public <T> List<T> getList(String key, Class<T> elementType) {
        requireKey(key);
        requireNonNull(elementType, "elementType");
        List<String> entries = redisTemplate.opsForList().range(key, 0, -1);
        if (entries == null || entries.isEmpty()) {
            return Collections.emptyList();
        }
        return entries.stream()
                .map(item -> deserialize(item, elementType))
                .collect(Collectors.toList());
    }

    @Override
    public void setList(String key, List<?> allList) {
        requireKey(key);
        redisTemplate.delete(key);
        if (allList == null || allList.isEmpty()) {
            return;
        }
        redisTemplate.opsForList().rightPushAll(key, serializeList(allList));
    }

    @Override
    public void addList(String key, List<?> partOfList) {
        requireKey(key);
        if (partOfList == null || partOfList.isEmpty()) {
            return;
        }
        redisTemplate.opsForList().rightPushAll(key, serializeList(partOfList));
    }

    private void requireKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Cache key must not be blank");
        }
    }

    private List<String> serializeList(List<?> list) {
        return list.stream()
                .filter(Objects::nonNull)
                .map(this::serialize)
                .collect(Collectors.toList());
    }

    private String serialize(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String str) {
            return str;
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Could not serialize value for redis cache", ex);
        }
    }

    private <T> T deserialize(String payload, Class<T> type) {
        if (type == String.class) {
            return type.cast(payload);
        }
        try {
            return objectMapper.readValue(payload, type);
        } catch (Exception ex) {
            throw new IllegalStateException("Could not deserialize value for redis cache", ex);
        }
    }
}

