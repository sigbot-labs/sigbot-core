package com.wl4g.signaltrading.poc.cache;

import java.time.Duration;
import java.util.List;

public interface ICache {
    <T> T get(String key, Class<T> type);

    void set(String key, Object value, Duration expire);

    boolean setNx(String key, Object value, Duration expire);

    <T> List<T> getList(String key, Class<T> elementType);

    void setList(String key, List<?> allList);

    void addList(String key, List<?> partOfList);
}

