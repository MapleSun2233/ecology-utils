package com.weaver.util.slf;

import com.cloudstore.dev.api.util.Util_DataCache;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author slf
 * @date 2023/9/14
 */
public class CacheUtil {
    private static final Set<String> KEYS = new CopyOnWriteArraySet<>();
    private static final String TIMEOUT_SUFFIX = "-timeout";
    private static final String CACHE_PREFIX = "slf-";

    /**
     * 设置永久缓存
     * @param key key
     * @param value value
     */
    public static void set(String key, Object value) {
        String cacheKey = CACHE_PREFIX + key;
        Util_DataCache.setObjVal(cacheKey, value);
        KEYS.add(cacheKey);
    }

    /**
     * 设置缓存，指定过期时间
     * @param key key
     * @param value value
     * @param timeout timeout
     * @param unit unit
     */
    public static void set(String key, Object value, int timeout, ChronoUnit unit) {
        set(key, value);
        String timeoutKey = CACHE_PREFIX + key + TIMEOUT_SUFFIX;
        Util_DataCache.setObjVal(timeoutKey, LocalDateTime.now().plus(timeout, unit));
        KEYS.add(timeoutKey);
    }

    /**
     * 判断数据是否存在且有效
     * @param key key
     * @return bool
     */
    public static boolean contains(String key) {
        String cacheKey = CACHE_PREFIX + key;
        String timeoutKey = cacheKey + TIMEOUT_SUFFIX;
        if (Util_DataCache.containsKey(cacheKey)) {
            if (Util_DataCache.containsKey(timeoutKey)) {
                LocalDateTime timeout = (LocalDateTime)Util_DataCache.getObjVal(timeoutKey);
                if (LocalDateTime.now().isBefore(timeout)) {
                    return true;
                } else {
                    Util_DataCache.clearVal(cacheKey);
                    Util_DataCache.clearVal(timeoutKey);
                    return false;
                }
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    /**
     * 指定类型获取缓存对象
     * @param key key
     * @param t T
     * @param <T> T
     * @return T
     */
    public static <T> T get(String key, Class<T> t) {
        Object obj = Util_DataCache.getObjVal(CACHE_PREFIX + key);
        if (t.isInstance(obj)) {
            return t.cast(obj);
        }
        return null;
    }

    /**
     * 获取缓存对象
     * @param key key
     * @return obj
     */
    public static Object get(String key) {
        return Util_DataCache.getObjVal(CACHE_PREFIX + key);
    }

    /**
     * 清除缓存
     * @param key key
     */
    public static void clear(String key) {
        String cacheKey = CACHE_PREFIX + key;
        String timeoutKey = cacheKey + TIMEOUT_SUFFIX;
        Util_DataCache.clearVal(cacheKey);
        Util_DataCache.clearVal(timeoutKey);
        KEYS.remove(key);
    }

    /**
     * 清除超时缓存
     */
    public static void clearTimeout() {
        Set<String> removeSet = new HashSet<>();
        for (String key : KEYS) {
            if (!contains(key)) {
                removeSet.add(key);
            }
        }
        KEYS.removeAll(removeSet);
    }
    /**
     * 清除指定后缀的缓存
     * @param suffix suffix
     */
    public static void clearContainSuffix(String suffix) {
        Set<String> removeSet = new HashSet<>();
        for (String key : KEYS) {
            if (key.endsWith(suffix)) {
                clear(key);
                removeSet.add(key);
            }
        }
        for (String key : removeSet) {
            KEYS.remove(key);
        }
    }

    /**
     * 清除所有缓存
     */
    public static void clearAll() {
        for (String key : KEYS) {
            String cacheKey = CACHE_PREFIX + key;
            String timeoutKey = cacheKey + TIMEOUT_SUFFIX;
            Util_DataCache.clearVal(cacheKey);
            Util_DataCache.clearVal(timeoutKey);
        }
        KEYS.clear();
    }
}
