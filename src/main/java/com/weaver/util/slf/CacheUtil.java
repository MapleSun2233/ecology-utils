package com.weaver.util.slf;

import com.cloudstore.dev.api.util.Util_DataCache;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
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
        KEYS.add(key);
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
        Util_DataCache.setObjVal(CACHE_PREFIX + key + TIMEOUT_SUFFIX, LocalDateTime.now().plus(timeout, unit));
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
                    clear(key);
                    return false;
                }
            } else {
                return true;
            }
        } else {
            clear(key);
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
        Util_DataCache.clearVal(cacheKey);
        Util_DataCache.clearVal(cacheKey + TIMEOUT_SUFFIX);
        KEYS.remove(key);
    }

    /**
     * 清除超时缓存
     */
    public static void clearTimeout() {
        KEYS.forEach(CacheUtil::contains);
    }
    /**
     * 清除指定后缀的缓存
     * @param suffix suffix
     */
    public static void clearContainSuffix(String suffix) {
        KEYS.stream().filter(key -> key.endsWith(suffix))
                .forEach(CacheUtil::clear);
    }

    /**
     * 清除所有缓存
     */
    public static void clearAll() {
        KEYS.forEach(CacheUtil::clear);
    }
}
