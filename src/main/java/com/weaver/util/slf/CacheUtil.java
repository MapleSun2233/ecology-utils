package com.weaver.util.slf;

import cn.hutool.core.util.ObjectUtil;
import com.cloudstore.api.util.Util_Redis;
import com.cloudstore.dev.api.util.Util_DataCache;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * @author slf
 * @date 2023/9/14
 */
public class CacheUtil {
    private static final String TIMEOUT_SUFFIX = "-timeout";
    private static final String CACHE_PREFIX = "slf-";

    /**
     * 设置缓存，过期时间默认30分钟
     * @param key key
     * @param value value
     */
    public static void set(String key, Object value) {
        set(key, value, 1800);
    }

    /**
     * 废弃，已更换底层实现，时间单位不生效，timeout单位固定为秒
     * @param key key
     * @param value value
     * @param timeout timeout
     * @param unit unit
     */
    @Deprecated
    public static void set(String key, Object value, int timeout, ChronoUnit unit) {
        set(key, value, timeout);
    }

    /**
     * 设置过期缓存
     * @param key
     * @param value
     * @param seconds
     */
    public static void set(String key, Object value, int seconds) {
        if (ObjectUtil.isNull(Util_Redis.getInstance())) {
            // 无redis时，额外设置过期时间KV
            Util_DataCache.setObjVal(CACHE_PREFIX + key + TIMEOUT_SUFFIX, LocalDateTime.now().plus(seconds, ChronoUnit.SECONDS));
        }
        Util_DataCache.setObjVal(CACHE_PREFIX + key, value, seconds);
    }

    /**
     * 判断数据是否存在且有效
     * @param key key
     * @return bool
     */
    public static boolean contains(String key) {
        if (ObjectUtil.isNull(Util_Redis.getInstance())) {
            // 无redis时检查过期时间KV
            if (Util_DataCache.containsKeylWithEh(CACHE_PREFIX + key)) {
                String timeoutKey = CACHE_PREFIX + key + TIMEOUT_SUFFIX;
                if (Util_DataCache.containsKey(timeoutKey)) {
                    LocalDateTime timeout = (LocalDateTime)Util_DataCache.getObjVal(timeoutKey);
                    if (LocalDateTime.now().isBefore(timeout)) {
                        // 数据key，过期时间key都存在且未过期，则视为有效
                        return true;
                    }
                }
            }
            return false;
        }
        return Util_DataCache.containsKeyWithRedis(CACHE_PREFIX + key);
    }

    /**
     * 指定类型获取缓存对象
     * @param key key
     * @param t T
     * @param <T> T
     * @return T
     */
    public static <T> T get(String key, Class<T> t) {
        Object obj = get(key);
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
        Util_DataCache.clearVal(CACHE_PREFIX + key);
        Util_DataCache.clearValWithEh(CACHE_PREFIX + key + TIMEOUT_SUFFIX);
    }

    /**
     * 废弃无效，清除超时缓存
     */
    @Deprecated
    public static void clearTimeout() {
//        KEYS.forEach(CacheUtil::contains);
    }
    /**
     * 废弃无效，清除指定后缀的缓存
     * @param suffix suffix
     */
    @Deprecated
    public static void clearContainSuffix(String suffix) {
//        KEYS.stream().filter(key -> key.endsWith(suffix))
//                .forEach(CacheUtil::clear);
    }

    /**
     * 废弃无效,清除所有缓存
     */
    @Deprecated
    public static void clearAll() {
//        KEYS.forEach(CacheUtil::clear);
    }
}
