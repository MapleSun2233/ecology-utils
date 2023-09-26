package com.weaver.util.slf;

import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;
import weaver.general.BaseBean;

import java.util.HashMap;
import java.util.Map;

/**
 * @author slf
 * @date 2023/8/16
 * 简易的IOC容器实现，用于解决对象频繁实例化的问题
 */
public class IocUtil {
    private final static Map<Class, Object> OBJ_HOLDER = new HashMap<>();
    private final static BaseBean LOG = new BaseBean();
    public static <T> T getObj(Class<T> clazz, Object... params) {
        // 是null直接返回null
        if (ObjectUtil.isNull(clazz)) {
            return null;
        }
        Object obj = OBJ_HOLDER.get(clazz);
        // 判断是否存在
        if (ObjectUtil.isNull(obj)) {
            obj = ReflectUtil.newInstance(clazz, params);
            OBJ_HOLDER.put(clazz, obj);
            LOG.writeLog(StrUtil.format("IocUtil ::: new Instance class {} params {}", clazz.getName(), ArrayUtil.toString(params)));
        }
        return (T) obj;
    }
}
