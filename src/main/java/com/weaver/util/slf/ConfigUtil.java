package com.weaver.util.slf;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import weaver.conn.RecordSet;
import weaver.general.BaseBean;
import weaver.general.GCONST;

import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * @author slf
 * 配置工具
 */
public class ConfigUtil {
    private static final int EXPIRE = 3;
    private static final BaseBean UTILS = new BaseBean();
    private static String normalConfigTable = "uf_kfffpzgl";

    /**
     * 读取配置
     * @param fileName 配置文件名
     * @return map
     */
    public static Map<String, String> readPropertiesConfig(String fileName) {
        Map<String, String> config = new HashMap<>(10);
        try {
            String path = GCONST.getPropertyPath() + fileName + ".properties";
            List<String> lines = FileUtil.readUtf8Lines(path);
            lines.stream()
                    .filter(StrUtil::isNotBlank)
                    .filter(line -> !StrUtil.startWithAny(line, "#", "=") && !StrUtil.endWith(line, "=") && StrUtil.contains(line, "="))
                    .forEach(line -> {
                        int index = line.indexOf('=');
                        config.put(line.substring(0, index), line.substring(index+1));
                    });
            UTILS.writeLog(fileName + " ::: " + config);
        } catch (Exception e) {
            UTILS.writeLog("config read error ::: " + e.getMessage());
        }
        return config;
    }
    /**
     * 读取配置
     * @param fileName 配置文件名
     * @return map
     */
    public static Map<String, String> readPropertiesConfigWithCache(String fileName) {
        String path = GCONST.getPropertyPath() + fileName + ".properties";
        if (CacheUtil.contains(path)) {
            Map<String, String> config = CacheUtil.get(path, Map.class);
            UTILS.writeLog("file config read from cache");
            return config;
        } else {
            Map<String, String> config = readPropertiesConfig(fileName);
            CacheUtil.set(path, config, EXPIRE, ChronoUnit.MINUTES);
            return config;
        }
    }
    /**
     * 读取配置
     * @param fileName 配置文件名
     * @return json
     */
    public static JSONObject readJsonConfig(String fileName) {
        JSONObject res = new JSONObject();
        try {
            String path = GCONST.getPropertyPath() + fileName + ".json";
            res = JSONObject.parseObject(FileUtil.readUtf8String(path));
            UTILS.writeLog(fileName + " config ::: " + res.toJSONString());
        } catch (Exception e) {
            UTILS.writeLog("config read error ::: " + e.getMessage());
        }
        return res;
    }
    /**
     * 读取配置
     * @param fileName 配置文件名
     * @return json
     */
    public static JSONArray readJsonArrConfig(String fileName) {
        JSONArray res = new JSONArray();
        try {
            String path = GCONST.getPropertyPath() + fileName + ".json";
            res = JSONArray.parseArray(FileUtil.readUtf8String(path));
            UTILS.writeLog(fileName + " config ::: " + res.toJSONString());
        } catch (Exception e) {
            UTILS.writeLog("config read error ::: " + e.getMessage());
        }
        return res;
    }
    /**
     * 读取配置
     * @param fileName 配置文件名
     * @return json
     */
    public static JSONObject readJsonConfigWithCache(String fileName) {
        String path = GCONST.getPropertyPath() + fileName + ".json";
        if (CacheUtil.contains(path)) {
            JSONObject res = CacheUtil.get(path, JSONObject.class);
            UTILS.writeLog(fileName + " config read from cache");
            return res;
        } else {
            JSONObject res = readJsonConfig(fileName);
            CacheUtil.set(path, res, EXPIRE, ChronoUnit.MINUTES);
            return res;
        }
    }
    /**
     * 读取配置
     * @param fileName 配置文件名
     * @return json
     */
    public static JSONArray readJsonArrConfigWithCache(String fileName) {
        String path = GCONST.getPropertyPath() + fileName + ".json";
        if (CacheUtil.contains(path)) {
            JSONArray res = CacheUtil.get(path, JSONArray.class);
            UTILS.writeLog(fileName + " config read from cache");
            return res;
        } else {
            JSONArray res = readJsonArrConfig(fileName);
            CacheUtil.set(path, res, EXPIRE, ChronoUnit.MINUTES);
            return res;
        }
    }

    /**
     * 从数据库表获取配置
     * @param tableName 表名
     * @param configNameField 数据库配置项字段
     * @param configValueField 数据库配置值字段
     * @return config
     */
    public static Map<String, String> readConfigFromTable(String tableName, String configNameField, String configValueField) {
        try {
            RecordSet rs = new RecordSet();
            String sql = StrUtil.format("select {},{} from {}", configNameField, configValueField, tableName);
            UTILS.writeLog("read config from table, sql :: " + sql);
            rs.execute(sql);
            Map<String, String> config = new HashMap<>(rs.getCounts());
            while (rs.next()) {
                config.put(Convert.toDBC(rs.getString(configNameField)), Convert.toDBC(rs.getString(configValueField)));
            }
            UTILS.writeLog(tableName + " ::: " + config);
            return config;
        } catch (Exception e) {
            UTILS.writeLog("config read error :: " + e.getMessage());
            return Collections.emptyMap();
        }
    }
    /**
     * 从数据库表根据条件获取配置
     * @param tableName 表名
     * @param configNameField 数据库配置项字段
     * @param configValueField 数据库配置值字段
     * @param condition 条件
     * @return config
     */
    public static Map<String, String> readConfigFromTableOnCondition(String tableName, String configNameField, String configValueField, String condition) {
        try {
            RecordSet rs = new RecordSet();
            String sql = StrUtil.format("select {},{} from {} where {}", configNameField, configValueField, tableName, condition);
            UTILS.writeLog("read config from table, sql :: " + sql);
            rs.execute(sql);
            Map<String, String> config = new HashMap<>(rs.getCounts());
            while (rs.next()) {
                config.put(Convert.toDBC(rs.getString(configNameField)), Convert.toDBC(rs.getString(configValueField)));
            }
            UTILS.writeLog(tableName + " ::: " + config);
            return config;
        } catch (Exception e) {
            UTILS.writeLog("config read error :: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * 设置通用配置表
     * @param tableName 新配置表名
     */
    public static void setNormalConfigTable(String tableName) {
        normalConfigTable = tableName;
    }

    /**
     * 根据服务名读取配置内容
     * @param serviceName 服务名
     * @return 配置内容
     */
    public static String readConfigContentFromTableByServiceName(String serviceName) {
        String sql = StrUtil.format("select pznr from {} where fwm = '{}'", normalConfigTable, serviceName);
        UTILS.writeLog("readConfigContentFromTableByServiceName: " + sql);
        RecordSet rs = new RecordSet();
        ValidatorUtil.validate(rs.executeQuery(sql) && rs.next(), BooleanUtil::isFalse, "根据服务名获取配置失败： " + serviceName);
        return Convert.toDBC(rs.getString("pznr"));
    }

    /**
     * 根据服务名读取properties配置
     * @param serviceName 服务名
     * @return properties
     */
    public static Map<String, String> readPropertiesConfigByServiceName(String serviceName) {
        String configContent = readConfigContentFromTableByServiceName(serviceName);
        String[] contentArr = configContent.split(StrUtil.LF);
        Map<String, String> config = new HashMap<>(contentArr.length);
        Arrays.stream(contentArr)
                .filter(StrUtil::isNotBlank)
                .filter(line -> !StrUtil.startWithAny(line, "#", "=") && !StrUtil.endWith(line, "=") && StrUtil.contains(line, "="))
                .forEach(line -> {
                    int index = line.indexOf('=');
                    config.put(line.substring(0, index), line.substring(index+1));
                });
        return config;
    }

    /**
     * 根据服务名读取json配置
     * @param serviceName 服务名
     * @return json配置
     */
    public static JSONObject readJsonConfigByServiceName(String serviceName) {
        return JSONObject.parseObject(readConfigContentFromTableByServiceName(serviceName));
    }

    /**
     * 根据服务名读取json数组配置
     * @param serviceName 服务名
     * @return  json数组
     */
    public static JSONArray readJsonArrByServiceName(String serviceName) {
        return JSONArray.parseArray(readConfigContentFromTableByServiceName(serviceName));
    }
}
