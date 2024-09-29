package com.weaver.util.slf;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import weaver.conn.RecordSet;
import weaver.conn.RecordSetDataSource;
import weaver.general.BaseBean;

import java.util.Arrays;

/**
 * @author slf
 * sql工具
 */
public class SqlUtil {
    private static final BaseBean UTILS = new BaseBean();
    private static final String[] SENSITIVE_WORDS = {"delete", "update", "into", ";", "drop"};
    /**
     * 执行任意sql结果转jsonArr
     * @param sql sql
     * @return jsonArr
     */
    public static JSONArray executeAndToJson(String sql) {
        ValidatorUtil.builder()
                .append(sql.toLowerCase(), s -> !s.startsWith("select"), "sql配置错误，该方式禁止用于修改数据！")
                .append(sql.toLowerCase(), s -> Arrays.stream(SENSITIVE_WORDS).anyMatch(s::contains), "安全检查未通过，请勿包含敏感词！")
                .validate();
        RecordSet rs = new RecordSet();
        if (!rs.executeQuery(sql)) {
            UTILS.writeLog("SQL执行出错 :: " + sql);
            throw new RuntimeException("SQL执行出错，请检查SQL配置");
        }
        String[] columns = rs.getColumnName();
        if (ObjectUtil.isNull(columns) || columns.length == 0) {
            throw new RuntimeException("SQL字段列获取失败，请检查SQL配置");
        }
        JSONArray res = new JSONArray();
        while (rs.next()) {
            JSONObject obj = new JSONObject();
            Arrays.stream(columns).forEach(col -> obj.put(col, rs.getString(col)));
            res.add(obj);
        }
        return res;
    }
    /**
     * 使用外部数据源执行任意sql结果转jsonArr
     * @param sql sql
     * @return jsonArr
     */
    public static JSONArray executeAndToJson(String dataSourceName, String sql) {
        ValidatorUtil.builder()
                .append(sql.toLowerCase(), s -> !s.startsWith("select"), "sql配置错误，该方式禁止用于修改数据！")
                .append(sql.toLowerCase(), s -> Arrays.stream(SENSITIVE_WORDS).anyMatch(s::contains), "安全检查未通过，请勿包含敏感词！")
                .append(dataSourceName, StrUtil::isBlank, "数据源名称不能为空！")
                .validate();
        RecordSetDataSource rs = new RecordSetDataSource(dataSourceName);
        if (!rs.execute(sql)) {
            UTILS.writeLog("SQL执行出错 :: " + sql);
            throw new RuntimeException("SQL执行出错，请检查SQL配置");
        }
        String[] columns = rs.getColumnName();
        if (ObjectUtil.isNull(columns) || columns.length == 0) {
            throw new RuntimeException("SQL字段列获取失败，请检查SQL配置");
        }
        JSONArray res = new JSONArray();
        while (rs.next()) {
            JSONObject obj = new JSONObject();
            Arrays.stream(columns).forEach(col -> obj.put(col, rs.getString(col)));
            res.add(obj);
        }
        return res;
    }

    /**
     * 构建select语句
     * @param fields   字段
     * @param tableName 表名
     * @return sql
     */
    public static String buildSelectSql(String fields, String tableName) {
        return StrUtil.format("select {} from {}", fields, tableName);
    }

    /**
     * 构建select 语句
     * @param fields    字段
     * @param tableName 表名
     * @param condition 条件
     * @return sql
     */
    public static String buildSelectSql(String fields, String tableName, String condition) {
        String sql = buildSelectSql(fields, tableName);
        if (StrUtil.isBlank(condition)) {
            return sql;
        }
        return sql + StrUtil.SPACE + condition;
    }
}
