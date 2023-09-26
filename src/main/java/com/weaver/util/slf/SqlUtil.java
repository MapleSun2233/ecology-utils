package com.weaver.util.slf;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import weaver.conn.RecordSet;
import weaver.general.BaseBean;

import java.util.Arrays;

/**
 * @author slf
 * sql工具
 */
public class SqlUtil {
    private static final BaseBean utils = new BaseBean();

    /**
     * 执行任意sql结果转jsonArr
     * @param sql sql
     * @return jsonArr
     */
    public static JSONArray executeAndToJson(String sql) {
        try {
            RecordSet rs = new RecordSet();
            String[] columns = null;
            if (rs.execute(sql)) {
                columns = rs.getColumnName();
            }
            if (ObjectUtil.isNull(columns)) {
                return null;
            }
            JSONArray res = new JSONArray();
            while (rs.next()) {
                JSONObject obj = new JSONObject();
                Arrays.stream(columns).forEach(col -> obj.put(col, rs.getString(col)));
                res.add(obj);
            }
            return res;
        } catch (Exception e) {
            utils.writeLog("executeAndToJson exception: " + e.getMessage());
            return null;
        }
    }
}
