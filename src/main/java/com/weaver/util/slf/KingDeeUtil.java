package com.weaver.util.slf;

import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpRequest;
import com.alibaba.fastjson.JSONObject;
import weaver.general.BaseBean;

import java.util.Map;

/**
 * @author slf
 * @date 2023/7/27
 * 金蝶集成工具
 */
public class KingDeeUtil {
    private static final BaseBean UTILS = new BaseBean();
    /**
     * 获取appToken
     * @param config 配置信息
     * @return appToken
     */
    public static String getAppToken(Map<String, String> config){
        JSONObject body = new JSONObject();
        body.put("appId", config.get("appId"));
        body.put("appSecret", config.get("appSecret"));
        body.put("tenantid", config.get("tenantId"));
        body.put("accountId", config.get("accountId"));
        UTILS.writeLog("getAppToken params ::: " + body);
        String jsonStr = HttpRequest.post(config.get("appTokenApi"))
                .body(body.toJSONString())
                .execute().body();
        UTILS.writeLog("res ::: " + jsonStr);
        JSONObject result = JSONObject.parseObject(jsonStr);
        if (result.getBoolean("status")) {
            return result.getJSONObject("data").getString("app_token");
        } else {
            return StrUtil.EMPTY;
        }
    }

    /**
     * 获取accessToken
     * @param config 配置信息
     * @param appToken appToken
     * @return accessToken
     */
    public static String getAccessToken(Map<String, String> config, String appToken){
        JSONObject body = new JSONObject();
        body.put("user", config.get("user"));
        body.put("apptoken", appToken);
        body.put("tenantid", config.get("tenantId"));
        body.put("accountId", config.get("accountId"));
        body.put("usertype", config.get("usertype"));
        UTILS.writeLog("getAccessToken params ::: " + body);
        String jsonStr = HttpRequest.post(config.get("accessTokenApi"))
                .body(body.toJSONString())
                .execute().body();
        UTILS.writeLog("res ::: " + jsonStr);
        JSONObject result = JSONObject.parseObject(jsonStr);
        if (result.getBoolean("status")) {
            return result.getJSONObject("data").getString("access_token");
        } else {
            return StrUtil.EMPTY;
        }
    }


}
