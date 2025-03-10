package com.weaver.util.slf;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import weaver.general.BaseBean;

import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Pattern;

/**
 * @author slf
 * @date 2023/8/1
 */
public class JsonUtil {
    private static BaseBean UTILS = new BaseBean();
    private static Pattern JUDGE_SCIENTIFIC_NOTATION = Pattern.compile("^[+-]?\\d+\\.?\\d*[Ee][+-]?\\d+$");

    /**
     * 读取body
     * @param request request
     * @return body
     */
    public static JSONObject readBodyJsonFromRequest(HttpServletRequest request) {
        try {
            InputStream is = request.getInputStream();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int len = -1;
            while ((len = is.read(buffer)) != -1) {
                bos.write(buffer, 0, len);
            }
            bos.close();
            is.close();
            String json = bos.toString("UTF-8");
            // 消除可能因为安全机制导致的字符全角
            json = Convert.toDBC(json, Collections.singleton('，'));
            return JSONObject.parseObject(json);
        } catch (Exception e) {
            throw new RuntimeException("json body读取错误");
        }
    }
    /**
     * 字节数组转JSONArray，用来解决fastjson序列化默认将字节数组转Base64的问题
     * @param bytes
     * @return jsonArr
     */
    public static JSONArray byteArrToJsonArray(byte[] bytes) {
        return JSONArray.parseArray(Arrays.toString(bytes));
    }

    /**
     * json数组转byte数组
     * @param arr json
     * @return byte
     */
    public static byte[] jsonArrayToByte(JSONArray arr) {
        return JSONArray.parseObject(arr.toJSONString(), byte[].class);
    }

    /**
     * 输入流转JSONArray
     * @param inputStream 输入流
     * @return jsonArr
     */
    public static JSONArray inputStreamToJsonArray(InputStream inputStream) {
        return byteArrToJsonArray(IoUtil.readBytes(inputStream));
    }

    /**
     * 判断字符串是否是科学计数法
     * @param str str
     * @return boolean
     */
    public static boolean isScientificNotation(String str) {
        return JUDGE_SCIENTIFIC_NOTATION.matcher(str).find();
    }

    /**
     * 科学计数法转普通小数
     * @param str str
     * @return str
     */
    public static String convertScientificNotationToNormalString(String str) {
        return new BigDecimal(str).toPlainString();
    }

    /**
     * 构建标准返回信息
     * @param ok 状态
     * @param msg 消息
     * @param data 数据
     * @return result
     */
    public static String buildStandardResult(boolean ok, String msg, Object data) {
        JSONObject res = new JSONObject();
        res.put("status", ok);
        res.put("msg", msg);
        res.put("data", data);
        return res.toJSONString();
    }

    /**
     * 成功返回
     * @param data 数据
     * @return result
     */
    public static String buildSuccessResult(Object data) {
        return buildStandardResult(true, StrUtil.EMPTY, data);
    }

    /**
     * 失败返回
     * @param msg 消息
     * @return result
     */
    public static String buildFailureResult(String msg) {
        return buildStandardResult(false, msg, null);
    }

    /**
     * 安全解析JSONObject
     * @param json json
     * @param isReturnEmpty boolean
     * @return jsonObject
     */
    public static JSONObject securityParseJsonObject(String json, boolean isReturnEmpty) {
        try {
            return JSONObject.parseObject(json);
        } catch (Exception e) {
            UTILS.writeLog("json解析失败, json ::: " + json);
            if (isReturnEmpty) {
                return new JSONObject();
            }
            return null;
        }
    }

    /**
     * 安全解析JSONArray
     * @param json json
     * @param isReturnEmpty boolean
     * @return jsonArray
     */
    public static JSONArray securityParseJsonArray(String json, boolean isReturnEmpty) {
        try {
            return JSONArray.parseArray(json);
        } catch (Exception e) {
            UTILS.writeLog("json解析失败, json ::: " + json);
            if (isReturnEmpty) {
                return new JSONArray();
            }
            return null;
        }
    }

}
