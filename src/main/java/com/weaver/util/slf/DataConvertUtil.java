package com.weaver.util.slf;

import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;

import java.math.BigDecimal;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author slf
 * @date 2023/11/24
 * 数据转换工具
 */
public class DataConvertUtil {
    public static String ZERO = "0";
    public static String PLACE_HOLDER_PATTERN = "\\$\\{(\\w+)\\}";

    /**
     * 非法字符串返回0字符串，用于合法化数值类型入参
     * @param num 数值字符串
     * @return 合法数值
     */
    public static String illegalStrToZero(String num) {
        if (NumberUtil.isNumber(num)) {
            return num;
        }
        return ZERO;
    }

    /**
     * 字符串安全转换到BigDecimal
     * @param num 数值字符串
     * @return BigDecimal
     */
    public static BigDecimal strToBigDecimal(String num) {
        try {
            return new BigDecimal(num);
        } catch (Exception e) {
            return new BigDecimal(ZERO);
        }
    }

    /**
     * 比较是否为0
     * @param b bigDecimal
     * @return boolean
     */
    public static boolean equalsZero(BigDecimal b) {
        return b.compareTo(BigDecimal.ZERO) == 0;
    }

    /**
     * 比较是否为0，非法字符认为是0
     * @param num num
     * @return boolean
     */
    public static boolean equalsZero(String num) {
        return equalsZero(strToBigDecimal(num));
    }

    /**
     * 比较是否为0
     * @param d double
     * @return boolean
     */
    public static boolean equalsZero(double d) {
        return equalsZero(BigDecimal.valueOf(d));
    }

    /**
     * 拆分id为字符串数组
     * @param ids ids
     * @return arr
     */
    public static String[] splitIdsToStrArr(String ids) {
        if (StrUtil.isBlank(ids)) {
            return new String[0];
        }
        return StrUtil.cleanBlank(ids).split(StrUtil.COMMA);
    }

    /**
     * 安全转换为double
      * @param str str
     * @return double
     */
    public static double securityToDouble(String str) {
        if (NumberUtil.isNumber(str)) {
            return NumberUtil.parseNumber(str).doubleValue();
        } else {
            return 0f;
        }
    }

    /**
     * 处理占位符并注入数据
     * @param templateStr 模板字符串
     * @param data data
     * @return handledStr
     */
    public static String handlePlaceHolderAndInjectData(String templateStr, Map<String, String> data) {
        Pattern pattern = Pattern.compile(PLACE_HOLDER_PATTERN);
        Matcher matcher = pattern.matcher(templateStr);
        while (matcher.find()) {
            templateStr = templateStr.replaceAll(convertPlaceHolder(matcher.group(0)), data.getOrDefault(matcher.group(1), StrUtil.EMPTY));
        }
        return templateStr;
    }
    /**
     * 处理占位符并注入数据
     * @param templateStr 模板字符串
     * @param data data
     * @return handledStr
     */
    public static String handlePlaceHolderAndInjectData(String templateStr, JSONObject data) {
        Pattern pattern = Pattern.compile(PLACE_HOLDER_PATTERN);
        Matcher matcher = pattern.matcher(templateStr);
        while (matcher.find()) {
            String value = ObjectUtil.isNull(data.getString(matcher.group(1))) ? StrUtil.EMPTY : data.getString(matcher.group(1));
            templateStr = templateStr.replaceAll(convertPlaceHolder(matcher.group(0)), value);
        }
        return templateStr;
    }

    /**
     * 转义展位符
     * @param placeHolder
     * @return
     */
    public static String convertPlaceHolder(String placeHolder) {
        return placeHolder.replace("$", "\\$").replace("{", "\\{").replace("}", "\\}");
    }
}
