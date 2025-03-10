package com.weaver.util.slf;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import weaver.conn.RecordSet;
import weaver.general.BaseBean;

import java.math.BigDecimal;
import java.util.*;
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
    private static final BaseBean UTILS = new BaseBean();

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
     * 转义展位符
     * @param placeHolder
     * @return
     */
    public static String convertPlaceHolder(String placeHolder) {
        return placeHolder.replace("$", "\\$").replace("{", "\\{").replace("}", "\\}");
    }

    /**
     * 根据策略sql转换数据
     *
     * @param dataConvertStrategy 数据转换策略集合
     * @param data data 原值
     * @param strategyName 策略名称
     * @return 结果值
     */
    public static String convertDataByStrategySql(Map<String, String> dataConvertStrategy, String data, String strategyName) {
        ValidatorUtil.validate(strategyName, StrUtil::isBlank, "被指定的数据转换策略名不能为空");
        if (StrUtil.isBlank(data)) {
            return null;
        }
        String result = null;
        RecordSet rs = new RecordSet();
        Set<String> idList = new LinkedHashSet<>();
        if (strategyName.endsWith("_multiple")) {
            String finalStrategyName = strategyName.substring(0, strategyName.length() - 9);
            ValidatorUtil.validate(dataConvertStrategy.containsKey(finalStrategyName), BooleanUtil::isFalse, StrUtil.format("未找到数据转换策略{}， 请联系管理员添加数据转换策略", finalStrategyName));
            String convertSql = dataConvertStrategy.get(finalStrategyName);
            String[] paramArr = data.split(StrUtil.COMMA);
            if (convertSql.contains("{}")) {
                for (String param : paramArr) {
                    if (rs.executeQuery(StrUtil.format(convertSql, param)) && rs.next()) {
                        idList.add(rs.getString(1));
                    }
                }
            } else {
                for (String param : paramArr) {
                    if (rs.executeQuery(convertSql, param) && rs.next()) {
                        idList.add(rs.getString(1));
                    }
                }
            }
            if (!idList.isEmpty()) {
                result = StrUtil.join(StrUtil.COMMA, idList);
            }
        } else {
            ValidatorUtil.validate(dataConvertStrategy.containsKey(strategyName), BooleanUtil::isFalse, StrUtil.format("未找到数据转换策略{}， 请联系管理员添加数据转换策略", strategyName));
            String convertSql = dataConvertStrategy.get(strategyName);
            if (convertSql.contains("{}")) {
                if (rs.executeQuery(StrUtil.format(convertSql, data)) && rs.next()) {
                    result = rs.getString(1);
                }
            } else {
                if (rs.executeQuery(convertSql, data) && rs.next()) {
                    result = rs.getString(1);
                }
            }
        }
        UTILS.writeLog(StrUtil.format("convert {} to {}", data, result));
        return result;
    }
}
