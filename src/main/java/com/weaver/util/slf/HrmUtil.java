package com.weaver.util.slf;

import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import weaver.conn.RecordSet;
import weaver.general.BaseBean;
import weaver.hrm.User;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author slf
 * @date 2023/10/31
 */
public class HrmUtil {
    private static BaseBean UTILS = new BaseBean();
    /**
     * 兼容workCode获取user
     * @param userId 用户id或工号
     * @param isWorkCode 是否是工号
     * @return 用户对象
     */
    public static User getUserCompatibleWorkCode(String userId, boolean isWorkCode) {
        if (StrUtil.isBlank(userId)) {
            UTILS.writeLog("userId缺失， 默认获取管理员用户对象");
            return User.getUser(1, 0);
        } else {
            if (isWorkCode) {
                String realUserId = convertWorkCodesToUserIds(userId);
                if (StrUtil.isNotBlank(realUserId) && NumberUtil.isInteger(realUserId)) {
                    UTILS.writeLog(StrUtil.format("转换工号{}为用户ID{}", userId, realUserId));
                    return User.getUser(NumberUtil.parseInt(realUserId), 0);
                } else {
                    UTILS.writeLog("工号转换失败，默认获取管理员用户对象");
                    return User.getUser(1, 0);
                }
            } else {
                return User.getUser(NumberUtil.parseInt(userId), 0);
            }
        }
    }
    /**
     * 工号转用户id
     * @param workCodes 工号
     * @return 用户id
     */
    public static String convertWorkCodesToUserIds(String workCodes) {
        if (StrUtil.isBlank(workCodes)) {
            return StrUtil.EMPTY;
        }
        UTILS.writeLog("转工号" + workCodes);
        String[] codeRecord = workCodes.replaceAll(StrUtil.SPACE, StrUtil.EMPTY).split(StrUtil.COMMA);
        workCodes = workCodes.replaceAll(StrUtil.COMMA, "','");
        RecordSet rs = new RecordSet();
        rs.execute(StrUtil.format("select id, workcode from HrmResource where workcode in ('{}')", workCodes));
        Map<String, String> userIds = new HashMap<>(rs.getCounts());
        while (rs.next()) {
            userIds.put(rs.getString("workcode"), rs.getString("id"));
        }
        UTILS.writeLog("工号转换用户ID结果" + userIds);
        String failCodes = Arrays.stream(codeRecord).filter(code -> !userIds.containsKey(code)).collect(Collectors.joining(StrUtil.COMMA));
        ValidatorUtil.validate(failCodes, StrUtil::isNotBlank, StrUtil.format("工号转换失败，{}不存在", failCodes));
        return String.join(StrUtil.COMMA, userIds.values());
    }

    /**
     * 部门编码转部门id
     * @param departmentCodes 部门编码
     * @return 部门id
     */
    public static String convertDepartmentCodesToDepartmentIds(String departmentCodes) {
        if (StrUtil.isBlank(departmentCodes)) {
            return StrUtil.EMPTY;
        }
        UTILS.writeLog("转部门编码" + departmentCodes);
        String[] codeRecord = departmentCodes.replaceAll(StrUtil.SPACE, StrUtil.EMPTY).split(StrUtil.COMMA);
        departmentCodes = departmentCodes.replaceAll(StrUtil.COMMA, "','");
        RecordSet rs = new RecordSet();
        rs.execute(StrUtil.format("select id, departmentcode from HrmDepartment where departmentcode in ('{}')", departmentCodes));
        Map<String, String> deptIds = new HashMap<>(rs.getCounts());
        while (rs.next()) {
            deptIds.put(rs.getString("departmentcode"), rs.getString("id"));
        }
        UTILS.writeLog("部门编码转换部门ID结果" + deptIds);
        String failCodes = Arrays.stream(codeRecord).filter(code -> !deptIds.containsKey(code)).collect(Collectors.joining(StrUtil.COMMA));
        ValidatorUtil.validate(failCodes, StrUtil::isNotBlank, StrUtil.format("部门编码转换失败，{}不存在", failCodes));
        return String.join(StrUtil.COMMA, deptIds.values());
    }

    /**
     * 公司编码转公司id
     * @param companyCodes 公司编码
     * @return 公司id
     */
    public static String convertCompanyCodesToCompanyIds(String companyCodes) {
        if (StrUtil.isBlank(companyCodes)) {
            return StrUtil.EMPTY;
        }
        UTILS.writeLog("转换分部编码" + companyCodes);
        String[] codeRecord = companyCodes.replaceAll(StrUtil.SPACE, StrUtil.EMPTY).split(StrUtil.COMMA);
        companyCodes = companyCodes.replaceAll(StrUtil.COMMA, "','");
        RecordSet rs = new RecordSet();
        rs.execute(StrUtil.format("select id, subcompanycode from HrmSubCompany where subcompanycode in ('{}')", companyCodes));
        Map<String, String> subCompIds = new HashMap<>(rs.getCounts());
        while (rs.next()) {
            subCompIds.put(rs.getString("subcompanycode"), rs.getString("id"));
        }
        UTILS.writeLog("分部编码转分部ID结果" + subCompIds);
        String failCodes = Arrays.stream(codeRecord).filter(code -> !subCompIds.containsKey(code)).collect(Collectors.joining(StrUtil.COMMA));
        ValidatorUtil.validate(failCodes, StrUtil::isNotBlank, StrUtil.format("分部编码转换失败，{}不存在", failCodes));
        return String.join(StrUtil.COMMA, subCompIds.values());
    }

    /**
     * 获取不带分部的部门树
     * @return
     */
    public JSONObject getDepartmentTreeWithoutCompany() {
        RecordSet rs = new RecordSet();
        Map<Integer, JSONObject> deptMap = new HashMap<>();
        LinkedList<JSONObject> handleQueue = new LinkedList<>();
        JSONArray subDepartments = new JSONArray();
        // 1. 获取所有的一级部门，并建立索引映射
        rs.execute("select id, departmentname, departmentcode, canceled from HrmDepartment where supdepid is null or supdepid = 0");
        while (rs.next()) {
            int id = rs.getInt("id");
            JSONObject item = new JSONObject();
            item.put("id", id);
            item.put("name", rs.getString("departmentname"));
            item.put("code", rs.getString("departmentcode"));
            item.put("canceled", StrUtil.equals("1", rs.getString("canceled")) ? true : false);
            item.put("subDepartments", new JSONArray());
            item.put("parent", 0);
            subDepartments.add(item);
            deptMap.put(id, item);
        }
        // 2. 获取所有非一级部门，循环处理，并建立索引映射，直到所有部门被处理完毕
        rs.execute("select id, departmentname, departmentcode, supdepid, canceled from HrmDepartment where supdepid  > 0");
        while (rs.next()) {
            int id = rs.getInt("id");
            int parent = rs.getInt("supdepid");
            JSONObject item = new JSONObject();
            item.put("id", id);
            item.put("name", rs.getString("departmentname"));
            item.put("code", rs.getString("departmentcode"));
            item.put("canceled", StrUtil.equals("1", rs.getString("canceled")) ? true : false);
            item.put("subDepartments", new JSONArray());
            item.put("parent", parent);
            if (deptMap.containsKey(parent)) {
                deptMap.get(parent).getJSONArray("subDepartments").add(item);
                deptMap.put(id, item);
            } else {
                handleQueue.offerLast(item);
            }
        }
        while (!handleQueue.isEmpty()) {
            JSONObject item = handleQueue.pollFirst();
            int parent = item.getInteger("parent");
            if (deptMap.containsKey(parent)) {
                deptMap.get(parent).getJSONArray("subDepartments").add(item);
                deptMap.put(item.getInteger("id"), item);
            } else {
                handleQueue.offerLast(item);
            }
        }
        JSONObject root = new JSONObject();
        root.put("name", "root");
        root.put("subDepartments", subDepartments);
        return root;
    }

    /**
     * 根据用户id获取部门id
     * @param userId 用户id
     * @return 部门id
     */
    public static int getDepartmentIdByUserId(int userId) {
        RecordSet rs = new RecordSet();
        if (rs.execute("select departmentid from HrmResource where id = " + userId) && rs.next()) {
            return rs.getInt("departmentid");
        } else {
            return -1;
        }
    }
}
