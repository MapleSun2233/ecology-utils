package com.weaver.util.slf;

import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import weaver.conn.RecordSet;
import weaver.general.BaseBean;
import weaver.hrm.User;

import java.util.HashSet;
import java.util.Set;
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
        workCodes = workCodes.replaceAll(StrUtil.COMMA, "','");
        RecordSet rs = new RecordSet();
        rs.execute(StrUtil.format("select id from HrmResource where workcode in ('{}')", workCodes));
        Set<String> userIds = new HashSet<>();
        while (rs.next()) {
            userIds.add(rs.getString("id"));
        }
        UTILS.writeLog("工号转换用户ID结果" + userIds);
        return String.join(StrUtil.COMMA, userIds);
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
        departmentCodes = departmentCodes.replaceAll(StrUtil.COMMA, "','");
        RecordSet rs = new RecordSet();
        rs.execute(StrUtil.format("select id from HrmDepartment where departmentcode in ('{}')", departmentCodes));
        Set<String> deptIds = new HashSet<>();
        while (rs.next()) {
            deptIds.add(rs.getString("id"));
        }
        UTILS.writeLog("部门编码转换部门ID结果" + deptIds);
        return String.join(StrUtil.COMMA, deptIds);
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
        companyCodes = companyCodes.replaceAll(StrUtil.COMMA, "','");
        RecordSet rs = new RecordSet();
        rs.execute(StrUtil.format("select id from HrmSubCompany where subcompanycode in ('{}')", companyCodes));
        Set<String> subCompIds = new HashSet<>();
        while (rs.next()) {
            subCompIds.add(rs.getString("id"));
        }
        UTILS.writeLog("分部编码转分部ID结果" + subCompIds);
        return String.join(StrUtil.COMMA, subCompIds);
    }
}
