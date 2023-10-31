package com.weaver.util.slf;

import cn.hutool.core.util.StrUtil;
import weaver.conn.RecordSet;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author slf
 * @date 2023/10/31
 */
public class HrmUtil {
    /**
     * 工号转用户id
     * @param workCodes 工号
     * @return 用户id
     */
    public static String convertWorkCodesToUserIds(String workCodes) {
        if (StrUtil.isBlank(workCodes)) {
            return StrUtil.EMPTY;
        }
        workCodes = workCodes.replaceAll(StrUtil.COMMA, "','");
        RecordSet rs = new RecordSet();
        rs.execute(StrUtil.format("select id from HrmResource where workcode in ('{}')", workCodes));
        Set<String> userIds = new HashSet<>();
        while (rs.next()) {
            userIds.add(rs.getString("id"));
        }
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
        departmentCodes = departmentCodes.replaceAll(StrUtil.COMMA, "','");
        RecordSet rs = new RecordSet();
        rs.execute(StrUtil.format("select id from HrmDepartment where departmentcode in ('{}')", departmentCodes));
        Set<String> userIds = new HashSet<>();
        while (rs.next()) {
            userIds.add(rs.getString("id"));
        }
        return String.join(StrUtil.COMMA, userIds);
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
        companyCodes = companyCodes.replaceAll(StrUtil.COMMA, "','");
        RecordSet rs = new RecordSet();
        rs.execute(StrUtil.format("select id from HrmSubCompany where subcompanycode in ('{}')", companyCodes));
        Set<String> userIds = new HashSet<>();
        while (rs.next()) {
            userIds.add(rs.getString("id"));
        }
        return String.join(StrUtil.COMMA, userIds);
    }
}
