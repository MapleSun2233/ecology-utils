package com.weaver.util.slf;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import weaver.conn.RecordSet;
import weaver.formmode.setup.ModeRightInfo;
import weaver.general.BaseBean;
import com.weaver.util.slf.entity.SyncWriteDataConfig;

import java.util.*;
import java.util.stream.Collectors;


/**
 * @author slf
 * 建模工具
 */
public class ModelUtil {
    private static final String CHECK_EXISTS_SQL = "select count(id) as count from {} where {} = '{}'";
    private static final String CHECK_EXISTS_IN_DETAIL_SQL = "select count(id) as count from {} where mainid = {} and {} = '{}'";
    private static final String QUERY_NEW_ID_SQL = "select id from {} where id > {}";
    private static final String CLEAN_DETAIL_SQL = "delete from {} where mainid={}";
    private static final String MODE_TABLE_STATIC_FIELDS = ",formmodeid,modedatacreater,modedatacreatertype,modedatacreatedate,modedatacreatetime,modedatamodifydatetime";
    private static final int BATCH_NUM = 500;

    private static final BaseBean UTILS = new BaseBean();
    /**
     * 写入数据到建模表
     *
     * @param config 配置
     * @param data 被写入数据
     */
    public static void writeData(SyncWriteDataConfig config, JSONArray data) {
        UTILS.writeLog("开始写入数据...");
        RecordSet rs = new RecordSet();
        String modeStaticInfo = StrUtil.format("'{}','{}','{}','{}','{}','{}'",
                config.getFormModeId(), 1, 0,
                DatePattern.NORM_DATE_FORMAT.format(DateTime.now()),
                DatePattern.NORM_TIME_FORMAT.format(DateTime.now()),
                DatePattern.NORM_DATETIME_FORMAT.format(DateTime.now()));
        try {
            for (int i = 0; i < data.size(); i++) {
                if (checkExists(config, data.getJSONObject(i).getString(config.getRemoteOnlyCheckField()), rs)) {
                    rs.execute(buildUpdateSql(config, data.getJSONObject(i)));
                } else {
                    rs.execute(buildInsertSql(config, data.getJSONObject(i), modeStaticInfo));
                    buildUfAuth(config, rs);
                }
            }
            UTILS.writeLog("写入数据" + data.size() + "条，写入数据完成...");
        } catch (Exception e) {
            UTILS.writeLog("写入数据错误，异常消息:" + e.getMessage());
        }
    }
    /**
     * 批量写入数据到建模表，不支持Oracle数据库
     * @param config 配置
     * @param data 被写入数据
     * @param isUpdate 是否更新
     */
    public static void batchWriteData(SyncWriteDataConfig config, JSONArray data, boolean isUpdate) {
        UTILS.writeLog("开始写入数据...");
        RecordSet rs = new RecordSet();
        String modeStaticInfo = StrUtil.format("'{}','{}','{}','{}','{}','{}'",
                config.getFormModeId(), 1, 0,
                DatePattern.NORM_DATE_FORMAT.format(DateTime.now()),
                DatePattern.NORM_TIME_FORMAT.format(DateTime.now()),
                DatePattern.NORM_DATETIME_FORMAT.format(DateTime.now()));
        try {
            // 计算批次
            int batchCount = data.size() / BATCH_NUM;
            if (data.size() % BATCH_NUM > 0) {
                batchCount ++;
            }
            Integer oldMaxId = Optional.ofNullable(getMaxId(config.getLocalTable(), rs)).orElse(0);
            int skipCount = 0;
            int updateCount = 0;
            for (int b = 0; b < batchCount; b++) {
                StringBuilder sb = new StringBuilder(buildInsertSqlPrefix(config));
                int boundary = b * BATCH_NUM + BATCH_NUM;
                for (int i = b * BATCH_NUM; i < boundary && i < data.size(); i++) {
                    if (checkExists(config, data.getJSONObject(i).getString(config.getRemoteOnlyCheckField()), rs)) {
                        if (isUpdate) {
                            rs.execute(buildUpdateSql(config, data.getJSONObject(i)));
                            updateCount++;
                        } else {
                            skipCount++;
                        }
                    } else {
                        sb.append("(").append(buildInsertData(config, data.getJSONObject(i), modeStaticInfo)).append("),");
                    }
                }
                if (sb.charAt(sb.length() - 1) == StrUtil.C_COMMA) {
                    sb.deleteCharAt(sb.length() - 1);
                    rs.execute(sb.toString());
                }
            }
            buildUfAuthBatch(config, oldMaxId, rs);
            if (isUpdate) {
                UTILS.writeLog("更新数据" + updateCount + "条");
            } else {
                UTILS.writeLog("跳过重复数据" + skipCount + "条");
            }
            UTILS.writeLog("写入数据" + (data.size() - updateCount - skipCount) + "条，写入数据完成...");
        } catch (Exception e) {
            e.printStackTrace();
            UTILS.writeLog("写入数据错误，异常消息:" + e.getMessage());
        }
    }
    /**
     * 批量写入数据到建模表明细表，不支持Oracle数据库
     * isUpdate is true: isSkip决定更新还是跳过
     * isUpdate is false: 直接清空该mainid的明细再插入
     * @param config 配置
     * @param mainId 主表id
     * @param data 被写入数据
     * @param isUpdate 是否更新
     * @param isSkip 是否跳过
     */
    public static void batchWriteDetail(SyncWriteDataConfig config, String mainId, JSONArray data, boolean isUpdate, boolean isSkip) {
        RecordSet rs = new RecordSet();
        if (isUpdate) {
            if (StrUtil.isBlank(config.getLocalOnlyCheckField()) || StrUtil.isBlank(config.getRemoteOnlyCheckField())) {
                UTILS.writeLog("唯一确认字段未配置，无法完成明细表更新，任务结束");
                return;
            }
        } else {
            rs.execute(StrUtil.format(CLEAN_DETAIL_SQL, config.getLocalTable(), mainId));
        }
        UTILS.writeLog("开始写入数据...");
        try {
            // 计算批次
            int batchCount = data.size() / BATCH_NUM;
            if (data.size() % BATCH_NUM > 0) {
                batchCount ++;
            }
            int skipCount = 0;
            int updateCount = 0;
            for (int b = 0; b < batchCount; b++) {
                StringBuilder sb = new StringBuilder(buildInsertDetailSqlPrefix(config));
                int boundary = b * BATCH_NUM + BATCH_NUM;
                for (int i = b * BATCH_NUM; i < boundary && i < data.size(); i++) {
                    if (isUpdate && checkExistsInDetail(config, data.getJSONObject(i).getString(config.getRemoteOnlyCheckField()), mainId, rs)) {
                        if (isSkip) {
                            skipCount++;
                        } else {
                            rs.execute(buildUpdateSql(config, data.getJSONObject(i)));
                            updateCount++;
                        }
                    } else {
                        sb.append("(").append(mainId).append(StrUtil.COMMA)
                                .append(buildInsertData(config, data.getJSONObject(i), StrUtil.EMPTY))
                                .append("),");
                    }
                }
                if (sb.charAt(sb.length() - 1) == StrUtil.C_COMMA) {
                    sb.deleteCharAt(sb.length() - 1);
                    rs.execute(sb.toString());
                }
            }
            if (isUpdate) {
                if (isSkip) {
                    UTILS.writeLog("跳过重复数据" + skipCount + "条");
                } else {
                    UTILS.writeLog("更新数据" + updateCount + "条");
                }
            }
            UTILS.writeLog("写入数据" + (data.size() - updateCount - skipCount) + "条，写入数据完成...");
        } catch (Exception e) {
            UTILS.writeLog("写入数据错误，异常消息:" + e.getMessage());
        }
    }



    /**
     * 构建数据写入配置
     * @param config config
     * @return syncConfig
     */
    public static SyncWriteDataConfig buildWriteConfig(Map<String, String> config) {
        String[] emptyArr = new String[0];
        return SyncWriteDataConfig.builder()
                .remoteFields(StrUtil.isBlank(config.get("remoteFields")) ? emptyArr : config.get("remoteFields").split(","))
                .localTable(config.get("localTable"))
                .localFields(StrUtil.isBlank(config.get("localFields")) ? emptyArr : config.get("localFields").split(","))
                .localOnlyCheckField(config.get("localOnlyCheckField"))
                .remoteOnlyCheckField(config.get("remoteOnlyCheckField"))
                .formModeId(NumberUtil.isInteger(config.get("formModeId")) ? Integer.parseInt(config.get("formModeId")) : null)
                .doubleIndex(StrUtil.isBlank(config.get("doubleIndex")) ? Collections.emptyList() : Arrays.stream(config.get("doubleIndex").split(",")).filter(NumberUtil::isInteger).map(Integer::parseInt).collect(Collectors.toList()))
                .build();
    }

    /**
     * 获取插入sql
     *
     * @param config 配置
     * @param data           数据体
     * @param modeStaticInfo 建模表固定插入
     * @return sql
     */
    private static String buildInsertSql(SyncWriteDataConfig config, JSONObject data, String modeStaticInfo) {
        StringBuilder sqlInsert = new StringBuilder(buildInsertSqlPrefix(config));
        sqlInsert.append("(").append(buildInsertData(config, data, modeStaticInfo)).append(")");
        return sqlInsert.toString();
    }

    /**
     * 构建插入数据
     *
     * @param config 配置
     * @param data           数据体
     * @param modeStaticInfo 建模表固定插入
     * @return sql
     */
    private static String buildInsertData(SyncWriteDataConfig config, JSONObject data, String modeStaticInfo) {
        StringBuilder sqlInsert = new StringBuilder();
        for (int i = 0; i < config.getRemoteFields().length; i++) {
            String str = data.getString(config.getRemoteFields()[i]);
            if (StrUtil.isBlank(str)) {
                if (config.getDoubleIndex().contains(i)) {
                    sqlInsert.append("'0',");
                } else {
                    sqlInsert.append("'',");
                }
            } else if (JsonUtil.isScientificNotation(str)) {
                sqlInsert.append(StrUtil.format("'{}',", JsonUtil.convertScientificNotationToNormalString(str)));
            } else {
                sqlInsert.append(StrUtil.format("'{}',", str));
            }
        }

        if (ObjectUtil.isNotNull(config.getFormModeId())) {
            sqlInsert.append(modeStaticInfo);
        } else if (sqlInsert.charAt(sqlInsert.length()-1) == StrUtil.C_COMMA) {
            sqlInsert.deleteCharAt(sqlInsert.length()-1);
        }
        return sqlInsert.toString();
    }
    /**
     * 构建插入sql前缀
     *
     * @param config 配置
     * @return sql
     */
    private static String buildInsertSqlPrefix(SyncWriteDataConfig config) {
        StringBuilder sqlInsert = new StringBuilder();
        sqlInsert.append("insert into ").append(config.getLocalTable()).append(" (").append(String.join(",", config.getLocalFields()));
        if (ObjectUtil.isNotNull(config.getFormModeId())) {
            sqlInsert.append(MODE_TABLE_STATIC_FIELDS);
        }
        sqlInsert.append(") VALUES ");
        return sqlInsert.toString();
    }
    /**
     * 构建明细表插入sql前缀
     *
     * @param config 配置
     * @return sql
     */
    private static String buildInsertDetailSqlPrefix(SyncWriteDataConfig config) {
        return new StringBuilder("insert into ")
                .append(config.getLocalTable())
                .append(" (mainid,")
                .append(String.join(",", config.getLocalFields()))
                .append(") VALUES ").toString();
    }

    /**
     * 获取更新sql
     *
     * @param config 配置
     * @param data           数据体
     * @return sql
     */
    private static String buildUpdateSql(SyncWriteDataConfig config, JSONObject data) {
        if (config.getLocalFields().length > 0 && config.getLocalFields().length == config.getRemoteFields().length) {
            StringBuilder sqlUpdate = new StringBuilder();
            sqlUpdate.append("update ").append(config.getLocalTable()).append(" set ");
            for (int i = 0; i < config.getLocalFields().length; i++) {
                sqlUpdate.append(StrUtil.format("{}=", config.getLocalFields()[i]));
                if (config.getDoubleIndex().contains(i) && StrUtil.isBlank(data.getString(config.getRemoteFields()[i]))) {
                    sqlUpdate.append("'0',");
                } else {
                    sqlUpdate.append(StrUtil.format("'{}',", data.getString(config.getRemoteFields()[i])));
                }
            }
            if (ObjectUtil.isNotNull(config.getFormModeId())) {
                sqlUpdate.append(StrUtil.format("modedatamodifydatetime='{}'", DatePattern.NORM_DATETIME_FORMAT.format(DateTime.now())));
            } else if (sqlUpdate.charAt(sqlUpdate.length()-1) == StrUtil.C_COMMA) {
                sqlUpdate.deleteCharAt(sqlUpdate.length()-1);
            }
            sqlUpdate.append(StrUtil.format(" where {}='{}'", config.getLocalOnlyCheckField(), data.getString(config.getRemoteOnlyCheckField())));
            return sqlUpdate.toString();
        }

        return StrUtil.EMPTY;
    }

    /**
     * 赋权
     *
     * @param config 配置
     * @param rs RecordSet
     */
    private static void buildUfAuth(SyncWriteDataConfig config, RecordSet rs) {
        if (ObjectUtil.isNull(config.getFormModeId())) {
            return;
        }
        Integer id = getMaxId(config.getLocalTable(), rs);
        if (ObjectUtil.isNull(id)) {
            return;
        }
        ModeRightInfo moderightinfo = new ModeRightInfo();
        moderightinfo.setNewRight(true);
        moderightinfo.editModeDataShare(1, config.getFormModeId(), id);
    }
    /**
     * 批量赋权
     *
     * @param config 配置
     * @param oldMaxId 旧数据最大id
     * @param rs RecordSet
     */
    private static void buildUfAuthBatch(SyncWriteDataConfig config, int oldMaxId, RecordSet rs) {
        if (ObjectUtil.isNull(config.getFormModeId())) {
            return;
        }
        rs.execute(StrUtil.format(QUERY_NEW_ID_SQL, config.getLocalTable(), oldMaxId));
        Map<Integer, Integer> creatorMap = new HashMap<>(rs.getCounts());
        while (rs.next()) {
            creatorMap.put(rs.getInt("id"), 1);
        }
        if (MapUtil.isEmpty(creatorMap)) {
            return;
        }
        ModeRightInfo moderightinfo = new ModeRightInfo();
        moderightinfo.setNewRight(true);
        moderightinfo.editModeDataShare(creatorMap, config.getFormModeId(), creatorMap.keySet().stream().collect(Collectors.toList()));
    }

    /**
     * 获取最后一条数据的id
     *
     * @param table 表名
     * @param rs    数据库链接
     * @return id
     */
    private static Integer getMaxId(String table, RecordSet rs) {
        String sql = "select max(id)  from " + table;
        if (rs.execute(sql) && rs.next()) {
            return rs.getInt(1);
        }
        return null;
    }

    /**
     * 检查数据是否存在
     *
     * @param config 配置
     * @param checkValue 唯一性检查
     * @return isExists
     */
    private static boolean checkExists(SyncWriteDataConfig config, String checkValue, RecordSet rs) {
        rs.execute(StrUtil.format(CHECK_EXISTS_SQL,
                config.getLocalTable(),
                config.getLocalOnlyCheckField(),
                checkValue));
        return rs.next() && rs.getInt("count") > 0;
    }
    /**
     * 检查数据是否存在明细表
     *
     * @param config 配置
     * @param checkValue 唯一性检查
     * @param mainid 主表id
     * @param rs RecordSet
     * @return isExists
     */
    private static boolean checkExistsInDetail(SyncWriteDataConfig config, String checkValue, String mainid, RecordSet rs) {
        rs.execute(StrUtil.format(CHECK_EXISTS_IN_DETAIL_SQL,
                config.getLocalTable(),
                mainid,
                config.getLocalOnlyCheckField(),
                checkValue
                ));
        return rs.next() && rs.getInt("count") > 0;
    }
}
