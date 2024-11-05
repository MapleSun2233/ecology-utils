package com.weaver.util.slf;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.weaver.util.slf.entity.SyncWriteDataConfig;
import weaver.conn.RecordSet;
import weaver.conn.constant.DBConstant;
import weaver.formmode.setup.ModeRightInfo;
import weaver.general.BaseBean;

import java.util.*;


/**
 * @author slf
 * 建模工具
 */
public class ModelUtil {
    private static final String CHECK_EXISTS_SQL = "select count(id) as count from {} where {} = '{}'";
    private static final String CHECK_EXISTS_IN_DETAIL_SQL = "select count(id) as count from {} where mainid = {} and {} = '{}'";
    private static final String QUERY_NEW_ID_SQL = "select id from {} where id > {}";
    private static final String CLEAN_DETAIL_SQL = "delete from {} where mainid={}";
    private static final String[] MODE_TABLE_STATIC_FIELDS = new String[]{"formmodeid", "modedatacreater", "modedatacreatertype", "modedatacreatedate", "modedatacreatetime", "modedatamodifydatetime"};
    private static final int BATCH_NUM = 500;

    private static final BaseBean UTILS = new BaseBean();

    /**
     * 批量写入数据到建模表
     *
     * @param config   配置
     * @param data     被写入数据
     * @param isUpdate 是否更新
     */
    public static void batchWriteData(SyncWriteDataConfig config, JSONArray data, boolean isUpdate) {
        batchWriteDataHandler(config, data, 1, isUpdate, true);
    }

    /**
     * 批量写入数据到建模表
     *
     * @param config   配置
     * @param data     被写入数据
     * @param isUpdate 是否更新
     * @param isInsert 数据存在是否插入
     */
    public static void batchWriteData(SyncWriteDataConfig config, JSONArray data, boolean isUpdate, boolean isInsert) {
        batchWriteDataHandler(config, data, 1, isUpdate, isInsert);
    }

    /**
     * 批量写入数据到建模表
     *
     * @param config   配置
     * @param data     被写入数据
     * @param userId   创建者
     * @param isUpdate 是否更新
     */
    public static void batchWriteData(SyncWriteDataConfig config, JSONArray data, int userId, boolean isUpdate) {
        batchWriteDataHandler(config, data, userId, isUpdate, true);
    }

    /**
     * 批量写入数据到建模表
     *
     * @param config   配置
     * @param data     被写入数据
     * @param userId   创建者
     * @param isUpdate 是否更新
     * @param isInsert 数据不存在是否插入
     */
    public static void batchWriteData(SyncWriteDataConfig config, JSONArray data, int userId, boolean isUpdate, boolean isInsert) {
        batchWriteDataHandler(config, data, userId, isUpdate, isInsert);
    }

    /**
     * 批量写入数据到建模表处理器
     *
     * @param config   配置
     * @param data     被写入数据
     * @param userId   创建者
     * @param isUpdate 是否更新
     */
    private static void batchWriteDataHandler(SyncWriteDataConfig config, JSONArray data, int userId, boolean isUpdate, boolean isInsert) {
        UTILS.writeLog("SyncWriteDataConfig: " + config.toString());
        UTILS.writeLog(StrUtil.format("creator: {}, 开始写入数据...", userId));
        RecordSet rs = new RecordSet();
        List<Object> modeStaticInfo = Arrays.asList(config.getFormModeId(), userId, 0,
                DatePattern.NORM_DATE_FORMAT.format(DateTime.now()),
                DatePattern.NORM_TIME_FORMAT.format(DateTime.now()),
                DatePattern.NORM_DATETIME_FORMAT.format(DateTime.now()));
        try {
            // 构建插入sql
            String insertSql = buildInsertSql(config);
            UTILS.writeLog("insertSql: " + insertSql);
            List<List> params = new ArrayList<>(data.size());
            int skipCount = 0;
            int updateCount = 0;
            int oldMaxId = getMaxId(config.getLocalTable(), rs);
            UTILS.writeLog("oldMaxId: " + oldMaxId);
            for (int i = 0; i < data.size(); i++) {
                JSONObject item = data.getJSONObject(i);
                if (checkExists(config, item.getString(config.getRemoteOnlyCheckField()), rs)) {
                    if (isUpdate) {
                        rs.execute(buildUpdateSql(config, data.getJSONObject(i)));
                        updateCount++;
                    } else {
                        skipCount++;
                    }
                } else if (isInsert) {
                    params.add(buildInsertData(config, item, modeStaticInfo));
                } else {
                    skipCount++;
                }
            }
            String dbType = rs.getDBType();
            UTILS.writeLog("dbType ::: " + dbType);
            if (StrUtil.equals(DBConstant.DB_TYPE_ORACLE, dbType)) {
                singleInsert(insertSql, params, rs);
            } else {
                securityBatchInsert(insertSql, params, rs);
            }
            buildUfAuthBatch(config, oldMaxId, userId, rs);
            if (isUpdate) {
                UTILS.writeLog("更新数据" + updateCount + "条");
            } else {
                UTILS.writeLog("跳过重复数据" + skipCount + "条");
            }
            UTILS.writeLog("写入数据" + params.size() + "条，写入数据完成...");
        } catch (Exception e) {
            e.printStackTrace();
            UTILS.writeLog("写入数据错误，异常消息:" + e.getMessage());
        }
    }

    /**
     * 批量写入数据到建模表明细表，不支持Oracle数据库
     * isUpdate is true: isSkip决定更新还是跳过
     * isUpdate is false: 直接清空该mainid的明细再插入
     *
     * @param config   配置
     * @param mainId   主表id
     * @param data     被写入数据
     * @param isUpdate 是否更新
     * @param isSkip   是否跳过
     */
    public static void batchWriteDetail(SyncWriteDataConfig config, String mainId, JSONArray data, boolean isUpdate, boolean isSkip) {
        UTILS.writeLog("SyncWriteDataConfig: " + config.toString());
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
        String insertSql = buildInsertDetailSql(config);
        UTILS.writeLog("insertSql: " + insertSql);
        List<List> params = new ArrayList<>(data.size());
        try {
            int skipCount = 0;
            int updateCount = 0;
            for (int i = 0; i < data.size(); i++) {
                JSONObject item = data.getJSONObject(i);
                if (isUpdate && checkExistsInDetail(config, item.getString(config.getRemoteOnlyCheckField()), mainId, rs)) {
                    if (isSkip) {
                        skipCount++;
                    } else {
                        rs.execute(buildUpdateSql(config, item));
                        updateCount++;
                    }
                } else {
                    params.add(buildInsertDetailData(config, item, mainId));
                }
            }
            String dbType = rs.getDBType();
            UTILS.writeLog("dbType ::: " + dbType);
            if (StrUtil.equals(DBConstant.DB_TYPE_ORACLE, dbType)) {
                singleInsert(insertSql, params, rs);
            } else {
                securityBatchInsert(insertSql, params, rs);
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
     * 批量追加数据到建模表明细表，不支持Oracle数据库
     *
     * @param config   配置
     * @param mainId   主表id
     * @param data     被写入数据
     */
    public static void batchWriteDetailAppend(SyncWriteDataConfig config, String mainId, JSONArray data) {
        RecordSet rs = new RecordSet();
        UTILS.writeLog("开始写入数据...");
        String insertSql = buildInsertDetailSql(config);
        UTILS.writeLog("insertSql: " + insertSql);
        List<List> params = new ArrayList<>(data.size());
        try {
            for (int i = 0; i < data.size(); i++) {
                JSONObject item = data.getJSONObject(i);
                params.add(buildInsertDetailData(config, item, mainId));
            }
            String dbType = rs.getDBType();
            UTILS.writeLog("dbType ::: " + dbType);
            if (StrUtil.equals(DBConstant.DB_TYPE_ORACLE, dbType)) {
                singleInsert(insertSql, params, rs);
            } else {
                securityBatchInsert(insertSql, params, rs);
            }
            UTILS.writeLog("写入数据" + params.size() + "条，写入数据完成...");
        } catch (Exception e) {
            UTILS.writeLog("写入数据错误，异常消息:" + e.getMessage());
        }
    }

    /**
     * 获取浮点数字段集合
     * @param tableName
     * @return
     */
    public static Set<String> getDoubleFields(String tableName) {
        RecordSet rs = new RecordSet();
        Set<String> doubleFields = new HashSet<>();
        if (tableName.matches("\\w+_dt\\d+$")) {
            rs.executeQuery("select fieldname from workflow_billfield where fieldhtmltype = 1 and type = 3 and detailtable = ?", tableName);
        } else {
            rs.executeQuery("select b.fieldname from workflow_bill a left join workflow_billfield b on a.id = b.billid where b.fieldhtmltype = 1 and b.type = 3 and a.tablename = ?", tableName);
        }
        while (rs.next()) {
            doubleFields.add(rs.getString(1));
        }
        return doubleFields;
    }


    /**
     * 构建数据写入配置
     *
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
                .doubleFields(getDoubleFields(config.get("localTable")))
                .build();
    }

    /**
     * 构建数据写入配置
     *
     * @param config config
     * @return syncConfig
     */
    public static SyncWriteDataConfig buildWriteConfig(JSONObject config) {
        String[] emptyArr = new String[0];
        return SyncWriteDataConfig.builder()
                .remoteFields(StrUtil.isBlank(config.getString("remoteFields")) ? emptyArr : config.getString("remoteFields").split(","))
                .localTable(config.getString("localTable"))
                .localFields(StrUtil.isBlank(config.getString("localFields")) ? emptyArr : config.getString("localFields").split(","))
                .localOnlyCheckField(config.getString("localOnlyCheckField"))
                .remoteOnlyCheckField(config.getString("remoteOnlyCheckField"))
                .formModeId(NumberUtil.isInteger(config.getString("formModeId")) ? Integer.parseInt(config.getString("formModeId")) : null)
                .doubleFields(getDoubleFields(config.getString("localTable")))
                .build();
    }

    /**
     * 插入单条数据
     *
     * @param insertSql 插入语句
     * @param params    params
     * @param rs        rs
     */
    public static void singleInsert(String insertSql, List<List> params, RecordSet rs) {
        for (List itemParams : params) {
            if (!rs.executeUpdate(insertSql, itemParams)) {
                UTILS.writeLog("插入数据失败" + rs.getExceptionMsg());
                UTILS.writeLog("insertSql: " + insertSql);
                UTILS.writeLog("params json: " + JSONObject.toJSONString(itemParams));
            }
        }
    }

    /**
     * 安全的批量插入，不支持oracle
     *
     * @param insertSql 插入语句
     * @param params    params
     * @param rs        rs
     */
    public static void securityBatchInsert(String insertSql, List<List> params, RecordSet rs) {
        int batchCounts = params.size() / BATCH_NUM;
        for (int i = 0; i < batchCounts; i++) {
            List<List> subParams = params.subList(i * BATCH_NUM, i * BATCH_NUM + BATCH_NUM);
            if (!rs.executeBatchSql(insertSql, subParams)) {
                UTILS.writeLog("批量插入数据失败" + rs.getExceptionMsg());
                UTILS.writeLog("insertSql: " + insertSql);
                UTILS.writeLog("params json: " + JSONObject.toJSONString(subParams));
            }
        }
        if (params.size() % BATCH_NUM != 0) {
            List<List> subParams = params.subList(batchCounts * BATCH_NUM, params.size());
            if (!rs.executeBatchSql(insertSql, subParams)) {
                UTILS.writeLog("批量插入数据失败" + rs.getExceptionMsg());
                UTILS.writeLog("insertSql: " + insertSql);
                UTILS.writeLog("params json: " + JSONObject.toJSONString(subParams));
            }
        }
    }

    /**
     * 获取插入sql
     *
     * @param config 配置
     * @return sql
     */
    private static String buildInsertSql(SyncWriteDataConfig config) {
        String[] fields = config.getLocalFields();
        if (ObjectUtil.isNotNull(config.getFormModeId())) {
            fields = ArrayUtil.addAll(fields, MODE_TABLE_STATIC_FIELDS);
        }
        return StrUtil.format("insert into {} ({}) values ({})", config.getLocalTable(), String.join(StrUtil.COMMA, fields), String.join(StrUtil.COMMA, Collections.nCopies(fields.length, "?")));
    }

    /**
     * 构建插入数据
     *
     * @param config         配置
     * @param data           数据体
     * @param modeStaticInfo 建模表固定插入
     * @return sql
     */
    private static List<Object> buildInsertData(SyncWriteDataConfig config, JSONObject data, List<Object> modeStaticInfo) {
        List<Object> itemParams = new ArrayList<>(config.getRemoteFields().length + modeStaticInfo.size());
        injectNormalFields(config, data, itemParams);
        if (ObjectUtil.isNotNull(config.getFormModeId())) {
            itemParams.addAll(modeStaticInfo);
        }
        return itemParams;
    }

    /**
     * 构建明细插入SQL
     *
     * @param config config
     * @return sql
     */
    private static String buildInsertDetailSql(SyncWriteDataConfig config) {
        return StrUtil.format("insert into {} (mainid,{}) values ({})", config.getLocalTable(), String.join(StrUtil.COMMA, config.getLocalFields()), String.join(StrUtil.COMMA, Collections.nCopies(config.getLocalFields().length + 1, "?")));
    }

    /**
     * 构建明细表数据
     *
     * @param config config
     * @param data   data
     * @param mainId mainId
     * @return params
     */
    private static List<Object> buildInsertDetailData(SyncWriteDataConfig config, JSONObject data, String mainId) {
        List<Object> itemParams = new ArrayList<>(config.getRemoteFields().length + 1);
        itemParams.add(mainId);
        injectNormalFields(config, data, itemParams);
        return itemParams;
    }

    /**
     * 注入普通字段
     *
     * @param config     config
     * @param data       data
     * @param itemParams params
     */
    private static void injectNormalFields(SyncWriteDataConfig config, JSONObject data, List<Object> itemParams) {
        for (int i = 0; i < config.getRemoteFields().length; i++) {
            String str = data.getString(config.getRemoteFields()[i]);
            if (config.getDoubleFields().contains(config.getLocalFields()[i])) {
                // 判断是否是浮点数字段
                if (StrUtil.isBlank(str)) {
                    // 是否为空
                    itemParams.add(null);
                } else if (NumberUtil.isNumber(str)) {
                    // 是否为数字
                    itemParams.add(str);
                } else if (JsonUtil.isScientificNotation(str)) {
                    // 是否为科学计数法
                    itemParams.add(JsonUtil.convertScientificNotationToNormalString(str));
                } else {
                    // 不是数字屏蔽掉
                    itemParams.add(null);
                }
            } else {
                // 其他数据正常处理
                itemParams.add(str);
            }
        }
    }

    /**
     * 获取更新sql
     *
     * @param config 配置
     * @param data   数据体
     * @return sql
     */
    private static String buildUpdateSql(SyncWriteDataConfig config, JSONObject data) {
        if (config.getLocalFields().length > 0 && config.getLocalFields().length == config.getRemoteFields().length) {
            StringBuilder sqlUpdate = new StringBuilder();
            sqlUpdate.append("update ").append(config.getLocalTable()).append(" set ");
            for (int i = 0; i < config.getLocalFields().length; i++) {
                sqlUpdate.append(StrUtil.format("{}=", config.getLocalFields()[i]));
                String str = data.getString(config.getRemoteFields()[i]);
                if (config.getDoubleFields().contains(config.getLocalFields()[i])) {
                    // 判断是否是浮点数字段
                    if (StrUtil.isBlank(str)) {
                        // 是否为空
                        sqlUpdate.append("null,");
                    } else if (NumberUtil.isNumber(str)) {
                        // 是否为数字
                        sqlUpdate.append(StrUtil.format("'{}',", str));
                    } else if (JsonUtil.isScientificNotation(str)) {
                        // 是否为科学计数法
                        sqlUpdate.append(StrUtil.format("'{}',", JsonUtil.convertScientificNotationToNormalString(str)));
                    } else {
                        // 不是数字屏蔽掉
                        sqlUpdate.append("null,");
                    }
                } else {
                    // 其他数据正常处理
                    sqlUpdate.append(StrUtil.format("'{}',", str));
                }
            }
            if (ObjectUtil.isNotNull(config.getFormModeId())) {
                sqlUpdate.append(StrUtil.format("modedatamodifydatetime='{}'", DatePattern.NORM_DATETIME_FORMAT.format(DateTime.now())));
            } else if (sqlUpdate.charAt(sqlUpdate.length() - 1) == StrUtil.C_COMMA) {
                sqlUpdate.deleteCharAt(sqlUpdate.length() - 1);
            }
            sqlUpdate.append(StrUtil.format(" where {}='{}'", config.getLocalOnlyCheckField(), data.getString(config.getRemoteOnlyCheckField())));
            UTILS.writeLog("updateSql:" + sqlUpdate);
            return sqlUpdate.toString();
        }

        return StrUtil.EMPTY;
    }

    /**
     * 赋权
     * @param modeId
     * @param userId
     * @param dataId
     */
    public static void buildUfAuth(int modeId, int userId, int dataId) {
        ModeRightInfo moderightinfo = new ModeRightInfo();
        moderightinfo.setNewRight(true);
        moderightinfo.editModeDataShare(userId, modeId, dataId);
    }

    /**
     * 批量赋权
     *
     * @param config   配置
     * @param oldMaxId 旧数据最大id
     * @param rs       RecordSet
     */
    private static void buildUfAuthBatch(SyncWriteDataConfig config, int oldMaxId, int userId, RecordSet rs) {
        if (ObjectUtil.isNull(config.getFormModeId())) {
            return;
        }
        ModeRightInfo moderightinfo = new ModeRightInfo();
        moderightinfo.setNewRight(true);
        rs.execute(StrUtil.format(QUERY_NEW_ID_SQL, config.getLocalTable(), oldMaxId));
        if (StrUtil.equals(DBConstant.DB_TYPE_ORACLE, rs.getDBType())) {
            while (rs.next()) {
                moderightinfo.editModeDataShare(userId, config.getFormModeId(), rs.getInt("id"));
            }
        } else {
            Map<Integer, Integer> creatorMap = new HashMap<>(rs.getCounts());
            while (rs.next()) {
                creatorMap.put(rs.getInt("id"), userId);
                if (creatorMap.size() >= 500) {
                    moderightinfo.editModeDataShare(creatorMap, config.getFormModeId(), new ArrayList<>(creatorMap.keySet()));
                    creatorMap.clear();
                }
            }
            if (MapUtil.isNotEmpty(creatorMap)) {
                moderightinfo.editModeDataShare(creatorMap, config.getFormModeId(), new ArrayList<>(creatorMap.keySet()));
            }
        }
    }

    /**
     * 获取最后一条数据的id
     *
     * @param table 表名
     * @param rs    数据库链接
     * @return id
     */
    private static int getMaxId(String table, RecordSet rs) {
        String sql = "select max(id)  from " + table;
        if (rs.execute(sql) && rs.next()) {
            return rs.getInt(1);
        }
        return 0;
    }

    /**
     * 检查数据是否存在
     *
     * @param config     配置
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
     * @param config     配置
     * @param checkValue 唯一性检查
     * @param mainid     主表id
     * @param rs         RecordSet
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

    /**
     * 获取主数据id
     * @param tableName
     * @param onlyCheckField
     * @param onlyCheckValue
     * @return mainId
     */
    public static int getMainId(String tableName, String onlyCheckField, String onlyCheckValue) {
        String querySql = StrUtil.format("select id from {} where {} = ?", tableName, onlyCheckField);
        UTILS.writeLog(StrUtil.format("getMainIdSql: {} onlyCheckValue: {}", querySql, onlyCheckValue));
        RecordSet rs = new RecordSet();
        if (rs.executeQuery(querySql, onlyCheckValue) && rs.next()) {
            return rs.getInt(1);
        }
        return -1;
    }
}
