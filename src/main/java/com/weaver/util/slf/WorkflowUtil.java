package com.weaver.util.slf;

import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HtmlUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.engine.common.util.ServiceUtil;
import com.engine.workflow.constant.PAResponseCode;
import com.engine.workflow.entity.publicApi.PAResponseEntity;
import com.engine.workflow.entity.publicApi.ReqOperateRequestEntity;
import com.engine.workflow.entity.publicApi.WorkflowDetailTableInfoEntity;
import com.engine.workflow.publicApi.WorkflowRequestOperatePA;
import com.engine.workflow.publicApi.impl.WorkflowRequestOperatePAImpl;
import weaver.conn.RecordSet;
import weaver.general.BaseBean;
import weaver.hrm.User;
import weaver.soa.workflow.request.DetailTable;
import weaver.soa.workflow.request.Property;
import weaver.soa.workflow.request.RequestInfo;
import weaver.workflow.request.RequestManager;
import weaver.workflow.webservices.WorkflowRequestTableField;
import weaver.workflow.webservices.WorkflowRequestTableRecord;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author slf
 * @date 2023/7/14
 * 流程工具
 */
public class WorkflowUtil {
    private static final BaseBean UTILS = new BaseBean();

    /**
     * 提交流程的指定节点至下一个节点
     *
     * @param requestId requestId
     * @param nodeId    nodeId 流转指定节点，防止无差别提交
     * @param fields    字段赋值列表
     * @param remark    签字意见
     * @return boolean 操作结果
     */
    @Deprecated
    public static boolean submitWorkflow(int requestId, int nodeId, List<WorkflowRequestTableField> fields, String remark) {
        RecordSet rs = new RecordSet();
        WorkflowRequestOperatePA operatePa = ServiceUtil.getService(WorkflowRequestOperatePAImpl.class);
        if (ObjectUtil.isNull(operatePa)) {
            UTILS.writeLog("WorkflowRequestOperatePA获取失败");
            return false;
        }
        while (rs.executeQuery("select userid from workflow_currentoperator a left join workflow_nodebase b on a.nodeid = b.id where a.requestid = ? and a.nodeid = ? and isremark = 0", requestId, nodeId) && rs.next()) {
            User user = User.getUser(rs.getInt("userid"), 0);
            ReqOperateRequestEntity entity = new ReqOperateRequestEntity();
            entity.setRequestId(requestId);
            entity.setUserId(user.getUID());
            entity.setClientIp("0:0:0:0:0:0:0:1");
            entity.setMainData(fields);
            entity.setRemark(remark.replaceAll("\"", "\\\"").replaceAll("'", "\\'").replaceAll("\\?", StrUtil.EMPTY));
            PAResponseEntity entityResult = operatePa.submitRequest(user, entity);
            if (entityResult.getCode().getStatusCode() != 1) {
                UTILS.writeLog(entityResult.getCode().getMessage());
                return false;
            }
        }
        return true;
    }

    /**
     * 创建流程， 该用户必须具有流程创建权限
     *
     * @param workflowId workflowId
     * @param title      标题
     * @param user       用户
     * @param fields     字段赋值列表
     * @return jsonObject
     */
    @Deprecated
    public static JSONObject createWorkflow(int workflowId, String title, User user, List<WorkflowRequestTableField> fields) {
        JSONObject res = new JSONObject();
        res.put("status", true);
        WorkflowRequestOperatePA operatePa = ServiceUtil.getService(WorkflowRequestOperatePAImpl.class);
        if (operatePa != null) {
            ReqOperateRequestEntity entity = new ReqOperateRequestEntity();
            entity.setWorkflowId(workflowId);
            entity.setRequestName(title);
            entity.setUserId(user.getUID());
            entity.setClientIp("0:0:0:0:0:0:0:1");
            entity.setMainData(fields);
            PAResponseEntity entityResult = operatePa.doCreateRequest(user, entity);
            res.put("status", entityResult.getCode().getStatusCode() == 1);
            res.put("msg", entityResult.getCode().getMessage());
        } else {
            res.put("status", false);
            res.put("msg", "WorkflowRequestOperatePA获取失败");
        }
        return res;
    }

    /**
     * 删除流程
     *
     * @param userId    用户id
     * @param requestId 流程id
     */
    @Deprecated
    public static void deleteRequest(String userId, int requestId) {
        Map<String, Integer> nowNodeInfo = getNowNodeInfoByRequestId(requestId);
        WorkflowRequestOperatePA operatePa = ServiceUtil.getService(WorkflowRequestOperatePAImpl.class);
        ValidatorUtil.builder()
                .append(nowNodeInfo, Map::isEmpty, StrUtil.format("删除失败，流程{}不存在", requestId))
                .append(nowNodeInfo.get("nowNodeType"), s -> s != 0, StrUtil.format("删除失败，流程{}处于非创建节点", requestId))
                .append(operatePa, ObjectUtil::isNull, "WorkflowRequestOperatePA获取失败")
                .validate();
        User user = null;
        if (StrUtil.isNotBlank(userId) && NumberUtil.isInteger(userId)) {
            user = User.getUser(NumberUtil.parseInt(userId), 0);
        } else {
            user = User.getUser(1, 0);
        }
        ReqOperateRequestEntity entity = new ReqOperateRequestEntity();
        entity.setRequestId(requestId);
        PAResponseEntity responseEntity = operatePa.deleteRequest(user, entity);
        if (!responseEntity.getCode().equals(PAResponseCode.SUCCESS)) {
            throw new RuntimeException(StrUtil.format("删除失败，流程{}操作者无权删除", requestId));
        }
        UTILS.writeLog("delete request ::: " + requestId);
    }

    /**
     * 删除流程
     *
     * @param deleteEntity 参数实体
     */
    public static void deleteRequest(JSONObject deleteEntity) {
        WorkflowRequestOperatePA operatePa = ServiceUtil.getService(WorkflowRequestOperatePAImpl.class);
        ValidatorUtil.validate(deleteEntity.getString("requestId"), t -> StrUtil.isBlank(t) || !NumberUtil.isNumber(t), "requestId参数异常");
        int requestId = deleteEntity.getInteger("requestId");
        Map<String, Integer> nowNodeInfo = getNowNodeInfoByRequestId(requestId);
        ValidatorUtil.builder()
                .append(nowNodeInfo, Map::isEmpty, StrUtil.format("删除失败，流程{}不存在", requestId))
                .append(nowNodeInfo.get("nowNodeType"), s -> s == 3, StrUtil.format("删除失败，流程{}已归档", requestId))
                .append(operatePa, ObjectUtil::isNull, "WorkflowRequestOperatePA获取失败")
                .validate();
        String userId = deleteEntity.getString("operator");
        boolean isWorkCode = Optional.ofNullable(deleteEntity.getBoolean("operatorIsWorkCode")).orElse(false);
        User user = HrmUtil.getUserCompatibleWorkCode(userId, isWorkCode);
        ReqOperateRequestEntity entity = new ReqOperateRequestEntity();
        entity.setRequestId(requestId);
        PAResponseEntity responseEntity;
        if (nowNodeInfo.get("nowNodeType") != 0) {
            boolean allowDelete = Optional.ofNullable(deleteEntity.getBoolean("allowDelete")).orElse(false);
            if (allowDelete) {
                responseEntity = operatePa.withdrawRequest(user, entity);
                if (!responseEntity.getCode().equals(PAResponseCode.SUCCESS)) {
                    handleResponseEntityForFailure(responseEntity, "撤销审批中流程失败");
                }
            } else {
                throw new RuntimeException("流程删除失败，当前流程已处于审批中禁止删除！");
            }
        }
        responseEntity = operatePa.deleteRequest(user, entity);
        if (!responseEntity.getCode().equals(PAResponseCode.SUCCESS)) {
            handleResponseEntityForFailure(responseEntity, "删除流程失败，请检查操作者是否是流程创建者");
        }
        UTILS.writeLog("delete request ::: " + requestId);
    }

    /**
     * 创建流程， 该用户必须具有流程创建权限
     *
     * @param createEntity createEntity
     * @return PAResponseEntity result
     */
    public static int createWorkflow(JSONObject createEntity) {
        ValidatorUtil.validate(createEntity, ObjectUtil::isNull, "创建数据实体不能为空");
        WorkflowRequestOperatePA operatePa = ServiceUtil.getService(WorkflowRequestOperatePAImpl.class);
        ValidatorUtil.validate(operatePa, ObjectUtil::isNull, "WorkflowRequestOperatePA获取失败");
        int workflowId = createEntity.getInteger("workflowId");
        String tableName = getTableNameByWorkflowId(workflowId);
        String userId = createEntity.getString("creator");
        boolean isWorkCode = Optional.ofNullable(createEntity.getBoolean("creatorIsWorkCode")).orElse(false);
        String title = createEntity.getString("title");
        ValidatorUtil.builder()
                .append(tableName, StrUtil::isBlank, "流程数据表获取失败")
                .append(title, StrUtil::isBlank, "流程标题不能为空")
                .validate();
        User user = HrmUtil.getUserCompatibleWorkCode(userId, isWorkCode);
        ReqOperateRequestEntity entity = new ReqOperateRequestEntity();
        entity.setWorkflowId(workflowId);
        entity.setRequestName(title);
        entity.setUserId(user.getUID());
        entity.setClientIp("0:0:0:0:0:0:0:1");
        JSONObject mainData = createEntity.getJSONObject("mainData");
        if (ObjectUtil.isNotNull(mainData)) {
            entity.setMainData(buildMainData(mainData));
        }
        JSONArray detailData = createEntity.getJSONArray("detailData");
        if (ObjectUtil.isNotNull(detailData)) {
            entity.setDetailData(buildDetailData(detailData, tableName));
        }
        if (createEntity.containsKey("isSubmit") && createEntity.getBoolean("isSubmit")) {
            String remark = createEntity.getString("remark");
            if (StrUtil.isNotBlank(remark)) {
                entity.setRemark(remark.replaceAll("\"", "\\\"").replaceAll("'", "\\'").replaceAll("\\?", StrUtil.EMPTY));
            }
        } else {
            entity.setOtherParams(MapUtil.builder(new HashMap<String, Object>()).put("isnextflow", "0").build());
        }
        UTILS.writeLog("createWorkflowEntity ::: " + JSONObject.toJSONString(entity));
        PAResponseEntity responseEntity = operatePa.doCreateRequest(user, entity);
        UTILS.writeLog("responseEntity :: " + JSONObject.toJSONString(responseEntity));
        if (!responseEntity.getCode().equals(PAResponseCode.SUCCESS)) {
            handleResponseEntityForFailure(responseEntity, "流程创建/提交失败，请检查参数信息");
        }
        return JSONObject.parseObject(JSONObject.toJSONString(responseEntity.getData())).getInteger("requestid");
    }

    /**
     * 更新并提交流程， 该用户必须具有流程创建权限
     *
     * @param submitEntity submitEntity
     * @return PAResponseEntity result
     */
    public static int submitWorkflow(JSONObject submitEntity) {
        ValidatorUtil.validate(submitEntity, ObjectUtil::isNull, "提交数据实体不能为空");
        WorkflowRequestOperatePA operatePa = ServiceUtil.getService(WorkflowRequestOperatePAImpl.class);
        ValidatorUtil.validate(operatePa, ObjectUtil::isNull, "WorkflowRequestOperatePA获取失败");
        int requestId = submitEntity.getInteger("requestId");
        String tableName = getTableNameByRequestId(requestId);
        String userId = submitEntity.getString("operator");
        boolean isWorkCode = Optional.ofNullable(submitEntity.getBoolean("operatorIsWorkCode")).orElse(false);
        ValidatorUtil.validate(tableName, StrUtil::isBlank, "流程数据表获取失败");
        User user = HrmUtil.getUserCompatibleWorkCode(userId, isWorkCode);
        ReqOperateRequestEntity entity = new ReqOperateRequestEntity();
        entity.setRequestId(requestId);
        entity.setUserId(user.getUID());
        entity.setClientIp("0:0:0:0:0:0:0:1");
        JSONObject mainData = submitEntity.getJSONObject("mainData");
        if (ObjectUtil.isNotNull(mainData)) {
            entity.setMainData(buildMainData(mainData));
        }
        JSONArray detailData = submitEntity.getJSONArray("detailData");
        if (ObjectUtil.isNotNull(detailData)) {
            entity.setDetailData(buildDetailData(detailData, tableName));
        }
        if (submitEntity.containsKey("isSubmit") && submitEntity.getBoolean("isSubmit")) {
            String remark = submitEntity.getString("remark");
            if (StrUtil.isNotBlank(remark)) {
                entity.setRemark(remark.replaceAll("\"", "\\\"").replaceAll("'", "\\'").replaceAll("\\?", StrUtil.EMPTY));
            }
        } else {
            entity.setOtherParams(MapUtil.builder(new HashMap<String, Object>()).put("src", "save").build());
        }
        UTILS.writeLog("submitWorkflowEntity ::: " + JSONObject.toJSONString(entity));
        PAResponseEntity responseEntity = operatePa.submitRequest(user, entity);
        UTILS.writeLog("responseEntity :: " + JSONObject.toJSONString(responseEntity));
        if (!responseEntity.getCode().equals(PAResponseCode.SUCCESS)) {
            handleResponseEntityForFailure(responseEntity, "流程提交失败，请检查参数信息");
        }
        return requestId;
    }
    /**
     * 创建流程， 该用户必须具有流程创建权限
     *
     * @param createEntity createEntity
     * @param dataConvertStrategy 数据转换策略
     * @return PAResponseEntity result
     */
    public static int createWorkflowV2(JSONObject createEntity, Map<String, String> dataConvertStrategy) {
        ValidatorUtil.validate(createEntity, ObjectUtil::isNull, "创建数据实体不能为空");
        WorkflowRequestOperatePA operatePa = ServiceUtil.getService(WorkflowRequestOperatePAImpl.class);
        ValidatorUtil.validate(operatePa, ObjectUtil::isNull, "WorkflowRequestOperatePA获取失败");
        int workflowId = createEntity.getInteger("workflowId");
        String tableName = getTableNameByWorkflowId(workflowId);
        String userId = createEntity.getString("operator");
        String userIdConvertStrategy = createEntity.getString("operatorConvertStrategy");
        String title = createEntity.getString("title");
        ValidatorUtil.builder()
                .append(userId, StrUtil::isBlank, "操作者operator不能为空")
                .append(userIdConvertStrategy, StrUtil::isBlank, "操作者数据转换策略operatorConvertStrategy不能为空")
                .append(tableName, StrUtil::isBlank, "流程数据表获取失败")
                .append(title, StrUtil::isBlank, "流程标题不能为空")
                .validate();
        userId = DataConvertUtil.convertDataByStrategySql(dataConvertStrategy, userId, userIdConvertStrategy);
        UTILS.writeLog("converted operatorId: " + userId);
        User user = User.getUser(NumberUtil.parseInt(userId), 0);
        ReqOperateRequestEntity entity = new ReqOperateRequestEntity();
        entity.setWorkflowId(workflowId);
        entity.setRequestName(title);
        entity.setUserId(user.getUID());
        entity.setClientIp("0:0:0:0:0:0:0:1");
        JSONObject mainData = createEntity.getJSONObject("mainData");
        if (ObjectUtil.isNotNull(mainData)) {
            entity.setMainData(buildMainData(mainData, dataConvertStrategy));
        }
        JSONArray detailData = createEntity.getJSONArray("detailData");
        if (ObjectUtil.isNotNull(detailData)) {
            entity.setDetailData(buildDetailData(detailData, tableName, dataConvertStrategy));
        }
        if (createEntity.containsKey("isSubmit") && !createEntity.getBoolean("isSubmit")) {
            entity.setOtherParams(MapUtil.builder(new HashMap<String, Object>()).put("isnextflow", "0").build());
        } else {
            // 默认提交
            String remark = createEntity.getString("remark");
            if (StrUtil.isNotBlank(remark)) {
                entity.setRemark(remark.replaceAll("\"", "\\\"").replaceAll("'", "\\'").replaceAll("\\?", StrUtil.EMPTY));
            }
        }
        UTILS.writeLog("createWorkflowEntity ::: " + JSONObject.toJSONString(entity));
        PAResponseEntity responseEntity = operatePa.doCreateRequest(user, entity);
        UTILS.writeLog("responseEntity :: " + JSONObject.toJSONString(responseEntity));
        if (!responseEntity.getCode().equals(PAResponseCode.SUCCESS)) {
            handleResponseEntityForFailure(responseEntity, "流程创建/提交失败，请检查参数信息");
        }
        return JSONObject.parseObject(JSONObject.toJSONString(responseEntity.getData())).getInteger("requestid");
    }
    /**
     * 更新并提交流程， 该用户必须具有流程创建权限，支持自定义数据转换策略
     *
     * @param submitEntity submitEntity
     * @param dataConvertStrategy 数据转换策略
     * @return PAResponseEntity result
     */
    public static int submitWorkflowV2(JSONObject submitEntity, Map<String, String> dataConvertStrategy) {
        ValidatorUtil.validate(submitEntity, ObjectUtil::isNull, "提交数据实体不能为空");
        WorkflowRequestOperatePA operatePa = ServiceUtil.getService(WorkflowRequestOperatePAImpl.class);
        ValidatorUtil.validate(operatePa, ObjectUtil::isNull, "WorkflowRequestOperatePA获取失败");
        int requestId = submitEntity.getIntValue("requestId");
        String tableName = getTableNameByRequestId(requestId);
        String userId = submitEntity.getString("operator");
        String userIdConvertStrategy = submitEntity.getString("operatorConvertStrategy");
        ValidatorUtil.builder()
                .append(tableName, StrUtil::isBlank, "流程数据表获取失败")
                .append(userId, StrUtil::isBlank, "操作者operator不能为空")
                .append(userIdConvertStrategy, StrUtil::isBlank, "操作者数据转换策略operatorConvertStrategy不能为空")
                .validate();
        userId = DataConvertUtil.convertDataByStrategySql(dataConvertStrategy, userId, userIdConvertStrategy);
        UTILS.writeLog("converted operatorId: " + userId);
        User user = User.getUser(NumberUtil.parseInt(userId), 0);
        ReqOperateRequestEntity entity = new ReqOperateRequestEntity();
        entity.setRequestId(requestId);
        entity.setUserId(user.getUID());
        entity.setClientIp("0:0:0:0:0:0:0:1");
        JSONObject mainData = submitEntity.getJSONObject("mainData");
        if (ObjectUtil.isNotNull(mainData)) {
            entity.setMainData(buildMainData(mainData, dataConvertStrategy));
        }
        JSONArray detailData = submitEntity.getJSONArray("detailData");
        if (ObjectUtil.isNotNull(detailData)) {
            entity.setDetailData(buildDetailData(detailData, tableName, dataConvertStrategy));
        }
        if (submitEntity.containsKey("isSubmit") && !submitEntity.getBoolean("isSubmit")) {
            entity.setOtherParams(MapUtil.builder(new HashMap<String, Object>()).put("src", "save").build());
        } else {
            // 默认提交
            String remark = submitEntity.getString("remark");
            if (StrUtil.isNotBlank(remark)) {
                entity.setRemark(remark.replaceAll("\"", "\\\"").replaceAll("'", "\\'").replaceAll("\\?", StrUtil.EMPTY));
            }
        }
        UTILS.writeLog("submitWorkflowEntity ::: " + JSONObject.toJSONString(entity));
        PAResponseEntity responseEntity = operatePa.submitRequest(user, entity);
        UTILS.writeLog("responseEntity :: " + JSONObject.toJSONString(responseEntity));
        if (!responseEntity.getCode().equals(PAResponseCode.SUCCESS)) {
            handleResponseEntityForFailure(responseEntity, "流程提交失败，请检查参数信息");
        }
        return requestId;
    }

    /**
     * 处理流程操作错误信息
     *
     * @param responseEntity 错误返回体
     * @param additionalMsg  附加消息
     */
    private static void handleResponseEntityForFailure(PAResponseEntity responseEntity, String additionalMsg) {
        StringBuilder errMsg = new StringBuilder();
        switch (responseEntity.getCode()) {
            case PARAM_ERROR:
                errMsg.append("参数错误，");
                break;
            case NO_PERMISSION:
                errMsg.append("权限不足，");
                break;
            case SYSTEM_INNER_ERROR:
                errMsg.append("系统错误，");
                break;
            default:
        }
        try {
            if (!responseEntity.getErrMsg().isEmpty()) {
                errMsg.append(JSONObject.toJSONString(responseEntity.getErrMsg()));
            }
            if (ObjectUtil.isNotNull(responseEntity.getReqFailMsg().getMsgInfo().get("detail"))) {
                errMsg.append(JSONObject.toJSONString(responseEntity.getReqFailMsg().getMsgInfo().get("detail")));
            }
        } catch (Exception e) {
            UTILS.writeLog(e.getMessage());
            UTILS.writeLog(JSONObject.toJSONString(responseEntity));
        }
        throw new RuntimeException(HtmlUtil.cleanHtmlTag(errMsg.toString()) + additionalMsg);
    }

    /**
     * 构建主表数据
     *
     * @param mainData 主表数据
     * @return main
     */
    public static List<WorkflowRequestTableField> buildMainData(JSONObject mainData) {
        return Arrays.asList(buildFields(mainData.getJSONArray("fields"), mainData.getJSONObject("convertFields")));
    }

    /**
     * 构建明细表
     *
     * @param detailsData 明细表数据
     * @param tableName   主数据表名
     * @return tables
     */
    public static List<WorkflowDetailTableInfoEntity> buildDetailData(JSONArray detailsData, String tableName) {
        List<WorkflowDetailTableInfoEntity> detailData = new ArrayList<>(detailsData.size());
        for (int i = 0; i < detailsData.size(); i++) {
            JSONObject detailTableData = detailsData.getJSONObject(i);
            ValidatorUtil.validate(detailTableData.getString("detailTableName"), StrUtil::isBlank, "明细表名不能为空");
            JSONObject convertFields = detailTableData.getJSONObject("convertFields");
            WorkflowDetailTableInfoEntity entity = new WorkflowDetailTableInfoEntity();
            entity.setTableDBName(tableName + "_" + detailTableData.getString("detailTableName"));
            entity.setWorkflowRequestTableRecords(buildLines(detailTableData.getJSONArray("lines"), convertFields));
            detailData.add(entity);
        }
        return detailData;
    }

    /**
     * 构建明细行
     *
     * @param linesData     行数据
     * @param convertFields 转换字段
     * @return lines
     */
    private static WorkflowRequestTableRecord[] buildLines(JSONArray linesData, JSONObject convertFields) {
        int linesLength = linesData.size();
        WorkflowRequestTableRecord[] workflowRequestTableRecords = new WorkflowRequestTableRecord[linesLength];
        for (int j = 0; j < linesLength; j++) {
            JSONArray fieldsData = linesData.getJSONArray(j);
            WorkflowRequestTableRecord requestTableRecord = new WorkflowRequestTableRecord();
            requestTableRecord.setRecordOrder(0);
            requestTableRecord.setWorkflowRequestTableFields(buildFields(fieldsData, convertFields));
            workflowRequestTableRecords[j] = requestTableRecord;
        }
        return workflowRequestTableRecords;
    }

    /**
     * 构建字段
     *
     * @param fieldsData    字段数据
     * @param convertFields 转换字段
     * @return fields
     */
    private static WorkflowRequestTableField[] buildFields(JSONArray fieldsData, JSONObject convertFields) {
        handleConvertFields(fieldsData, convertFields);
        int fieldsLength = fieldsData.size();
        WorkflowRequestTableField[] fields = new WorkflowRequestTableField[fieldsLength];
        for (int k = 0; k < fieldsLength; k++) {
            JSONObject item = fieldsData.getJSONObject(k);
            WorkflowRequestTableField itemField = new WorkflowRequestTableField();
            itemField.setFieldName(item.getString("fieldName"));
            itemField.setFieldValue(item.getString("fieldValue"));
            fields[k] = itemField;
        }
        return fields;
    }

    /**
     * 处理字段转换
     *
     * @param fields        字段值
     * @param convertFields 转换字段配置
     */
    public static void handleConvertFields(JSONArray fields, JSONObject convertFields) {
        if (ObjectUtil.isNull(convertFields)) {
            return;
        }
        for (int i = 0; i < fields.size(); i++) {
            JSONObject item = fields.getJSONObject(i);
            String fieldName = item.getString("fieldName");
            String fieldValue = item.getString("fieldValue");
            if (convertFields.containsKey(fieldName)) {
                switch (convertFields.getString(fieldName)) {
                    case "employee":
                        item.put("fieldValue", HrmUtil.convertWorkCodesToUserIds(fieldValue));
                        break;
                    case "department":
                        item.put("fieldValue", HrmUtil.convertDepartmentCodesToDepartmentIds(fieldValue));
                        break;
                    case "company":
                        item.put("fieldValue", HrmUtil.convertCompanyCodesToCompanyIds(fieldValue));
                        break;
                    case "fileBytes":
                        item.put("fieldValue", convertJsonFileList(fieldValue));
                    default:
                }
            }
        }
    }
    /**
     * 构建主表数据，支持自定义数据转换策略
     *
     * @param mainData 主表数据
     * @param dataConvertStrategy 数据转换策略
     * @return main
     */
    public static List<WorkflowRequestTableField> buildMainData(JSONObject mainData, Map<String, String> dataConvertStrategy) {
        return Arrays.asList(buildFields(mainData.getJSONArray("fields"), mainData.getJSONObject("convertFields"), dataConvertStrategy));
    }
    /**
     * 构建明细表，支持自定义数据转换策略
     *
     * @param detailsData 明细表数据
     * @param tableName   主数据表名
     * @return tables
     */
    public static List<WorkflowDetailTableInfoEntity> buildDetailData(JSONArray detailsData, String tableName, Map<String, String> dataConvertStrategy) {
        List<WorkflowDetailTableInfoEntity> detailData = new ArrayList<>(detailsData.size());
        for (int i = 0; i < detailsData.size(); i++) {
            JSONObject detailTableData = detailsData.getJSONObject(i);
            ValidatorUtil.validate(detailTableData.getString("detailTableName"), StrUtil::isBlank, "明细表名不能为空");
            JSONObject convertFields = detailTableData.getJSONObject("convertFields");
            WorkflowDetailTableInfoEntity entity = new WorkflowDetailTableInfoEntity();
            entity.setTableDBName(tableName + "_" + detailTableData.getString("detailTableName"));
            entity.setWorkflowRequestTableRecords(buildLines(detailTableData.getJSONArray("lines"), convertFields, dataConvertStrategy));
            detailData.add(entity);
        }
        return detailData;
    }
    /**
     * 构建明细行，支持自定义数据转换
     *
     * @param linesData     行数据
     * @param convertFields 转换字段
     * @param dataConvertStrategy 数据转换策略
     * @return lines
     */
    private static WorkflowRequestTableRecord[] buildLines(JSONArray linesData, JSONObject convertFields, Map<String, String> dataConvertStrategy) {
        int linesLength = linesData.size();
        WorkflowRequestTableRecord[] workflowRequestTableRecords = new WorkflowRequestTableRecord[linesLength];
        for (int j = 0; j < linesLength; j++) {
            JSONArray fieldsData = linesData.getJSONArray(j);
            WorkflowRequestTableRecord requestTableRecord = new WorkflowRequestTableRecord();
            requestTableRecord.setRecordOrder(0);
            requestTableRecord.setWorkflowRequestTableFields(buildFields(fieldsData, convertFields, dataConvertStrategy));
            workflowRequestTableRecords[j] = requestTableRecord;
        }
        return workflowRequestTableRecords;
    }
    /**
     * 构建字段，支持自定义数据转换
     *
     * @param fieldsData    字段数据
     * @param convertFields 转换字段
     * @param dataConvertStrategy 数据转换策略
     * @return fields
     */
    private static WorkflowRequestTableField[] buildFields(JSONArray fieldsData, JSONObject convertFields, Map<String, String> dataConvertStrategy) {
        handleConvertFields(fieldsData, convertFields, dataConvertStrategy);
        int fieldsLength = fieldsData.size();
        WorkflowRequestTableField[] fields = new WorkflowRequestTableField[fieldsLength];
        for (int k = 0; k < fieldsLength; k++) {
            JSONObject item = fieldsData.getJSONObject(k);
            WorkflowRequestTableField itemField = new WorkflowRequestTableField();
            itemField.setFieldName(item.getString("fieldName"));
            itemField.setFieldValue(item.getString("fieldValue"));
            fields[k] = itemField;
        }
        return fields;
    }
    /**
     * 处理字段转换，支持自定义数据转换策略
     *
     * @param fields        字段值
     * @param convertFields 转换字段配置
     * @param dataConvertStrategy 数据转换策略
     */
    public static void handleConvertFields(JSONArray fields, JSONObject convertFields, Map<String, String> dataConvertStrategy) {
        if (ObjectUtil.isNull(convertFields)) {
            return;
        }
        for (int i = 0; i < fields.size(); i++) {
            JSONObject item = fields.getJSONObject(i);
            String fieldName = item.getString("fieldName");
            if (convertFields.containsKey(fieldName)) {
                String strategyName = convertFields.getString(fieldName);
                item.put("fieldValue", DataConvertUtil.convertDataByStrategySql(dataConvertStrategy, item.getString("fieldValue"), strategyName));
            }
        }
    }

    /**
     * 用于解决通用流程创建附件上传无法直接传递base64的问题，使用字节数组转换
     *
     * @param fieldValue fieldValue
     * @return fieldValue
     */
    private static String convertJsonFileList(String fieldValue) {
        JSONArray list = JSONArray.parseArray(fieldValue);
        for (int i = 0; i < list.size(); i++) {
            JSONObject item = list.getJSONObject(i);
            String base64 = "base64:" + DocUtil.bytesToBase64(JsonUtil.jsonArrayToByte(item.getJSONArray("filePath")));
            item.put("filePath", base64);
        }
        return list.toJSONString();
    }

    /**
     * 强制归档，该流程所处的当前节点必须开启了强制归档，否则无权限
     *
     * @param requestId requestId
     * @return jsonObject
     */
    public static JSONObject forceArchive(int requestId) {
        RecordSet rs = new RecordSet();
        rs.executeQuery("select userid from workflow_currentoperator where requestid=? and isremark=0", requestId);
        JSONObject res = new JSONObject();
        res.put("status", true);
        if (rs.next()) {
            User user = User.getUser(rs.getInt("userid"), 0);
            WorkflowRequestOperatePA operatePa = ServiceUtil.getService(WorkflowRequestOperatePAImpl.class);
            if (operatePa != null) {
                ReqOperateRequestEntity entity = new ReqOperateRequestEntity();
                entity.setRequestId(requestId);
                entity.setUserId(user.getUID());
                entity.setClientIp("0:0:0:0:0:0:0:1");
                PAResponseEntity entityResult = operatePa.doForceOver(user, entity);
                res.put("status", entityResult.getCode().getStatusCode() == 1);
                res.put("msg", entityResult.getCode().getMessage());
            } else {
                res.put("status", false);
                res.put("msg", "WorkflowRequestOperatePA获取失败");
            }
        } else {
            res.put("status", false);
            res.put("msg", "未找到当前流程操作者");
        }
        return res;
    }

    /**
     * 获取主表数据
     *
     * @param requestInfo 节点请求信息
     * @return map
     */
    public static Map<String, String> getMainTableData(RequestInfo requestInfo) {
        Map<String, String> result = new HashMap<>(requestInfo.getMainTableInfo().getPropertyCount());
        for (Property property : requestInfo.getMainTableInfo().getProperty()) {
            result.put(property.getName(), property.getValue());
        }
        return result;
    }

    /**
     * 获取流程基本信息
     *
     * @param requestInfo 节点请求信息
     * @return map
     */
    public static Map<String, String> getBaseInfo(RequestInfo requestInfo) {
        RequestManager requestManager = requestInfo.getRequestManager();
        String headline = StrUtil.EMPTY;
        RecordSet rs = new RecordSet();
        rs.execute("select requestname  from workflow_requestbase where requestid = " + requestManager.getRequestid());
        if (rs.next()) {
            headline = rs.getString("requestname");
        }
        return MapUtil.builder(new HashMap<String, String>(8))
                .put("mainId", String.valueOf(requestManager.getBillid()))
                .put("workflowId", requestInfo.getWorkflowid())
                .put("requestId", requestInfo.getRequestid())
                .put("currentNodeId", String.valueOf(requestManager.getNodeid()))
                .put("nextNodeId", String.valueOf(requestManager.getNextNodeid()))
                .put("formId", String.valueOf(requestManager.getFormid()))
                .put("tableName", requestManager.getBillTableName())
                .put("headline", headline)
                .build();
    }

    /**
     * 获取明细表所有行数据
     *
     * @param detailTable 明细表
     * @return list
     */
    public static List<Map<String, String>> getDetailTableData(DetailTable detailTable) {
        return Arrays.stream(detailTable.getRow())
                .map(row -> {
                    Map<String, String> item = new HashMap<>(row.getCellCount() + 1);
                    item.put("id", row.getId());
                    Arrays.stream(row.getCell())
                            .forEach(cell -> item.put(cell.getName(), cell.getValue()));
                    return item;
                })
                .collect(Collectors.toList());
    }

    public static boolean isReject(RequestInfo request) {
        return StrUtil.equals("reject", request.getRequestManager().getSrc());
    }

    /**
     * 获取停留在指定节点的流程id
     *
     * @param nodeId 节点id
     * @return 流程id列表
     */
    public static List<Integer> getRequestIdListByCurrentNodeId(int nodeId) {
        RecordSet rs = new RecordSet();
        rs.executeQuery("select requestid from workflow_requestbase where currentnodeid = ?", nodeId);
        List<Integer> result = new ArrayList<>(rs.getCounts());
        while (rs.next()) {
            result.add(rs.getInt("requestid"));
        }
        return result;
    }

    /**
     * 根据requestId获取流程的基本信息
     *
     * @param requestId requestId
     * @return map
     */
    public static Map<String, String> getBaseInfoByRequestId(int requestId) {
        RecordSet rs = new RecordSet();
        if (rs.executeQuery("select a.requestid as requestId, a.currentnodeid as currentNodeId, b.id as workflowId, c.id as formId, c.tablename as tableName from (select requestid, workflowid, currentnodeid from workflow_requestbase where requestid = ?) a left join workflow_base b on a.workflowid = b.id left join workflow_bill c on b.formid = c.id", requestId) && rs.next()) {
            return MapUtil.builder(new HashMap<String, String>(6))
                    .put("requestId", rs.getString("requestId"))
                    .put("workflowId", rs.getString("workflowId"))
                    .put("formId", rs.getString("formId"))
                    .put("tableName", rs.getString("tableName"))
                    .put("currentNodeId", rs.getString("currentNodeId"))
                    .put("mainId", String.valueOf(getMainIdBaseRequestIdAndTableName(rs.getString("requestId"), rs.getString("tableName"), rs)))
                    .build();
        }
        return MapUtil.newHashMap();
    }

    /**
     * 获取数据mainId
     *
     * @param requestId requestId
     * @param tableName tableName
     * @param rs        rs
     * @return mainId
     */
    public static int getMainIdBaseRequestIdAndTableName(String requestId, String tableName, RecordSet rs) {
        if (ObjectUtil.isNull(rs)) {
            return getMainIdBaseRequestIdAndTableName(requestId, tableName);
        }
        if (rs.executeQuery(StrUtil.format("select id from {} where requestid={}", tableName, requestId)) && rs.next()) {
            return rs.getInt("id");
        } else {
            return -1;
        }
    }

    /**
     * 获取数据mainId
     *
     * @param requestId requestId
     * @param tableName tableName
     * @return mainId
     */
    public static int getMainIdBaseRequestIdAndTableName(String requestId, String tableName) {
        RecordSet rs = new RecordSet();
        return getMainIdBaseRequestIdAndTableName(requestId, tableName, rs);
    }

    /**
     * 根据workflowId获取节点列表
     *
     * @param workflowId workflowId
     * @return nodeList
     */
    public static List<Map<String, String>> getNodeList(int workflowId) {
        RecordSet rs = new RecordSet();
        if (rs.executeQuery("SELECT a.nodeid as nodeid, b.nodename as nodename, b.isstart as isstart, b.isreject as isreject, b.isend as isend FROM workflow_flownode a left join workflow_nodebase b on a.nodeid = b.id WHERE workflowid =  ?", workflowId)) {
            List<Map<String, String>> result = new ArrayList<>(rs.getCounts());
            while (rs.next()) {
                result.add(MapUtil.builder(new HashMap<String, String>(5))
                        .put("nodeId", rs.getString("nodeid"))
                        .put("nodeName", rs.getString("nodename"))
                        .put("isStart", rs.getString("isstart"))
                        .put("isReject", rs.getString("isreject"))
                        .put("isEnd", rs.getString("isend"))
                        .build()
                );
            }
            return result;
        }
        return Collections.emptyList();
    }

    /**
     * 根据workflowId获取数据表名
     *
     * @param workflowId 流程id
     * @return 表名
     */
    public static String getTableNameByWorkflowId(int workflowId) {
        RecordSet rs = new RecordSet();
        if (rs.executeQuery("select tablename from workflow_base a left join workflow_bill b on a.formid = b.id where a.id = ?", workflowId) && rs.next()) {
            return rs.getString("tablename");
        }
        return StrUtil.EMPTY;
    }

    /**
     * 根据requestId获取数据表名
     *
     * @param requestId 流程id
     * @return 表名
     */
    public static String getTableNameByRequestId(int requestId) {
        RecordSet rs = new RecordSet();
        if (rs.executeQuery("select c.tablename from workflow_requestbase a left join workflow_base b on a.workflowid = b.id left join workflow_bill c on b.formid = c.id where a.requestid = ?", requestId) && rs.next()) {
            return rs.getString("tablename");
        }
        return StrUtil.EMPTY;
    }

    /**
     * 获取当前节点信息
     *
     * @param requestId 流程id
     * @return map
     */
    public static Map<String, Integer> getNowNodeInfoByRequestId(int requestId) {
        RecordSet rs = new RecordSet();
        if (rs.executeQuery("select requestid, nownodeid, nownodetype from workflow_nownode where requestid = ?", requestId) && rs.next()) {
            return MapUtil.builder(new HashMap<String, Integer>(3))
                    .put("requestId", (Integer) rs.getInt("requestid"))
                    .put("nowNodeId", (Integer) rs.getInt("nownodeid"))
                    .put("nowNodeType", (Integer) rs.getInt("nownodetype"))
                    .build();
        } else {
            return MapUtil.newHashMap();
        }
    }

    /**
     * 获取流程中所有附件id
     *
     * @param requestId requestId
     * @return 附件id列表
     */
    public static Set<Integer> getAllAttachmentDocIdListByRequestId(int requestId) {
        UTILS.writeLog("getAllAttachmentDocIdListByRequestId : " + requestId);
        RecordSet rs = new RecordSet();
        rs.executeQuery("select d.fieldname, c.tablename, d.detailtable, d.viewtype from workflow_requestbase a left join workflow_base b on a.workflowid = b.id left join workflow_bill c on b.formid = c.id left join workflow_billfield d on c.id = d.billid  where a.requestid = ? and d.fieldhtmltype = 6", requestId);
        Set<Integer> attachmentIds = new HashSet<>();
        Set<String> mainFields = new HashSet<>();
        Map<String, Set<String>> detailFieldsMap = new HashMap<>();
        String tableName = StrUtil.EMPTY;
        while (rs.next()) {
            if (StrUtil.isBlank(tableName)) {
                tableName = rs.getString("tablename");
                UTILS.writeLog("tablename : " + tableName);
            }
            switch (rs.getInt("viewtype")) {
                // 主表
                case 0:
                    mainFields.add(rs.getString("fieldname"));
                    break;
                // 明细表
                case 1:
                    String detailName = rs.getString("detailtable");
                    if (detailFieldsMap.containsKey(detailName)) {
                        detailFieldsMap.get(detailName).add(rs.getString("fieldname"));
                    } else {
                        Set<String> fieldList = new HashSet<>();
                        fieldList.add(rs.getString("fieldname"));
                        detailFieldsMap.put(detailName, fieldList);
                    }
                    break;
                default:
            }
        }
        if (!mainFields.isEmpty()) {
            String mainTableAttachmentSql = StrUtil.format("select {} from {} where requestid = {}", String.join(StrUtil.COMMA, mainFields), tableName, requestId);
            UTILS.writeLog("mainTableAttachmentSql : " + mainTableAttachmentSql);
            if (rs.executeQuery(mainTableAttachmentSql) && rs.next()) {
                mainFields.stream()
                        .map(rs::getString)
                        .filter(StrUtil::isNotBlank)
                        .forEach(docIds -> attachmentIds.addAll(Arrays.stream(docIds.split(StrUtil.COMMA))
                                .map(Integer::parseInt)
                                .collect(Collectors.toList())));
            }
        }
        if (!detailFieldsMap.isEmpty()) {
            int mainId = getMainIdBaseRequestIdAndTableName(String.valueOf(requestId), tableName, rs);
            UTILS.writeLog("mainId : " + mainId);
            for (Map.Entry<String, Set<String>> entry : detailFieldsMap.entrySet()) {
                String detailTableAttachmentSql = StrUtil.format("select {} from {} where mainid = {}", String.join(StrUtil.COMMA, entry.getValue()), entry.getKey(), mainId);
                UTILS.writeLog("detailTableAttachmentSql : " + detailTableAttachmentSql);
                rs.executeQuery(detailTableAttachmentSql);
                while (rs.next()) {
                    entry.getValue().stream()
                            .map(rs::getString)
                            .filter(StrUtil::isNotBlank)
                            .forEach(docIds -> attachmentIds.addAll(Arrays.stream(docIds.split(StrUtil.COMMA))
                                    .map(Integer::parseInt)
                                    .collect(Collectors.toList())));
                }

            }
        }
        return attachmentIds;
    }
}
