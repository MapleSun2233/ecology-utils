package com.weaver.util.slf;

import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
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
     * 创建流程， 该用户必须具有流程创建权限
     *
     * @param createEntity createEntity
     * @return PAResponseEntity result
     */
    public static int createWorkflow(JSONObject createEntity) {
        WorkflowRequestOperatePA operatePa = ServiceUtil.getService(WorkflowRequestOperatePAImpl.class);
        ValidatorUtil.builder()
                .append(operatePa, ObjectUtil::isNull, "WorkflowRequestOperatePA获取失败")
                .append(createEntity, ObjectUtil::isNull, "创建数据实体不能为空")
                .append(createEntity.getString("workflowId"), id -> StrUtil.isBlank(id) || !NumberUtil.isInteger(id), "workflowId错误")
                .append(createEntity.getString("title"), StrUtil::isBlank, "流程标题不能为空")
                .validate();
//        List<WorkflowRequestTableField> mainFields, List<WorkflowDetailTableInfoEntity> detailFields
        User user = null;
        String userId = createEntity.getString("creator");
        if (StrUtil.isNotBlank(userId) && NumberUtil.isInteger(userId)) {
            user = User.getUser(NumberUtil.parseInt(userId), 0);
        } else {
            user = User.getUser(1, 0);
        }
        ReqOperateRequestEntity entity = new ReqOperateRequestEntity();
        entity.setWorkflowId(createEntity.getInteger("workflowId"));
        entity.setRequestName(createEntity.getString("title"));
        entity.setUserId(user.getUID());
        entity.setClientIp("0:0:0:0:0:0:0:1");
        JSONObject mainData = createEntity.getJSONObject("mainData");
        if (ObjectUtil.isNotNull(mainData)) {
            entity.setMainData(buildMainData(mainData));
        }
        JSONArray detailData = createEntity.getJSONArray("detailData");
        if (ObjectUtil.isNotNull(detailData)) {
            entity.setDetailData(buildDetailData(detailData));
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
            throw new RuntimeException(JSONObject.toJSONString(responseEntity.getErrMsg()));
        }
        return JSONObject.parseObject(JSONObject.toJSONString(responseEntity.getData())).getInteger("requestid");
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
     * @return tables
     */
    public static List<WorkflowDetailTableInfoEntity> buildDetailData(JSONArray detailsData) {
        List<WorkflowDetailTableInfoEntity> detailData = new ArrayList<>(detailsData.size());
        for (int i = 0; i < detailsData.size(); i++) {
            JSONObject detailTableData = detailsData.getJSONObject(i);
            ValidatorUtil.validate(detailTableData.getString("detailTableName"), StrUtil::isBlank, "明细表名不能为空");
            JSONObject convertFields = detailTableData.getJSONObject("convertFields");
            WorkflowDetailTableInfoEntity entity = new WorkflowDetailTableInfoEntity();
            entity.setTableDBName(detailTableData.getString("detailTableName"));
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
            requestTableRecord.setRecordOrder(j);
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
     * @param fields 字段值
     * @param convertFields  转换字段配置
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
                    default:
                }
            }
        }
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
        return MapUtil.builder(new HashMap<String, String>(7))
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
            return MapUtil.builder(new HashMap<String, String>(4))
                    .put("requestId", rs.getString("requestId"))
                    .put("workflowId", rs.getString("workflowId"))
                    .put("formId", rs.getString("formId"))
                    .put("tableName", rs.getString("tableName"))
                    .put("currentNodeId", rs.getString("currentNodeId"))
                    .build();
        }
        return MapUtil.newHashMap();
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
}
