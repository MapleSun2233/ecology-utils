package com.weaver.util.slf;

import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.engine.common.util.ServiceUtil;
import com.engine.workflow.entity.publicApi.PAResponseEntity;
import com.engine.workflow.entity.publicApi.ReqOperateRequestEntity;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
     * @param requestId requestId
     * @param fields 字段赋值列表
     * @return boolean 操作结果
     */
    public static boolean submitWorkflow(int requestId, int nodeId,List<WorkflowRequestTableField> fields) {
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
     * @param workflowId workflowId
     * @param title 标题
     * @param user 用户
     * @param fields 字段赋值列表
     * @return jsonObject
     */
    public static JSONObject createWorkflow(int workflowId, String title,User user, List<WorkflowRequestTableField> fields) {
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
     * 强制归档，该流程所处的当前节点必须开启了强制归档，否则无权限
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
}
