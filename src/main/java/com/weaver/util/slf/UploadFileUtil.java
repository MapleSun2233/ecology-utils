package com.weaver.util.slf;

import cn.hutool.core.io.FileUtil;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import weaver.general.BaseBean;
import weaver.general.GCONST;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.util.Collections;
import java.util.List;

/**
 * @author slf
 * @date 2024/3/18
 * 文件上传处理工具
 */
public class UploadFileUtil {
    /**
     * 设置内存临界值 - 超过后将产生临时文件并存储于临时目录中 , 10MB
     */
    private static final int MEMORY_THRESHOLD = 10485760;
    /**
     * 最大文件大小 200MB
     */
    private static final int MAX_FILE_SIZE = 209715200;
    /**
     *  最大请求大小 300MB
     */
    private static final int MAX_REQUEST_SIZE = 314572800;

    private static final BaseBean UTILS = new BaseBean();

    public static List<FileItem> getFormItem(HttpServletRequest request) {
        try {
            if (!ServletFileUpload.isMultipartContent(request)) {
                UTILS.writeLog("not multipart/data-form, return empty file item list");
                return Collections.emptyList();
            }
            String tempDirPath = GCONST.getSysFilePath();
            if (!tempDirPath.endsWith(File.separator)) {
                tempDirPath += File.separator + "/sliceFile";
            }
            File tempDir = FileUtil.file(tempDirPath);
            if (!tempDir.exists()) {
                tempDir.mkdir();
            }
            // 配置上传参数
            DiskFileItemFactory factory = new DiskFileItemFactory();
            // 设置内存临界值 - 超过后将产生临时文件并存储于临时目录中
            factory.setSizeThreshold(MEMORY_THRESHOLD);
            // 设置临时存储目录
            factory.setRepository(tempDir);

            ServletFileUpload upload = new ServletFileUpload(factory);

            // 设置最大文件上传值
            upload.setFileSizeMax(MAX_FILE_SIZE);

            // 设置最大请求值 (包含文件和表单数据)
            upload.setSizeMax(MAX_REQUEST_SIZE);

            // 中文处理
            upload.setHeaderEncoding("UTF-8");
            return upload.parseRequest(request);
        } catch (Exception e) {
            UTILS.writeLog("fail to get file items, reason : " + e.getMessage());
            return Collections.emptyList();
        }
    }
}
