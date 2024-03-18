package com.weaver.util.slf;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.api.doc.detail.service.DocSaveService;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import weaver.conn.RecordSet;
import weaver.docs.docs.DocImageManager;
import weaver.docs.docs.util.DesUtils;
import weaver.docs.webservices.DocInfo;
import weaver.docs.webservices.DocServiceImpl;
import weaver.file.ImageFileManager;
import weaver.general.BaseBean;
import weaver.hrm.User;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author slf
 * @date 2023/7/18
 * 文档工具
 */
public class DocUtil {
    /**
     * 公开下载地址
     */
    private static final String DOWNLOAD_TEMPLATE1 = "/weaver/weaver.file.FileDownload?fileid={}&download=1&ddcode={}";

    /**
     * 需要认证的下载地址
     */
    private static final String DOWNLOAD_TEMPLATE2 = "/weaver/weaver.file.FileDownload?fileid={}&download=1";
    private static final String DDCODE_TEMPLATE = "1_{}";

    private static final BaseBean UTILS = new BaseBean();

    /**
     * 构建无需认证的下载链接
     * @param desUtils desUtils
     * @param publicAddress 服务地址
     * @param imageFileId 附件id
     * @return 公开下载地址
     * @throws Exception exception
     */
    public static String buildDownloadUri(DesUtils desUtils, String publicAddress, String imageFileId) throws Exception {
        return publicAddress + StrUtil.format(DOWNLOAD_TEMPLATE1, imageFileId
                , desUtils.encrypt(StrUtil.format(DDCODE_TEMPLATE, imageFileId)));
    }

    /**
     * 构建需要认证的下载链接
     * @param publicAddress 服务地址
     * @param imageFileId 附件id
     * @return 需要认证的下载地址
     */
    public static String buildDownloadUriNeedAuth(String publicAddress, String imageFileId){
        return publicAddress + StrUtil.format(DOWNLOAD_TEMPLATE2, imageFileId);
    }

    /**
     * 上传文件
     * @param fileName 文件名（带拓展名）
     * @param fileBytes 字节数组
     * @return imageFileId
     */
    public static int uploadFile(String fileName, byte[] fileBytes) {
        ImageFileManager ifm = new ImageFileManager();
        ifm.setImageFileType(MimeTypeUtil.getMimeType(fileName));
        ifm.setImagFileName(fileName);
        ifm.setData(fileBytes);
        return ifm.saveImageFile();
    }

    /**
     * 上传文件
     * @param filePath 文件路径
     * @return imageFileId
     */
    public static int uploadFile(String filePath) {
        File file = FileUtil.file(filePath);
        return uploadFile(file);
    }

    /**
     * 上传文件
     * @param file 文件对象
     * @return imageFileId
     */
    public static int uploadFile(File file) {
        ImageFileManager ifm = new ImageFileManager();
        ifm.setImageFileType(MimeTypeUtil.getMimeType(FileUtil.getName(file)));
        ifm.setImagFileName(FileUtil.getName(file));
        ifm.setData(FileUtil.readBytes(file));
        return ifm.saveImageFile();
    }

    /**
     * 上传文件
     * @param response 请求响应
     * @return imageFileId
     * @throws IOException
     */
    public static int uploadFile(HttpResponse response) throws IOException {
        Header header = Optional.ofNullable(response.getFirstHeader("Content-Disposition"))
                .orElse(response.getFirstHeader("content-disposition"));
        String fileName = null;
        // 获取文件名
        if (ObjectUtil.isNotNull(header)) {
            String[] items = header.getValue().split(";");
            for (String s : items) {
                if (s.trim().startsWith("filename=")) {
                    String targetStr = s.trim() .replaceAll("\"", StrUtil.EMPTY);
                    UTILS.writeLog("before decode ::: " + targetStr);
                    fileName = URLDecoder.decode(targetStr.substring(targetStr.indexOf("=") + 1), "UTF-8");
                    UTILS.writeLog("after decode ::: " + fileName);
                    break;
                }
            }
        }
        if (StrUtil.isBlank(fileName)) {
            fileName = StrUtil.toString(System.currentTimeMillis());
            String type = response.getEntity().getContentType().getName();
            if (StrUtil.isNotBlank(type) && type.contains("/")) {
                fileName = StrUtil.format("{}.{}", fileName, type.substring(type.indexOf("/") + 1));
            }
        }
        return uploadFile(fileName, IoUtil.readBytes(response.getEntity().getContent()));
    }

    /**
     * 文件作为附件转文档
     * @param dirId dirId
     * @param imageFileId imageFileId
     * @param user 用户对象
     * @return docId
     * @throws Exception
     */
    public static int fileConvertToDoc(int dirId, int imageFileId, User user) throws Exception {
        if (ObjectUtil.isNull(user)) {
            return fileConvertToDoc(dirId, imageFileId);
        }
        return new DocSaveService().accForDoc(dirId, imageFileId, user);
    }

    /**
     * 文件作为附件转文档
     * @param dirId dirId
     * @param imageFileId imageFileId
     * @return docId
     * @throws Exception
     */
    public static int fileConvertToDoc(int dirId, int imageFileId) throws Exception {
        return new DocSaveService().accForDoc(dirId, imageFileId, User.getUser(1, 0));
    }

    /**
     * 文件作为附件转文档
     * @param dirId dirId
     * @param imageFileId imageFileId
     * @param userId userId
     * @return docId
     * @throws Exception
     */
    public static int fileConvertToDoc(int dirId, int imageFileId, int userId) throws Exception {
        return new DocSaveService().accForDoc(dirId, imageFileId, User.getUser(userId, 0));
    }

    /**
     * 根据docId反查文件imageFileId
     * @param docId docId
     * @return imageFileId
     */
    public static int getImageFileIdByDocId(int docId) {
        RecordSet rs = new RecordSet();
        rs.executeQuery("select imagefileid from DocImageFile where docid = ? order by versionid desc", docId);
        if (rs.next()) {
            return rs.getInt("imagefileid");
        }
        return -1;
    }
    /**
     * 忽略版本根据docId反查文件imageFileId列表
     * @param docId docId
     * @return list
     */
    public static List<Integer> getImageFileIdListByDocIdIgnoreVersion(int docId) {
        RecordSet rs = new RecordSet();
        rs.executeQuery("select imagefileid from DocImageFile where docid = ? order by versionid desc", docId);
        List<Integer> imageFileIdList = new ArrayList<>();
        while (rs.next()) {
            imageFileIdList.add(rs.getInt("imagefileid"));
        }
        return imageFileIdList;
    }

    /**
     * 根据docId反查文件imageFileId列表
     * @param docId docId
     * @return list
     */
    public static List<Integer> getImageFileIdListByDocId(int docId) {
        RecordSet rs = new RecordSet();
        RecordSet rs1 = new RecordSet();
        List<Integer> imageFileIdList = new ArrayList<>();
        rs.executeQuery("select distinct id from DocImageFile where docid = ?", docId);
        while (rs.next()) {
            rs1.executeQuery("select imagefileid from DocImageFile where id = ? and docid = ? order by versionId desc", rs.getInt("id"), docId);
            if (rs1.next()) {
                imageFileIdList.add(rs1.getInt("imagefileid"));
            }
        }
        return imageFileIdList;
    }

    /**
     * 根据docId获取ImageFileManager
     * @param docId docId
     * @return imageFileManager
     */
    public static ImageFileManager getImageFileManagerByDocId(int docId) {
        ImageFileManager imageFileManager = new ImageFileManager();
        imageFileManager.getImageFileInfoById(getImageFileIdByDocId(docId));
        return imageFileManager;
    }

    /**
     * 根据docId获取ImageFileManager列表
     * @param docId docId
     * @return list
     */
    public static List<ImageFileManager> getImageFileManagerListByDocId(int docId) {
        return getImageFileIdListByDocId(docId).stream().map(id -> {
            ImageFileManager imageFileManager = new ImageFileManager();
            imageFileManager.getImageFileInfoById(id);
            return imageFileManager;
        }).collect(Collectors.toList());
    }
    /**
     * 忽略版本根据docId获取ImageFileManager列表
     * @param docId docId
     * @return list
     */
    public static List<ImageFileManager> getImageFileManagerListByDocIdIgnoreVersion(int docId) {
        return getImageFileIdListByDocIdIgnoreVersion(docId).stream().map(id -> {
            ImageFileManager imageFileManager = new ImageFileManager();
            imageFileManager.getImageFileInfoById(id);
            return imageFileManager;
        }).collect(Collectors.toList());
    }
    /**
     * 根据docId获取ImageFileManager
     * @param imageFileId imageFileId
     * @return imageFileManager
     */
    public static ImageFileManager getImageFileManagerByImageFileId(int imageFileId) {
        ImageFileManager imageFileManager = new ImageFileManager();
        imageFileManager.getImageFileInfoById(imageFileId);
        return imageFileManager;
    }

    /**
     * 根据imageFileId获取文件流
     * @param imageFileId imageFileId
     * @return inputStream
     */
    public static InputStream getInputStreamByImageFileId(int imageFileId) {
        return getImageFileManagerByImageFileId(imageFileId).getInputStream();
    }

    /**
     * 根据imageFileId获取文件流
     * @param docId docId
     * @return inputStream
     */
    public static InputStream getInputStreamByDocId(int docId) {
        return getImageFileManagerByDocId(docId).getInputStream();
    }

    /**
     * 创建文档
     * @param subject 主题
     * @param content 内容
     * @param categoryId 目录id
     * @return docId
     * @throws Exception ex
     */
    public static int createDoc(String subject, String content, int categoryId) throws Exception {
        return createDocByUser(subject, content, categoryId, 1);
    }
    /**
     * 创建文档
     * @param subject 主题
     * @param content 内容
     * @param categoryId 目录id
     * @param userId 用户id
     * @return docId
     * @throws Exception ex
     */
    public static int createDocByUser(String subject, String content, int categoryId, int userId) throws Exception {
        return createDocByUser(subject, content, categoryId, User.getUser(userId, 0));
    }
    /**
     * 创建文档
     * @param subject 主题
     * @param content 内容
     * @param categoryId 目录id
     * @param user 用户
     * @return docId
     * @throws Exception ex
     */
    public static int createDocByUser(String subject, String content, int categoryId, User user) throws Exception {
        DocServiceImpl service = new DocServiceImpl();
        DocInfo doc = new DocInfo();
        doc.setDocSubject(subject);
        doc.setDoccontent(content);
        doc.setSeccategory(categoryId);
        return service.createDocByUser(doc, user);
    }

    /**
     * 文档加入附件
     * @param docId 文档id
     * @param imageFileManager 文件manager
     */
    public static void addAttachmentToDoc(int docId, ImageFileManager imageFileManager) {
        DocImageManager manager = new DocImageManager();
        manager.setDocid(docId);
        manager.setImagefileid(imageFileManager.getImageFileId());
        manager.setIsextfile("1");
        manager.setImagefilename(imageFileManager.getImageFileName());
        manager.AddDocImageInfo();
    }

    /**
     * 文档加入附件
     * @param docId 文档id
     * @param imageFileId 文件id
     * @param fileName 文件名
     */
    public static void addAttachmentToDoc(int docId, int imageFileId, String fileName) {
        DocImageManager manager = new DocImageManager();
        manager.setDocid(docId);
        manager.setImagefileid(imageFileId);
        manager.setIsextfile("1");
        manager.setImagefilename(fileName);
        manager.AddDocImageInfo();
    }

    /**
     * 打包指定的imageFile文件为zip文件，返回字节
     * 打包去重策略：
     * 1. 比较是否是同一文件
     * 2. 如果同一文件且文件名相同，视为重复，跳过
     * 3. 如果同一文件不同文件名，视为不重复，继续检查文件名是否与其他文件重复，正常打包
     * 4. 如果不同文件重复文件名，文件名重命名加入编号
     * 5. 不同文件不重复文件名，直接写入
     * @param imageFileManagerList imageFileList
     * @return byte[]
     */
    public static byte[] zipImageFiles(List<ImageFileManager> imageFileManagerList) throws IOException{
        Map<Integer, Set<String>> fileRecord = new HashMap<>(imageFileManagerList.size());
        Map<String, Integer> fileNameRecord = new HashMap<>();
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        ZipArchiveOutputStream zipOutput = new ZipArchiveOutputStream(byteOutput);
        for (ImageFileManager ifm : imageFileManagerList) {
            byte[] fileBytes = IoUtil.readBytes(ifm.getInputStream());
            int byteHashCode = Arrays.hashCode(fileBytes);
            String fileName = ifm.getImageFileName();
            if (fileRecord.containsKey(byteHashCode)) {
                // 存在相同文件
                if (fileRecord.get(byteHashCode).contains(fileName)) {
                    // 相同文件且重名，跳过
                    continue;
                } else {
                    // 相同文件不重名
                    // 继续处理是否与其他文件重名
                    fileName = handleDupFileName(fileNameRecord, fileName);
                    // 添加文件记录
                    fileRecord.get(byteHashCode).add(fileName);
                }
            } else {
                // 不存在相同文件
                fileName = handleDupFileName(fileNameRecord, fileName);
                // 添加文件记录
                fileRecord.put(byteHashCode, new HashSet<>(Collections.singletonList(fileName)));
            }
            // 添加到zip
            zipOutput.putArchiveEntry(new ZipArchiveEntry(fileName));
            zipOutput.write(fileBytes);
            zipOutput.closeArchiveEntry();
        }
        zipOutput.flush();
        zipOutput.close();
        return byteOutput.toByteArray();
    }

    /**
     * 字节转base64
     * @param bytes bytes
     * @return base64
     */
    public static String bytesToBase64(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * 处理重名文件命名
     * @param fileNameRecord 文件名记录
     * @param fileName 带拓展名的文件名
     * @return fileName
     */
    private static String handleDupFileName(Map<String, Integer> fileNameRecord, String fileName) {
        if (fileNameRecord.containsKey(fileName)) {
            // 重名
            int nextIndex = fileNameRecord.get(fileName) + 1;
            // 记录重名
            fileNameRecord.put(fileName, nextIndex);
            // 生成新文件名
            int dotIndex= fileName.lastIndexOf(StrUtil.DOT);
            return fileName.substring(0, dotIndex) + StrUtil.format("({})", nextIndex) + fileName.substring(dotIndex);
        } else {
            // 不重名，首次记录文件名
            fileNameRecord.put(fileName, 0);
        }
        return fileName;
    }
}
