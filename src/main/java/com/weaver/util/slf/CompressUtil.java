package com.weaver.util.slf;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import weaver.general.BaseBean;

import java.io.File;
import java.io.InputStream;

public class CompressUtil {
    private static BaseBean UTILS = new BaseBean();

    /**
     * 解压tar包内容到指定路径下
     * @param sourceFile
     * @param outDir
     * @return
     */
    public static boolean unTar(File sourceFile, File outDir) {
        try {
            if (!outDir.exists()) {
                FileUtil.mkParentDirs(outDir);
            }
            try (InputStream is = FileUtil.getInputStream(sourceFile); TarArchiveInputStream tis = new TarArchiveInputStream(is)) {
                TarArchiveEntry entry;
                while ((entry = tis.getNextTarEntry()) != null) {
                    File file = new File(outDir, entry.getName());
                    if (entry.isDirectory()) {
                        file.mkdir();
                    } else {
                        FileUtil.writeBytes(IoUtil.readBytes(tis, false), file);
                        file.setLastModified(entry.getLastModifiedDate().getTime());
                    }
                }
                return true;
            } catch (Exception e) {
                UTILS.writeLog("Fail to unTar, error: " + e.getMessage());
            }
        } catch (Exception e) {
            UTILS.writeLog("Fail to prepare unTar, error: " + e.getMessage());
        }
        return false;
    }

    /**
     * 尝试删除文件对象所指向的文件或文件夹
     * @param file
     */
    public static void deleteFile(File file) {
        if (file.isDirectory()) {
            try {
                UTILS.writeLog("try to delete dir: " + file.getAbsolutePath());
                FileUtils.deleteDirectory(file);
            } catch (Exception e) {
                UTILS.writeLog("fail to delete dir, error : " + e.getMessage());
            }
        } else {
            UTILS.writeLog("try to delete file: " + file.getAbsolutePath());
            if (file.delete() == false) {
                UTILS.writeLog("fail to delete file, error : " + file.getAbsolutePath());
            }
        }
    }
}
