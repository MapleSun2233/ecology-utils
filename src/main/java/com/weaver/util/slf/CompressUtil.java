package com.weaver.util.slf;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.ArrayUtil;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import weaver.general.BaseBean;

import java.io.*;

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
            try (InputStream is = FileUtil.getInputStream(sourceFile);
                 TarArchiveInputStream tis = new TarArchiveInputStream(is)) {
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
     * 压缩多文件为tar包
     * @param files
     * @param targetTar
     * @return
     */
    public static boolean tar(File[] files, File targetTar) {
        try (FileOutputStream fos = new FileOutputStream(targetTar);
             TarArchiveOutputStream tos = new TarArchiveOutputStream(fos)) {
            if (ArrayUtil.isNotEmpty(files)) {
                for (File file : files) {
                    tarRecursive(tos, file, "");
                }
            }
            return true;
        } catch (IOException e) {
            UTILS.writeLog("Fail to tar, error: " + e.getMessage());
            return false;
        }
    }

    /**
     * 压缩指定目录下的所有文件为tar包，默认不包含源目录，sourceDir为文件时包含源目录无效
     * @param sourceDir
     * @param targetTar
     * @return
     */
    public static boolean tar(File sourceDir, File targetTar) {
        return tar(sourceDir, targetTar, false);
    }
    /**
     * 压缩指定目录下的所有文件为tar包
     * @param sourceDir
     * @param targetTar
     * @param includeSourceDir 是否包含源目录，sourceDir为文件时该参数无效
     * @return
     */
    public static boolean tar(File sourceDir, File targetTar, boolean includeSourceDir) {
        try (FileOutputStream fos = new FileOutputStream(targetTar);
             TarArchiveOutputStream tos = new TarArchiveOutputStream(fos)) {
            if (sourceDir.isFile() || includeSourceDir) {
                tarRecursive(tos, sourceDir, "");
            } else {
                File[] files = sourceDir.listFiles();
                if (ArrayUtil.isNotEmpty(files)) {
                    for (File file : files) {
                        tarRecursive(tos, file, "");
                    }
                }
            }
            return true;
        } catch (IOException e) {
            UTILS.writeLog("Fail to tar, error: " + e.getMessage());
            return false;
        }
    }

    /**
     * 递归遍历文件夹
     * @param tos
     * @param srcFile
     * @param basePath
     * @throws IOException
     */
    private static void tarRecursive(TarArchiveOutputStream tos, File srcFile, String basePath) throws IOException {
        if (srcFile.isDirectory()) {
            File[] files = srcFile.listFiles();
            String nextBasePath = basePath + srcFile.getName() + "/";
            if (ArrayUtil.isEmpty(files)) {
                // 空目录
                TarArchiveEntry entry = new TarArchiveEntry(srcFile, nextBasePath);
                tos.putArchiveEntry(entry);
                tos.closeArchiveEntry();
            } else {
                for (File file : files) {
                    tarRecursive(tos, file, nextBasePath);
                }
            }
        } else {
            TarArchiveEntry entry = new TarArchiveEntry(srcFile, basePath + srcFile.getName());
            tos.putArchiveEntry(entry);
            IoUtil.write(tos, false, FileUtil.readBytes(srcFile));
            tos.closeArchiveEntry();
        }
    }
}
