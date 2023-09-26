package com.weaver.util.slf;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;

import java.util.HashMap;
import java.util.Map;


/**
 * @author slf
 * @date 2023/7/19
 */
public class MimeTypeUtil {
    public static final Map<String, String> MIME_TYPE_MAP = MapUtil.builder(new HashMap<String, String>())
            .put("json", "application/json")
            .put("doc", "application/msword")
            .put("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
            .build();
    public static String getMimeType(String filePath) {
        String type = FileUtil.getMimeType(filePath);
        if (ObjectUtil.isNull(type)) {
            int index = filePath.lastIndexOf(StrUtil.C_DOT);
            if (index == -1) {
                return type;
            }
            type = MIME_TYPE_MAP.get(filePath.substring(index + 1));
        }
        return type;
    }
}
