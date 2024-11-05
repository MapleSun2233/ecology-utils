package com.weaver.util.slf.entity;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Set;

/**
 * @author slf
 */
@Data
@Builder
public class SyncWriteDataConfig {
    private String[] remoteFields;
    private String localTable;
    private String[] localFields;
    private String localOnlyCheckField;
    private String remoteOnlyCheckField;
    private List<Integer> doubleIndex;
    private Set<String> doubleFields;
    private Integer formModeId;
}
