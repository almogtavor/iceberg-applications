package io.github.dormog.configuration.properties;

import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
@Getter
public enum ActiveTask {
    COMPACTION("compaction"),
    REWRITE_MANIFESTS("rewrite-manifests"),
    DELETE_ORPHANS("delete-orphans"),
    EXPIRE_SNAPSHOTS("expire-snapshots");

    private final String taskName;
}
