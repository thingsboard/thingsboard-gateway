package org.thingsboard.gateway.service.data;

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.kv.KvEntry;

import java.util.Optional;

/**
 * Created by ashvayka on 02.03.17.
 */
@Data
@Builder
public class AttributeResponse {

    private final int requestId;
    private final String deviceName;
    private final String key;
    private final boolean clientScope;
    private final Optional<KvEntry> data;

    private final String topicExpression;
    private final String valueExpression;
}
