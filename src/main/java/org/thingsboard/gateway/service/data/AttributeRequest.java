package org.thingsboard.gateway.service.data;

import lombok.Builder;
import lombok.Data;

/**
 * Created by ashvayka on 02.03.17.
 */
@Data
@Builder
public class AttributeRequest {

    private final int requestId;
    private final String deviceName;
    private final String attributeKey;

    private final boolean clientScope;
    private final String topicExpression;
    private final String valueExpression;
}
