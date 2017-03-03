package org.thingsboard.gateway.service.data;

import lombok.Builder;
import lombok.Data;

/**
 * Created by ashvayka on 02.03.17.
 */
@Data
public class AttributeRequestKey {
    private final int requestId;
    private final String deviceName;
}
