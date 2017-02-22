package org.thingsboard.gateway.extensions.mqtt.client.conf.mapping;

import lombok.Data;

/**
 * Created by ashvayka on 22.02.17.
 */
@Data
public class AttributeUpdatesMapping {

    private String deviceNameFilter;
    private String attributeFilter;
    private String topicExpression;
    private String valueExpression;

}
