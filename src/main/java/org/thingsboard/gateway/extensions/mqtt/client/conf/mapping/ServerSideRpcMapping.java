package org.thingsboard.gateway.extensions.mqtt.client.conf.mapping;

import lombok.Data;

/**
 * Created by ashvayka on 22.02.17.
 */
@Data
public class ServerSideRpcMapping {

    private String deviceNameFilter;
    private String methodFilter;
    private String requestTopicExpression;
    private String responseTopicExpression;
    private String valueExpression;

}
