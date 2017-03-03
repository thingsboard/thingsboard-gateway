package org.thingsboard.gateway.extensions.mqtt.client.conf.mapping;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.thingsboard.gateway.service.data.AttributeRequest;
import org.thingsboard.gateway.extensions.mqtt.client.converter.AbstractJsonConverter;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * Created by ashvayka on 22.02.17.
 */
@Data
@Slf4j
public class AttributeRequestsMapping extends AbstractJsonConverter {

    private String topicFilter;

    private String deviceNameJsonExpression;
    private String attributeKeyJsonExpression;
    private String requestIdJsonExpression;

    private String deviceNameTopicExpression;
    private String attributeKeyTopicExpression;
    private String requestIdTopicExpression;

    private Pattern deviceNameTopicPattern;
    private Pattern attributeKeyTopicPattern;
    private Pattern requestIdTopicPattern;

    private boolean clientScope;
    private String responseTopicExpression;
    private String valueExpression;

    public AttributeRequest convert(String topic, MqttMessage msg) {
        deviceNameTopicPattern = checkAndCompile(deviceNameTopicPattern, deviceNameTopicExpression);
        attributeKeyTopicPattern = checkAndCompile(attributeKeyTopicPattern, attributeKeyTopicExpression);
        requestIdTopicPattern = checkAndCompile(requestIdTopicPattern, requestIdTopicExpression);

        String data = new String(msg.getPayload(), StandardCharsets.UTF_8);
        DocumentContext document = JsonPath.parse(data);

        AttributeRequest.AttributeRequestBuilder builder = AttributeRequest.builder();
        builder.deviceName(eval(topic, deviceNameTopicPattern, document, deviceNameJsonExpression));
        builder.attributeKey(eval(topic, attributeKeyTopicPattern, document, attributeKeyJsonExpression));
        builder.requestId(Integer.parseInt(eval(topic, requestIdTopicPattern, document, requestIdJsonExpression)));

        builder.clientScope(this.isClientScope());
        builder.topicExpression(this.getResponseTopicExpression());
        builder.valueExpression(this.getValueExpression());
        return builder.build();
    }
}
