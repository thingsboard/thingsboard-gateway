package org.thingsboard.gateway.extensions.sigfox.conf.mapping;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DoubleValueTransformer.class, name = DoubleValueTransformer.INT_TO_DOUBLE_TRANSFORMER_NAME)
})
public interface DataValueTransformer {

    Double transformToDouble(String strValue);
    Long transformToLong(String strValue);
    String transformToString(String strValue);
    Boolean transformToBoolean(String strValue);

    String getName();

}
