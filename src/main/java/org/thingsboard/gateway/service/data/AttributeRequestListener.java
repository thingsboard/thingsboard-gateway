package org.thingsboard.gateway.service.data;

import lombok.Data;

import java.util.function.Consumer;

/**
 * Created by ashvayka on 03.03.17.
 */
@Data
public class AttributeRequestListener {
    private final AttributeRequest request;
    private final Consumer<AttributeResponse> listener;
}
