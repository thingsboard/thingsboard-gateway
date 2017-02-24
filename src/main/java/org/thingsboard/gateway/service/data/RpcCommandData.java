package org.thingsboard.gateway.service.data;

import lombok.Data;

/**
 * Created by ashvayka on 22.02.17.
 */
@Data
public class RpcCommandData {

    private String method;
    private String params;

}
