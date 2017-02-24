package org.thingsboard.gateway.service;

import org.thingsboard.gateway.service.data.RpcCommandData;

/**
 * Created by ashvayka on 22.02.17.
 */
public interface RpcCommandListener {

    void onRpcCommand(String deviceName, RpcCommandData command);

}
