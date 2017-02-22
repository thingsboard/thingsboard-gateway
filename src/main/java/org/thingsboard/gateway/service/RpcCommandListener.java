package org.thingsboard.gateway.service;

import org.thingsboard.server.common.data.kv.KvEntry;

import java.util.List;

/**
 * Created by ashvayka on 22.02.17.
 */
public interface RpcCommandListener {

    void onRpcCommand(String deviceName, RpcCommandData command);

}
