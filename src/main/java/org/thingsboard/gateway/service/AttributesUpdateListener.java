package org.thingsboard.gateway.service;

import org.thingsboard.server.common.data.kv.KvEntry;

import java.util.List;

/**
 * Created by ashvayka on 22.02.17.
 */
public interface AttributesUpdateListener {

    void onAttributesUpdated(String deviceName, List<KvEntry> attributes);

}
