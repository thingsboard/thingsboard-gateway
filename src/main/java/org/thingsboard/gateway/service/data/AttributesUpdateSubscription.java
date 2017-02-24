package org.thingsboard.gateway.service.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.thingsboard.gateway.service.AttributesUpdateListener;

/**
 * Created by ashvayka on 23.02.17.
 */
@Data
@AllArgsConstructor
public class AttributesUpdateSubscription {

    private String deviceNameFilter;
    private AttributesUpdateListener listener;

    public boolean matches(String deviceName) {
        return deviceName.matches(deviceNameFilter);
    }
}
