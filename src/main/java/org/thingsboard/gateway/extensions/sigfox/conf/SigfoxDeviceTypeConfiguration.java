package org.thingsboard.gateway.extensions.sigfox.conf;

import lombok.Data;
import org.thingsboard.gateway.extensions.sigfox.conf.mapping.SigfoxDeviceDataConverter;

@Data
public class SigfoxDeviceTypeConfiguration {
    private String deviceTypeId;
    private String token;
    private SigfoxDeviceDataConverter converter;
}
