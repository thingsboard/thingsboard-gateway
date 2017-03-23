package org.thingsboard.gateway.extensions.sigfox.conf;

import lombok.Data;
import org.thingsboard.gateway.extensions.sigfox.conf.mapping.SigfoxDeviceMapping;

@Data
public class SigfoxDeviceTypeConfiguration {
    private String deviceTypeId;
    private String token;
    private SigfoxDeviceMapping mapping;
}
