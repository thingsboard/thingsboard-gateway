package org.thingsboard.gateway.extensions.sigfox.conf;

import lombok.Data;

import java.util.List;

@Data
public class SigfoxConfiguration {
    List<SigfoxDeviceTypeConfiguration> deviceTypes;
}
