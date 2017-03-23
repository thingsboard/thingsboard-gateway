package org.thingsboard.gateway.extensions.sigfox;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.thingsboard.gateway.extensions.sigfox.conf.SigfoxConfiguration;
import org.thingsboard.gateway.extensions.sigfox.conf.SigfoxDeviceTypeConfiguration;
import org.thingsboard.gateway.service.GatewayService;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.gateway.util.ConfigurationTools;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@ConditionalOnProperty(prefix = "sigfox", value = "enabled", havingValue = "true")
public class DefaultSigfoxService implements SigfoxService {

    @Value("${sigfox.configuration}")
    private String configurationFile;

    @Autowired
    private GatewayService gateway;

    private Map<String, SigfoxDeviceTypeConfiguration> sigfoxDeviceTypeConfigurations = new HashMap<>();

    @PostConstruct
    public void init() throws IOException {
        SigfoxConfiguration configuration;
        try {
            configuration = ConfigurationTools.readConfiguration(configurationFile, SigfoxConfiguration.class);
            configuration
                    .getDeviceTypeConfigurations()
                    .forEach(dtc -> this.sigfoxDeviceTypeConfigurations.put(dtc.getDeviceTypeId(), dtc));
        } catch (IOException e) {
            log.error("Sigfox service configuration failed!", e);
            throw e;
        }
    }

    @Override
    public void processRequest(String deviceTypeId, String token, String body) throws Exception {
        SigfoxDeviceTypeConfiguration configuration = sigfoxDeviceTypeConfigurations.get(deviceTypeId);
        if (configuration != null) {
            if (configuration.getToken().equals(token)) {
                processBody(body, configuration);
            } else {
                log.error("Request token [{}] for device type id [{}] doesn't match configuration token!", token, deviceTypeId);
                throw new SecurityException("Request token [" + token + "] for device type id [" + deviceTypeId + "] doesn't match configuration token!");
            }
        } else {
            log.error("No configuration found for device type id [{}]. Please add configuration for this device type id!", deviceTypeId);
            throw new IllegalArgumentException("No configuration found for device type id [" + deviceTypeId + "]. Please add configuration for this device type id!");
        }
    }

    private void processBody(String body, SigfoxDeviceTypeConfiguration configuration) {
        DeviceData dd = configuration.getConverter().parseBody(body);
        if (dd != null) {
            // TODO: onDeviceConnect should return Future instead of Void. Executing future.get to ensure that there is no race conditions here.
            gateway.onDeviceConnect(dd.getName());
            if (!dd.getAttributes().isEmpty()) {
                gateway.onDeviceAttributesUpdate(dd.getName(), dd.getAttributes());
            }
            if (!dd.getTelemetry().isEmpty()) {
                gateway.onDeviceTelemetry(dd.getName(), dd.getTelemetry());
            }
            // TODO: onDeviceDisconnect should return Future instead of Void. Executing future.get to ensure that there is no race conditions here.
            gateway.onDeviceDisconnect(dd.getName());
        } else {
            log.error("DeviceData is null. Body [{}] was not parsed successfully!", body);
            throw new IllegalArgumentException("Device Data is null. Body [" + body + "] was not parsed successfully!");
        }
    }
}
