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
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
@ConditionalOnProperty(prefix = "sigfox", value = "enabled", havingValue = "true")
public class DefaultSigfoxService implements SigfoxService {

    @Value("${sigfox.configuration}")
    private String configurationFile;

    @Autowired
    private GatewayService gateway;

    private List<SigfoxDeviceTypeConfiguration> sigfoxDeviceTypeConfigurations;

    @PostConstruct
    public void init() throws IOException {
        SigfoxConfiguration configuration;
        try {
            configuration = ConfigurationTools.readConfiguration(configurationFile, SigfoxConfiguration.class);
        } catch (IOException e) {
            log.error("Sigfox service configurataion failed!", e);
            throw e;
        }
        this.sigfoxDeviceTypeConfigurations = configuration.getDeviceTypes();
    }

    @Override
    public void processRequest(String deviceTypeId, String token, String body) throws Exception {
        Optional<SigfoxDeviceTypeConfiguration> configuration = findDeviceTypeConfigurationByDeviceTypeId(deviceTypeId);
        if (configuration.isPresent()) {
            final SigfoxDeviceTypeConfiguration deviceTypeConfiguration = configuration.get();
            if (deviceTypeConfiguration.getToken().equals(token)) {
                processJson(body, deviceTypeConfiguration);
            } else {
                log.error("Request token [{}] for device type id [{}] doesn't match configuration token!", token, deviceTypeId);
                throw new SecurityException("Request token ["+ token + "] for device type id [" + deviceTypeId + "] doesn't match configuration token!");

            }
        } else {
            log.error("No configuration found for device type id [{}]. Please add configuration for this device type id!", deviceTypeId);
            throw new IllegalArgumentException("No configuration found for device type id [" + deviceTypeId + "]. Please add configuration for this device type id!");
        }
    }

    private void processJson(String body, SigfoxDeviceTypeConfiguration deviceTypeConfiguration) {
        DeviceData dd = deviceTypeConfiguration.getMapping().parseBody(body);
        if (dd != null) {
            gateway.onDeviceConnect(dd.getName());
            if (!dd.getAttributes().isEmpty()) {
                gateway.onDeviceAttributesUpdate(dd.getName(), dd.getAttributes());
            }
            if (!dd.getTelemetry().isEmpty()) {
                gateway.onDeviceTelemetry(dd.getName(), dd.getTelemetry());
            }
            gateway.onDeviceDisconnect(dd.getName());
        }
    }

    private Optional<SigfoxDeviceTypeConfiguration> findDeviceTypeConfigurationByDeviceTypeId(String deviceTypeId) {
        return this.sigfoxDeviceTypeConfigurations.stream().filter(
                conf -> conf.getDeviceTypeId().equals(deviceTypeId)
        ).findFirst();
    }

}
