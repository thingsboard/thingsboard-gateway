package org.thingsboard.gateway;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.thingsboard.gateway.service.DefaultTenantManagerService;
import org.thingsboard.gateway.service.PersistentFileService;
import org.thingsboard.gateway.service.PersistentFileServiceImpl;
import org.thingsboard.gateway.service.TenantManagerService;
import org.thingsboard.gateway.service.conf.TbPersistenceConfiguration;
import org.thingsboard.gateway.service.conf.TbTenantConfiguration;
import org.thingsboard.gateway.service.gateway.GatewayService;
import org.thingsboard.gateway.service.gateway.MqttGatewayService;

import java.util.function.Consumer;

/**
 * Created by Valerii Sosliuk on 1/23/2018.
 */
@Configuration
public class GatewayConfiguration {

    @Bean
    public TenantManagerService getTenantManagerService() {
        return new DefaultTenantManagerService() {
            @Override
            public GatewayService getGatewayService(TbTenantConfiguration configuration, Consumer<String> extensionsConfigListener) {
                return getGatewayServiceBean(configuration, extensionsConfigListener);
            }
        };
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public PersistentFileService getPersistentFileServiceBean(String tenantName, TbPersistenceConfiguration tbPersistenceConfiguration) {
        PersistentFileServiceImpl persistentFileService = new PersistentFileServiceImpl();
        persistentFileService.setTenantName(tenantName);
        persistentFileService.setPersistence(tbPersistenceConfiguration);
        return persistentFileService;
    }


    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public GatewayService getGatewayServiceBean(TbTenantConfiguration configuration, Consumer<String> extensionsConfigListener) {

        MqttGatewayService gatewayService = new MqttGatewayService(configuration, extensionsConfigListener);
        gatewayService.setPersistentFileService(getPersistentFileServiceBean(configuration.getLabel(), configuration.getPersistence()));
        return gatewayService;
    }
}
