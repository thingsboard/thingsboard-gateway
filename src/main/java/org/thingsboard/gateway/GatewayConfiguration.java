/**
 * Copyright © 2017 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.gateway;

import io.netty.channel.nio.NioEventLoopGroup;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
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

    public static final int NIO_EVENT_LOOP_GROUP_THREADS = 100;

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

    @Bean
    public NioEventLoopGroup getNioEventLoopGroupBean() {
        return new NioEventLoopGroup(NIO_EVENT_LOOP_GROUP_THREADS);
    }
}
