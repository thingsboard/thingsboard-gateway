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
package org.thingsboard.gateway.extensions.file;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.thingsboard.gateway.extensions.file.conf.FileTailConfiguration;
import org.thingsboard.gateway.service.GatewayService;
import org.thingsboard.gateway.util.ConfigurationTools;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ashvayka on 15.05.17.
 */
@Service
@ConditionalOnProperty(prefix = "file", value = "enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class DefaultFileTailService {
    @Autowired
    private GatewayService service;

    @Value("${file.configuration}")
    private String configurationFile;

    private List<FileMonitor> brokers;

    @PostConstruct
    public void init() throws Exception {
        log.info("Initializing File Tail service!");
        FileTailConfiguration configuration;
        try {
            configuration = ConfigurationTools.readConfiguration(configurationFile, FileTailConfiguration.class);
        } catch (Exception e) {
            log.error("File Tail service configuration failed!", e);
            throw e;
        }

        try {
            brokers = configuration.getFileMonitorConfigurations().stream().map(c -> new FileMonitor(service, c)).collect(Collectors.toList());
            brokers.forEach(FileMonitor::init);
        } catch (Exception e) {
            log.error("File Tail service initialization failed!", e);
            throw e;
        }
    }

    @PreDestroy
    public void preDestroy() {
        if (brokers != null) {
            brokers.forEach(FileMonitor::stop);
        }
    }

}
