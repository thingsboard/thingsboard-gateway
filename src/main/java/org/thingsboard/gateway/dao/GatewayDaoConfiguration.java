package org.thingsboard.gateway.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.jpa.core.JpaExecutor;
import org.springframework.integration.jpa.inbound.JpaPollingChannelAdapter;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.transaction.TransactionSynchronizationFactory;
import org.springframework.messaging.MessageHandler;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManagerFactory;

/**
 * Created by Valerii Sosliuk on 12/24/2017.
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan("org.thingsboard.gateway.dao")
@EnableJpaRepositories("org.thingsboard.gateway.dao")
@EntityScan("org.thingsboard.gateway.dao")
@EnableTransactionManagement
public class GatewayDaoConfiguration {
/*
    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Bean(name = PollerMetadata.DEFAULT_POLLER_METADATA_BEAN_NAME)
    @Transactional
    public PollerMetadata jpaPoller() {
        return new PollerMetadata();
    }

    @Bean
    public JpaExecutor jpaExecutor() {
        JpaExecutor executor = new JpaExecutor(this.entityManagerFactory);
        executor.setJpaQuery("from PersistentMqttMessage");
        executor.setDeleteAfterPoll(true);
        return executor;
    }

    @Bean
    @InboundChannelAdapter(channel = "jpaInputChannel",
            poller = @Poller(fixedDelay = "3000"))
    public MessageSource<?> jpaInbound() {
        JpaPollingChannelAdapter adapter = new JpaPollingChannelAdapter(jpaExecutor());
        adapter.se
        return adapter;
    }

    @Bean
    @ServiceActivator(inputChannel = "jpaInputChannel")
    public MessageHandler handler() {
        return message -> System.out.println("*** ZALOOPA!!! " + message.getPayload());
    }
    */
}
