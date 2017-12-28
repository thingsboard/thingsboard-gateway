package org.thingsboard.gateway.dao;

import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.util.UUID;

/**
 * Created by Valerii Sosliuk on 12/24/2017.
 */
public interface PersistentMqttMessageRepository extends PagingAndSortingRepository<PersistentMqttMessage, UUID> {

}
