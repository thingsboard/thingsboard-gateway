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

package org.thingsboard.gateway.extensions.modbus;

import com.ghgande.j2mod.modbus.procimg.InputRegister;
import com.ghgande.j2mod.modbus.util.BitVector;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.gateway.extensions.modbus.conf.ModbusExtensionConstants;
import org.thingsboard.gateway.extensions.modbus.conf.mapping.DeviceMapping;
import org.thingsboard.gateway.extensions.modbus.conf.mapping.PollingTagMapping;
import org.thingsboard.gateway.extensions.modbus.util.ModbusUtils;
import org.thingsboard.server.common.data.kv.*;

import java.util.*;

@Slf4j
public class ModbusDevice {
    private DeviceMapping configuration;

    private Map<Integer, List<PollingTagMapping>> tagsByPollPeriod = new HashMap<>();

    private Map<PollingTagMapping, KvEntry> attributes = new HashMap<>();
    private Map<PollingTagMapping, TsKvEntry> timeseries = new HashMap<>();

    private List<KvEntry> attributesUpdates = new LinkedList<>();
    private List<TsKvEntry> timeseriesUpdates = new LinkedList<>();

    public ModbusDevice(DeviceMapping conf) {
        this.configuration = conf;

        sortByPollPeriod(configuration.getAttributes(), configuration.getAttributesPollPeriod());
        sortByPollPeriod(configuration.getTimeseries(), configuration.getTimeseriesPollPeriod());

        configuration.getAttributes().stream().forEach(attr -> attributes.put(attr, null));
    }

    private void sortByPollPeriod(List<PollingTagMapping> mappings, int defaultPollPeriod) {
        mappings.stream().forEach(m -> {
            int pollPeriod = m.getPollPeriod();
            if (pollPeriod == ModbusExtensionConstants.NO_POLL_PERIOD_DEFINED) {
                pollPeriod = defaultPollPeriod;
            }

            tagsByPollPeriod.computeIfAbsent(pollPeriod, (k) -> new LinkedList<>()).add(m);
        });
    }

    public int getUnitId() {
        return configuration.getUnitId();
    }

    public String getName() {
        return configuration.getDeviceName();
    }

    public Map<Integer, List<PollingTagMapping>> getSortedTagMappings() {
        return tagsByPollPeriod;
    }

    public void updateTag(PollingTagMapping mapping, BitVector data) {
        updateTag(mapping, ModbusUtils.convertToDataEntry(mapping, data.getBit(ModbusExtensionConstants.DEFAULT_BIT_INDEX_FOR_BOOLEAN)));
    }

    public void updateTag(PollingTagMapping mapping, InputRegister[] data) {
        updateTag(mapping, ModbusUtils.convertToDataEntry(mapping, data));
    }

    private void updateTag(PollingTagMapping mapping, KvEntry entry) {
        if (attributes.containsKey(mapping.getTag())) {
            KvEntry oldEntry = attributes.get(mapping.getTag());
            if (oldEntry == null || !oldEntry.getValue().equals(entry.getValue())) {
                attributes.put(mapping, entry);
                attributesUpdates.add(entry);

                log.debug("MBD[{}] attribute update: key '{}', val '{}'", configuration.getDeviceName(), entry.getKey(), entry.getValue());
            }
        } else {
            TsKvEntry oldEntry = timeseries.get(mapping.getTag());
            if (oldEntry == null || !oldEntry.getValue().equals(entry.getValue())) {
                TsKvEntry newTsEntry = new BasicTsKvEntry(System.currentTimeMillis(), entry);
                timeseries.put(mapping, newTsEntry);
                timeseriesUpdates.add(newTsEntry);

                log.debug("MBD[{}] timeseries update:  key '{}', val '{}'", configuration.getDeviceName(), entry.getKey(), entry.getValue());
            }
        }
    }

    public List<PollingTagMapping> getAttributesMappings() {
        return configuration.getAttributes();
    }

    public List<PollingTagMapping> getTimeseriesMappings() {
        return configuration.getTimeseries();
    }

    public List<KvEntry> getAttributesUpdate() {
        return attributesUpdates;
    }

    public List<TsKvEntry> getTimeseriesUpdate() {
        return timeseriesUpdates;
    }

    public void clearUpdates() {
        attributesUpdates.clear();
        timeseriesUpdates.clear();
    }
}
