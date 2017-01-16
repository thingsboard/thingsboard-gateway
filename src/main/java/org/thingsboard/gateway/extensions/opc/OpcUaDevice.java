package org.thingsboard.gateway.extensions.opc;

import lombok.Data;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.thingsboard.gateway.extensions.opc.conf.mapping.DeviceMapping;

/**
 * Created by ashvayka on 16.01.17.
 */
@Data
public class OpcUaDevice {

    private final NodeId nodeId;
    private final DeviceMapping mapping;
}
