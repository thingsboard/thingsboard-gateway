package org.thingsboard.gateway.extensions.sigfox.conf.mapping;

import lombok.Data;
import org.thingsboard.gateway.extensions.opc.conf.mapping.KVMapping;

// TODO KVMapping at the moment used from OPC
@Data
public class SigfoxKVMapping extends KVMapping{
    private DataValueTransformer transformer;
}
