package org.thingsboard.gateway.extensions.opc.scan;

import lombok.Data;
import lombok.ToString;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.springframework.util.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ashvayka on 16.01.17.
 */
@Data
@ToString(exclude = "parent")
public class OpcUaNode {

    private final NodeId nodeId;
    private final OpcUaNode parent;
    private final String name;
    private final String fqn;

    public OpcUaNode(NodeId nodeId, String name) {
        this(null, nodeId, name);
    }

    public OpcUaNode(OpcUaNode parent, NodeId nodeId, String name) {
        this.parent = parent;
        this.nodeId = nodeId;
        this.name = name;
        this.fqn = ((parent != null && !StringUtils.isEmpty(parent.getFqn())) ? parent.getFqn() + "." : "") + name;
    }
}
