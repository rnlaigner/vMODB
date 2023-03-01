package dk.ku.di.dms.vms.sdk.core.event.handler;

import dk.ku.di.dms.vms.modb.common.schema.node.VmsNode;
import dk.ku.di.dms.vms.web_common.network.NetworkRunnable;

import java.nio.ByteBuffer;

public class VmsNetworkHandler extends NetworkRunnable implements IVmsEventDeliverer {

    private IVmsEventHandler vmsEventHandler;

    @Override
    public void run() {

        // vmsEventHandler.

    }

    @Override
    public void sendEvent(ByteBuffer buffer, VmsNode target) {

        // buffer it

    }

}
