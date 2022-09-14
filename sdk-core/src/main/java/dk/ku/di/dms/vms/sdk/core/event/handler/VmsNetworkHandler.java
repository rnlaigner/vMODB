package dk.ku.di.dms.vms.sdk.core.event.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.web_common.network.NetworkRunnable;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.nio.ByteBuffer;

public class VmsNetworkHandler extends NetworkRunnable implements IVmsEventDeliverer {

    private IVmsEventHandler vmsEventHandler;

    @Override
    public void run() {

        // vmsEventHandler.

    }

    @Override
    public void sendEvent(ByteBuffer buffer, VmsIdentifier target) {

        // buffer it

    }

}
