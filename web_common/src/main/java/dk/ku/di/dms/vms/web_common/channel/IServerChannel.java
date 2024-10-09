package dk.ku.di.dms.vms.web_common.channel;

import java.nio.channels.CompletionHandler;

public interface IServerChannel extends IChannel {

    void accept(CompletionHandler<IChannel, Void> handler);

}
