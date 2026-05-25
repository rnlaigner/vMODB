package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.api.enums.ExecutionModeEnum;

public interface ISchedulerCallback {

    void success(ExecutionModeEnum executionMode, OutboundEventResult outboundEventResult);

    void error(ExecutionModeEnum executionMode, long batch, long tid, Exception e);

}
