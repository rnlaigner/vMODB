package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.common.transaction.TransactionWrite;

import java.util.List;
import java.util.Map;

public record InboundEvent (
        long tid, long lastTid, long batch, String event, Object input, Map<String, List<TransactionWrite>> updates)
{ }
