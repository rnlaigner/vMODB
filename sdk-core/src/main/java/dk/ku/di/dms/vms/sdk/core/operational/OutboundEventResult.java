package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.common.transaction.TransactionWrite;

import java.util.List;
import java.util.Map;

/**
 * Just a placeholder.
 * The output event needs to be converted before being sent
 */
public record OutboundEventResult(long tid, long batch, String outputQueue, Object output, Map<String, List<TransactionWrite>> updates)
{}