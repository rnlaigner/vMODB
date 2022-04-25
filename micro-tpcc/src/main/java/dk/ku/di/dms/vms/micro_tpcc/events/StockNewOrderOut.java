package dk.ku.di.dms.vms.micro_tpcc.events;

import dk.ku.di.dms.vms.modb.common.event.IApplicationEvent;

public record StockNewOrderOut (
    int[] itemIds,
    String[] itemsDistInfo) implements IApplicationEvent {}