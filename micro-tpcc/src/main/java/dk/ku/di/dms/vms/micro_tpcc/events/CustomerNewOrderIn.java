package dk.ku.di.dms.vms.micro_tpcc.events;

import dk.ku.di.dms.vms.sdk.core.annotations.Event;

@Event
public record CustomerNewOrderIn(
     int c_w_id,
     int c_d_id,
     int c_id) {}
