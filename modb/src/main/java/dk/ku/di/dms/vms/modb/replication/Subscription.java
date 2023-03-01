package dk.ku.di.dms.vms.modb.replication;

public record Subscription (
        String vmsName, String table
) { }