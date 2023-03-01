package dk.ku.di.dms.vms.modb.common.constraint;

public record ExternalVmsForeignKeyReference
        (String vmsName,
         String vmsTableName, // this is always part of the same virtual microservice
         String columnName) {}