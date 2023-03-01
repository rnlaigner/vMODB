package dk.ku.di.dms.vms.sdk.core.metadata;

import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.schema.meta.VmsReplicatedTableSchema;
import dk.ku.di.dms.vms.modb.common.schema.meta.VmsTableSchema;
import dk.ku.di.dms.vms.modb.common.schema.meta.VmsEventSchema;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

/**
 * A data class that stores the mappings between events, queues, and transactions.
 * This record is built before the database tables are created, so no tables here.
 * Q: Why data schema is a map?
 * A: Potentially a server could hold two (or more) VMSs. But for now only one.
 * A catalog usually stores tables, views, columns, triggers and procedures in a DBMS
 * See <a href="https://en.wikipedia.org/wiki/Oracle_metadata">Oracle Metadata</a>
 * In our case, the table already stores the columns
 * Q: We don't have triggers nor stored procedures?
 * A: Actually we have mappings from events to application functions
 * For now, we don't have views, but we could implement the application-defined
 * queries as views and store them here
 */
public record VmsRuntimeMetadata(
        // microservice name
        String virtualMicroservice,

        // table schemas
        Map<String, VmsTableSchema> tableSchema,
        // replicated table schemas
        Map<String, VmsReplicatedTableSchema> replicatedTableSchema,

        // map from table (its external FKs) to the external tables
        Map<String, Set<String>> tableToReplicaFkMap,

        // event schemas
        Map<String, VmsEventSchema> inputEventSchema,
        Map<String, VmsEventSchema> outputEventSchema,

        Map<String, VmsTransactionMetadata> queueToVmsTransactionMap,
        Map<String, Class<?>> queueToEventMap,
        Map<Class<?>, String> eventToQueueMap,

        Map<String, Object> loadedVmsInstances,

        // key is the entity (or table) name
        Map<String, IVmsRepositoryFacade> repositoryFacades,
        Map<Class<?>, String> entityToTableNameMap,

        // the return type may be a DTO
        Map<String, Tuple<SelectStatement, Type>> staticQueries

){}