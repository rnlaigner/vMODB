package dk.ku.di.dms.vms.sdk.core.metadata;

import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.common.meta.*;
import dk.ku.di.dms.vms.modb.common.utils.IdentifiableNode;
import dk.ku.di.dms.vms.sdk.core.event.pubsub.IVmsInternalPubSubService;
import dk.ku.di.dms.vms.sdk.core.metadata.exception.UnsupportedConstraint;
import dk.ku.di.dms.vms.sdk.core.annotations.*;
import dk.ku.di.dms.vms.modb.common.event.IApplicationEvent;
import dk.ku.di.dms.vms.sdk.core.client.VmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.core.metadata.exception.QueueMappingException;
import dk.ku.di.dms.vms.sdk.core.metadata.exception.NotAcceptableTypeException;
import dk.ku.di.dms.vms.sdk.core.metadata.exception.NoPrimaryKeyFoundException;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;
import org.reflections.Configuration;
import org.reflections.Reflections;

import org.reflections.scanners.*;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

import static java.util.logging.Logger.GLOBAL_LOGGER_NAME;
import static java.util.logging.Logger.getLogger;

public class VmsMetadataLoader {

    private VmsMetadataLoader(){}

    private static final Logger logger = getLogger(GLOBAL_LOGGER_NAME);

    public static VmsMetadata load(String packageName, IVmsInternalPubSubService vmsInternalPubSubService) throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException {

        final Reflections reflections = configureReflections(packageName);

        Map<Class<?>,String> vmsTableNames = loadVmsTableNames(reflections);

        Map<String, Object> loadedVmsInstances = loadMicroserviceClasses(reflections, vmsInternalPubSubService);

        Map<String, VmsDataSchema> vmsDataSchema = buildDataSchema(reflections, reflections.getConfiguration(), vmsTableNames);

        // necessary remaining data structures to store a vms metadata
        Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToVmsTransactionMap = new HashMap<>();
        Map<String, Class<? extends IApplicationEvent>> queueToEventMap = new HashMap<>();
        Map<Class<? extends IApplicationEvent>,String> eventToQueueMap = new HashMap<>();

        mapVmsTransactionInputOutput(reflections, loadedVmsInstances, queueToEventMap, eventToQueueMap, eventToVmsTransactionMap);

        Map<String, VmsEventSchema> vmsEventSchema = buildEventSchema( reflections, eventToQueueMap );

        /* TODO look at this. we should provide this implementation
            SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
            SLF4J: Defaulting to no-operation (NOP) logger implementation
            SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
         */

        return new VmsMetadata( vmsDataSchema, vmsEventSchema, eventToVmsTransactionMap, queueToEventMap, eventToQueueMap, loadedVmsInstances );

    }

    private static Reflections configureReflections(String packageName){
        if(packageName == null) {
            packageName = "dk.ku.di.dms.vms";
        }
        Configuration reflectionsConfig = new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(packageName))
                .setScanners(
                        Scanners.SubTypes,
                        Scanners.TypesAnnotated,
                        Scanners.MethodsAnnotated,
                        Scanners.FieldsAnnotated
                );

        return new Reflections(reflectionsConfig);
    }

    /**
     * Map the entities annotated with {@link VmsTable}
     */
    private static Map<Class<?>, String> loadVmsTableNames(Reflections reflections) {

        Set<Class<?>> vmsTables = reflections.getTypesAnnotatedWith(VmsTable.class);
        Map<Class<?>, String> vmsTableNameMap = new HashMap<>();
        for(Class<?> vmsTable : vmsTables){

            Optional<Annotation> optionalVmsTableAnnotation = Arrays.stream(vmsTable.getAnnotations())
                    .filter(p -> p.annotationType() == VmsTable.class).findFirst();
            optionalVmsTableAnnotation.ifPresent(annotation -> vmsTableNameMap.put(vmsTable, ((VmsTable) annotation).name()));
        }

        return vmsTableNameMap;

    }

    /**
     * Building virtual microservice event schemas
     */
    protected static Map<String, VmsEventSchema> buildEventSchema(final Reflections reflections, Map<Class<? extends IApplicationEvent>, String> eventToQueueMap) {

        Map<String, VmsEventSchema> schemaMap = new HashMap<>();

        Set<Class<? extends IApplicationEvent>> eventsClazz = reflections.getSubTypesOf( IApplicationEvent.class );

        for( Class<? extends IApplicationEvent> eventClazz : eventsClazz ){

            Field[] fields = eventClazz.getDeclaredFields();

            String[] columnNames = new String[fields.length];
            DataType[] columnDataTypes = new DataType[fields.length];
            int i = 0;

            for( Field field : fields ){

                Class<?> attributeType = field.getType();
                columnDataTypes[i] = getColumnDataTypeFromAttributeType(attributeType);
                columnNames[i] = field.getName();
                i++;

            }

            // get queue name
            String queue = eventToQueueMap.get( eventClazz );

            schemaMap.put( queue, new VmsEventSchema( queue, columnNames, columnDataTypes ) );

        }

        return schemaMap;

    }

    /**
     * Building virtual microservice table schemas
     */
    @SuppressWarnings("unchecked")
    protected static Map<String, VmsDataSchema> buildDataSchema(final Reflections reflections,
                                                                final Configuration reflectionsConfig,
                                                                final Map<Class<?>, String> vmsTableNames) {

        Map<String, VmsDataSchema> schemaMap = new HashMap<>();

        // get all fields annotated with column
        Set<Field> allEntityFields = reflections.get(
                Scanners.FieldsAnnotated.with( Column.class )// .add(FieldsAnnotated.with(Id.class))
                        .as(Field.class, reflectionsConfig.getClassLoaders())
        );

        // group by class
        Map<Class<?>,List<Field>> columnMap = allEntityFields.stream()
                .collect( Collectors.groupingBy( Field::getDeclaringClass,
                        Collectors.toList()) );

        // get all fields annotated with column
        Set<Field> allPrimaryKeyFields = reflections.get(
                Scanners.FieldsAnnotated.with( Id.class )
                        .as(Field.class, reflectionsConfig.getClassLoaders())
        );

        // group by entity type
        Map<Class<?>,List<Field>> pkMap = allPrimaryKeyFields.stream()
                .collect( Collectors.groupingBy( Field::getDeclaringClass,
                        Collectors.toList()) );

        // get all foreign keys
        Set<Field> allAssociationFields = reflections.get(
                Scanners.FieldsAnnotated.with(VmsForeignKey.class)
                        .as(Field.class, reflectionsConfig.getClassLoaders())
        );

        // group by entity type
        Map<Class<?>,List<Field>> associationMap = allAssociationFields.stream()
                .collect( Collectors.groupingBy( Field::getDeclaringClass,
                        Collectors.toList()) );

        // build schema of each table
        // we build the schema in order to look up the fields and define the pk hash index
        for(final Map.Entry<Class<?>, List<Field>> entry : pkMap.entrySet()){

            Class<? extends IEntity<?>> tableClass = (Class<? extends IEntity<?>>) entry.getKey();
            List<Field> pkFields = entry.getValue();

            if(pkFields == null || pkFields.size() == 0){
                throw new NoPrimaryKeyFoundException("Table class "+tableClass.getCanonicalName()+" does not have a primary key.");
            }
            int totalNumberOfFields = pkFields.size();

            List<Field> foreignKeyFields = associationMap.get( tableClass );
            List<Field> columnFields = columnMap.get( tableClass );

            if(foreignKeyFields != null) {
                totalNumberOfFields += foreignKeyFields.size();
            }

            if(columnFields != null) {
                totalNumberOfFields += columnFields.size();
            }

            final String[] columnNames = new String[totalNumberOfFields];
            final DataType[] columnDataTypes = new DataType[totalNumberOfFields];

            int[] pkFieldsStr = new int[pkFields.size()];

            // iterating over pk columns
            int i = 0;
            for(final Field field : pkFields){
                Class<?> attributeType = field.getType();
                columnDataTypes[i] = getColumnDataTypeFromAttributeType(attributeType);
                pkFieldsStr[i] = i;
                columnNames[i] = field.getName();
                i++;
            }

            ForeignKeyReference[] foreignKeyReferences = null;
            if(foreignKeyFields != null) {
                foreignKeyReferences = new ForeignKeyReference[foreignKeyFields.size()];
                int j = 0;
                // iterating over association columns
                for (final Field field : foreignKeyFields) {

                    Class<?> attributeType = field.getType();
                    columnDataTypes[i] = getColumnDataTypeFromAttributeType(attributeType);
                    columnNames[i] = field.getName();
                    i++;

                    Optional<Annotation> fkAnnotation = Arrays.stream(field.getAnnotations())
                            .filter(p -> p.annotationType() == VmsForeignKey.class).findFirst();
                    if( fkAnnotation.isPresent() ) {
                        VmsForeignKey fk = (VmsForeignKey) fkAnnotation.get();
                        String fkTable = vmsTableNames.get(fk.table());
                        // later we parse into a Vms Table and check whether the types match
                        foreignKeyReferences[j] = new ForeignKeyReference(fkTable, fk.column());
                    } else {
                        // FIXME throw new exception
                    }

                    j++;
                }

            }

            // non-foreign key column constraints are inherent to the table, not referring to other tables
            ConstraintReference[] constraints = getConstraintReferences(columnFields, columnNames, columnDataTypes, i);

            Optional<Annotation> optionalVmsTableAnnotation = Arrays.stream(tableClass.getAnnotations())
                    .filter(p -> p.annotationType() == VmsTable.class).findFirst();
            if(optionalVmsTableAnnotation.isPresent()){
                String vmsTableName = ((VmsTable)optionalVmsTableAnnotation.get()).name();
                VmsDataSchema schema = new VmsDataSchema(vmsTableName, pkFieldsStr, columnNames, columnDataTypes, foreignKeyReferences, constraints);
                schemaMap.put(vmsTableName, schema);
            } else {
                // TODO throw exception should be annotated with vms table
            }

        }

        return schemaMap;

    }

    private static ConstraintReference[] getConstraintReferences(List<Field> columnFields, String[] columnNames, DataType[] columnDataTypes, int columnPosition)
            throws UnsupportedConstraint, NotAcceptableTypeException {
        if(columnFields != null) {

            ConstraintReference[] constraints = null;

            // iterating over non-pk and non-fk columns;
            for (final Field field : columnFields) {

                Class<?> attributeType = field.getType();
                columnDataTypes[columnPosition] = getColumnDataTypeFromAttributeType(attributeType);

                // get constraints ought to be applied to this column, e.g., non-negative, not null, nullable
                List<Annotation> constraintAnnotations = Arrays.stream(field.getAnnotations())
                        .filter(p -> p.annotationType() == Positive.class ||
                                p.annotationType() == PositiveOrZero.class ||
                                p.annotationType() == NotNull.class ||
                                p.annotationType() == Null.class
                        ).collect(Collectors.toList());

                constraints = new ConstraintReference[constraintAnnotations.size()];
                int nC = 0;
                for (Annotation constraint : constraintAnnotations) {
                    String constraintName = constraint.annotationType().getName();
                    switch (constraintName) {
                        case "javax.validation.constraints.PositiveOrZero":
                            constraints[nC] = new ConstraintReference(ConstraintEnum.POSITIVE_OR_ZERO, columnPosition);
                            break;
                        case "javax.validation.constraints.Positive":
                            constraints[nC] = new ConstraintReference(ConstraintEnum.POSITIVE, columnPosition);
                            break;
                        case "javax.validation.constraints.Null":
                            constraints[nC] = new ConstraintReference(ConstraintEnum.NULL, columnPosition);
                            break;
                        case "javax.validation.constraints.NotNull":
                            constraints[nC] = new ConstraintReference(ConstraintEnum.NOT_NULL, columnPosition);
                            break;
                        default:
                            throw new UnsupportedConstraint("Constraint " + constraintName + " not supported.");
                    }
                    nC++;
                }

                columnNames[columnPosition] = field.getName();
                columnPosition++;
            }

            return constraints;

        }
        return null;
    }

    @SuppressWarnings("unchecked")
    protected static Map<String,Object> loadMicroserviceClasses(Reflections reflections, IVmsInternalPubSubService vmsInternalPubSubService)
            throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException {

        Set<Class<?>> vmsClasses = reflections.getTypesAnnotatedWith(Microservice.class);

        Map<String,Object> loadedMicroserviceInstances = new HashMap<>();

        for(Class<?> clazz : vmsClasses) {

            String clazzName = clazz.getCanonicalName();
            Class<?> cls = Class.forName(clazzName);
            Constructor<?>[] constructors = cls.getDeclaredConstructors();
            Constructor<?> constructor = constructors[0];

            List<Object> repositoryList = new ArrayList<>();

            for (Class parameterType : constructor.getParameterTypes()) {

                VmsRepositoryFacade facade = new VmsRepositoryFacade(parameterType, vmsInternalPubSubService.requestQueue(), vmsInternalPubSubService.responseMap());

                Object proxyInstance = Proxy.newProxyInstance(
                        VmsMetadataLoader.class.getClassLoader(),
                        new Class[]{parameterType},
                        // it works without casting as long as all services respect
                        // the constructor rule to have only repositories
                        facade);

                repositoryList.add(proxyInstance);

            }

            Object vmsInstance = constructor.newInstance(repositoryList.toArray());

            loadedMicroserviceInstances.put(clazzName, vmsInstance);
        }

        return loadedMicroserviceInstances;

    }

    /**
     * Map transactions to input and output events
     */
    @SuppressWarnings("unchecked")
    protected static void mapVmsTransactionInputOutput(final Reflections reflections,
                                                       final Map<String, Object> loadedMicroserviceInstances,
                                                       final Map<String, Class<? extends IApplicationEvent>> queueToEventMap,
                                                       final Map<Class<? extends IApplicationEvent>, String> eventToQueueMap,
                                                       final Map<String,
                                                               List<IdentifiableNode<VmsTransactionSignature>>>
                                                               eventToVmsTransactionMap) {

        Set<Method> transactionalMethods = reflections.getMethodsAnnotatedWith(Transactional.class);

        for (final Method method : transactionalMethods) {
            logger.info("Mapped = " + method.getName());

            String className = method.getDeclaringClass().getCanonicalName();
            Object obj = loadedMicroserviceInstances.get(className);

            Class<? extends IApplicationEvent> outputType;
            try{
                outputType = (Class<? extends IApplicationEvent>) method.getReturnType();
            } catch(Exception e) {
                throw new QueueMappingException("All output events must implement IEvent interface.");
            }

            List<Class<? extends IApplicationEvent>> inputTypes = new ArrayList<>();

            for(int i = 0; i < method.getParameters().length; i++){
                try{
                    Class<? extends IApplicationEvent> clazz = (Class<? extends IApplicationEvent>) method.getParameters()[i].getType();
                    inputTypes.add( clazz);
                } catch(Exception e) {
                    throw new QueueMappingException("All input events must implement IEvent interface.");
                }
            }

            Annotation[] annotations = method.getAnnotations();

            final VmsTransactionSignature vmsTransactionSignature;

            Optional<Annotation> optionalInbound = Arrays.stream(annotations).filter(p -> p.annotationType() == Inbound.class ).findFirst();

            if (optionalInbound.isEmpty()){
                throw new QueueMappingException("Error mapping. A transactional method must be mapped to one or more input queues.");
            }

            Optional<Annotation> optionalOutbound = Arrays.stream(annotations).filter( p -> p.annotationType() == Outbound.class ).findFirst();

            if(optionalOutbound.isEmpty()) {
                throw new QueueMappingException("Outbound annotation not found in transactional method");
            }

            // In the first design, the microservice cannot have two
            // different operations reacting to the same payload
            // In other words, one payload to an operation mapping.
            // But one can achieve it by having two operations reacting
            // to the same input payload
            String[] inputQueues = ((Inbound) optionalInbound.get()).values();
            String outputQueue = ((Outbound) optionalOutbound.get()).value();

            if (eventToQueueMap.get(outputType) == null) {
                eventToQueueMap.put(outputType, outputQueue);
                vmsTransactionSignature = new VmsTransactionSignature(obj, method, inputQueues, outputQueue);
            } else {
                throw new QueueMappingException("Error mapping. An payload type cannot be mapped to two or more output queues.");
            }

            for (int i = 0; i < inputQueues.length; i++) {

                if (queueToEventMap.get(inputQueues[i]) == null) {
                    queueToEventMap.put(inputQueues[i], inputTypes.get(i));
                } else if (queueToEventMap.get(inputQueues[i]) != inputTypes.get(i)) {
                    throw new QueueMappingException("Error mapping. An input queue cannot be mapped to two or more payload types.");
                }

                List<IdentifiableNode<VmsTransactionSignature>> list = eventToVmsTransactionMap.get(inputQueues[i]);
                if (list == null) {
                    list = new ArrayList<>();
                    list.add( new IdentifiableNode<>(i, vmsTransactionSignature) );
                    eventToVmsTransactionMap.put(inputQueues[i], list);
                } else {
                    list.add(new IdentifiableNode<>(i, vmsTransactionSignature));
                }

            }

        }
    }

    private static DataType getColumnDataTypeFromAttributeType(Class<?> attributeType) throws NotAcceptableTypeException {
        String attributeCanonicalName = attributeType.getCanonicalName();
        if (attributeCanonicalName.equalsIgnoreCase("int") || attributeType == Integer.class){
            return DataType.INT;
        }
        else if (attributeCanonicalName.equalsIgnoreCase("float") || attributeType == Float.class){
            return DataType.FLOAT;
        }
        else if (attributeCanonicalName.equalsIgnoreCase("double") || attributeType == Double.class){
            return DataType.DOUBLE;
        }
        else if (attributeCanonicalName.equalsIgnoreCase("char") || attributeType == Character.class){
            return DataType.CHAR;
        }
        else if (attributeType == String.class){
            return DataType.STRING;
        }
        else if (attributeCanonicalName.equalsIgnoreCase("long") || attributeType == Long.class){
            return DataType.LONG;
        }
        else if (attributeType == Date.class){
            return DataType.DATE;
        }
        else {
            throw new NotAcceptableTypeException(attributeType.getCanonicalName() + " is not accepted");
        }
    }

}
