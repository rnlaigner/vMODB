package dk.ku.di.dms.vms.sdk.core.metadata;

import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.modb.api.enums.ExecutionModeEnum;
import dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.api.query.parser.Parser;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.constraint.ConstraintEnum;
import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.constraint.ForeignKeyReference;
import dk.ku.di.dms.vms.modb.common.constraint.ValueConstraintReference;
import dk.ku.di.dms.vms.modb.common.data_structure.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.core.metadata.exception.NoPrimaryKeyFoundException;
import dk.ku.di.dms.vms.sdk.core.metadata.exception.QueueMappingException;
import dk.ku.di.dms.vms.sdk.core.metadata.exception.UnsupportedConstraint;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;
import org.reflections.Configuration;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.validation.constraints.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

public final class VmsMetadataLoader {

    private VmsMetadataLoader(){}

    private static final System.Logger LOGGER = System.getLogger(VmsMetadataLoader.class.getName());

    /**
     * For tests that do not involve repositories (i.e., application code execution)
     */
    public static VmsRuntimeMetadata load(String... packages)
            throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Reflections reflections = configureReflections(packages);
        Set<Class<?>> vmsClasses = reflections.getTypesAnnotatedWith(Microservice.class);
        if(vmsClasses.isEmpty()) throw new IllegalStateException("No classes annotated with @Microservice in this application.");
        Map<Class<?>, String> entityToTableNameMap = loadVmsTableNames(reflections);
        Map<Class<?>, String> entityToVirtualMicroservice = mapEntitiesToVirtualMicroservice(vmsClasses, entityToTableNameMap);
        Map<String, VmsDataModel> vmsDataModelMap = buildVmsDataModel( entityToVirtualMicroservice, entityToTableNameMap );
        String packageName = ConfigUtils.getCallerPackage();
        return load(reflections, packageName, vmsClasses, vmsDataModelMap, Map.of(), Map.of());
    }

    public static VmsRuntimeMetadata load(Reflections reflections,
                                          String packageName,
                                          Set<Class<?>> vmsClasses,
                                          Map<String, VmsDataModel> vmsDataModelMap,
                                          Map<String, List<Object>> vmsToRepositoriesMap,
                                          Map<String, Object> tableToRepositoryMap)
            throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException {

        // also load the corresponding repository facade
        Map<String, Object> loadedVmsInstances = loadMicroserviceClasses(vmsClasses, vmsToRepositoriesMap);

        // necessary remaining data structures to store a vms metadata
        Map<String, VmsTransactionMetadata> queueToVmsTransactionMap = new HashMap<>();

        // event to type
        Map<String, Class<?>> queueToEventMap = new HashMap<>();

        // type to event
        Map<Class<?>, String> eventToQueueMap = new HashMap<>();

        // distinction between input and output events
        // key: queue name; value: (input),(output)
        Map<String, String> inputOutputEventDistinction = new HashMap<>();

        Set<Method> transactionalMethods = reflections
                .getMethodsAnnotatedWith(Transactional.class)
                .stream()
                .filter(p->p.getDeclaringClass().getPackageName().equals(packageName))
                .collect(Collectors.toSet());

        mapVmsMethodInputOutput(transactionalMethods,
                loadedVmsInstances, queueToEventMap, eventToQueueMap,
                inputOutputEventDistinction, queueToVmsTransactionMap);

        Map<String, VmsEventSchema> inputEventSchemaMap = new HashMap<>();
        Map<String, VmsEventSchema> outputEventSchemaMap = new HashMap<>();

        Set<Class<?>> eventsClazz = reflections.getTypesAnnotatedWith(Event.class);
        buildEventSchema(eventsClazz,
                eventToQueueMap,
                inputOutputEventDistinction,
                inputEventSchemaMap,
                outputEventSchemaMap);

        Map<String, String> clazzNameToVmsNameMap = mapClazzNameToVmsName(vmsClasses);

        return new VmsRuntimeMetadata(
                vmsDataModelMap,
                inputEventSchemaMap,
                outputEventSchemaMap,
                queueToVmsTransactionMap,
                queueToEventMap,
                eventToQueueMap,
                clazzNameToVmsNameMap,
                loadedVmsInstances,
                tableToRepositoryMap);
    }

    private static Map<String, String> mapClazzNameToVmsName(Set<Class<?>> vmsClasses) {
        Map<String, String> map = new HashMap<>();
        for(Class<?> clazz : vmsClasses) {
            var anno = clazz.getAnnotation(Microservice.class);
            map.put(clazz.getCanonicalName(), anno.value());
        }
        return map;
    }

    public static Reflections configureReflections(String[] packages){

        if(packages == null) {
            // https://stackoverflow.com/questions/67159160/java-using-reflections-to-scan-classes-from-all-packages
            throw new IllegalStateException("No package to scan.");
        }

        Collection<URL> urls = new ArrayList<>(packages.length);
        for(String package_ : packages){
            urls.addAll( ClasspathHelper.forPackage(package_) );
        }

        Configuration reflectionsConfig = new ConfigurationBuilder()
                .setUrls(urls)
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
    public static Map<Class<?>, String> loadVmsTableNames(Reflections reflections) {
        Set<Class<?>> vmsTables = reflections.getTypesAnnotatedWith(VmsTable.class);
        Map<Class<?>, String> vmsTableNameMap = new HashMap<>();
        for(Class<?> vmsTable : vmsTables){
            Optional<Annotation> optionalVmsTableAnnotation = Arrays.stream(vmsTable.getAnnotations())
                    .filter(p -> p.annotationType() == VmsTable.class).findFirst();
            optionalVmsTableAnnotation.ifPresent(
                    annotation -> vmsTableNameMap.put(vmsTable, ((VmsTable) annotation).name()));
        }
        return vmsTableNameMap;
    }

    /**
     * Building virtual microservice event schemas
     */
    private static void buildEventSchema(
            Set<Class<?>> eventsClazz,
            Map<Class<?>, String> eventToQueueMap,
            Map<String, String> inputOutputEventDistinction,
            Map<String, VmsEventSchema> inputEventSchemaMap,
            Map<String, VmsEventSchema> outputEventSchemaMap) {

        for(Class<?> eventClazz : eventsClazz){

            if(eventToQueueMap.get(eventClazz) == null) continue; // ignored the ones not mapped

            Field[] fields = eventClazz.getDeclaredFields();

            String[] columnNames = new String[fields.length];
            DataType[] columnDataTypes = new DataType[fields.length];
            int i = 0;

            for(Field field : fields){
                Class<?> attributeType = field.getType();
                columnDataTypes[i] = getEventDataTypeFromAttributeType(attributeType);
                columnNames[i] = field.getName();
                i++;
            }

            // get queue name
            String queue = eventToQueueMap.get( eventClazz );
            if(queue == null){
                LOGGER.log(WARNING, "Cannot find the queue of an event type found in this project: "+eventClazz);
                continue;
            }

            // is input?
            String category = inputOutputEventDistinction.get(queue);
            if(category.equalsIgnoreCase("input")){
                inputEventSchemaMap.put( queue, new VmsEventSchema( queue, columnNames, columnDataTypes ) );
            } else if(category.equalsIgnoreCase("output")) {
                outputEventSchemaMap.put( queue, new VmsEventSchema( queue, columnNames, columnDataTypes ) );
            } else {
                throw new IllegalStateException("Queue cannot be distinguished between input or output.");
            }

        }

    }

    /**
     * Building virtual microservice table schemas
     * Map key is the table name
     */
    @SuppressWarnings("unchecked")
    public static Map<String, VmsDataModel> buildVmsDataModel(Map<Class<?>, String> entityToVirtualMicroservice,
                                                                 Map<Class<?>, String> vmsTableNames) {
        Map<String, VmsDataModel> dataModelMap = new HashMap<>();

        // build schema of each table
        // we build the schema in order to look up the fields and define the pk hash index
        for(Map.Entry<Class<?>, String> entry : vmsTableNames.entrySet()){

            Class<? extends IEntity<?>> tableClass = (Class<? extends IEntity<?>>) entry.getKey();

            Stream<Field> stream = Arrays.stream(tableClass.getFields());

            List<Field> pkFields = stream.filter(p -> p.getAnnotation(Id.class) != null).toList();

            if(pkFields.isEmpty()){
                throw new NoPrimaryKeyFoundException("Table class "+tableClass.getCanonicalName()+" does not have a primary key.");
            }
            int totalNumberOfFields = pkFields.size();

            stream = Arrays.stream(tableClass.getFields());
            List<Field> foreignKeyFields = stream.filter(p-> p.getAnnotation(VmsForeignKey.class) != null).toList();

            stream = Arrays.stream(tableClass.getFields());
            List<Field> columnFields = stream.filter(p-> p.getAnnotation(Column.class) != null).toList();

            if(!columnFields.isEmpty()) {
                totalNumberOfFields += columnFields.size();
            }

            List<String> columnNames = new ArrayList<>();
            List<DataType> columnDataTypes = new ArrayList<>();

            int[] pkFieldsStr = new int[pkFields.size()];

            // iterating over pk columns
            int i = 0;
            for(Field field : pkFields){
                Class<?> attributeType = field.getType();
                columnDataTypes.add( DataTypeUtils.getColumnDataTypeFromAttributeType(attributeType) );
                pkFieldsStr[i] = i;
                columnNames.add( field.getName() );
                i++;
            }

            ForeignKeyReference[] foreignKeyReferences = null;
            if(!foreignKeyFields.isEmpty()) {
                foreignKeyReferences = new ForeignKeyReference[foreignKeyFields.size()];
                int j = 0;
                // iterating over association columns
                for (Field field : foreignKeyFields) {
                    VmsForeignKey fk = field.getAnnotation(VmsForeignKey.class);
                    String fkTable = vmsTableNames.get( fk.table() );
                    // later we parse into a Vms Table and check whether the types match
                    foreignKeyReferences[j] = new ForeignKeyReference(fkTable, fk.column(), field.getName());
                    j++;

                    // a fk must be either a column or an id
                    Id id = field.getAnnotation(Id.class);
                    Column column = field.getAnnotation(Column.class);
                    if(id == null && column == null){
                        throw new RuntimeException("Foreign key "+fk.column()+" to table "+fk.table()+" is not marked as Id or Column in "+entry.getValue());
                    }
                }

            }

            String[] columnNamesArray = new String[totalNumberOfFields];
            columnNames.toArray(columnNamesArray);

            DataType[] columnDataTypesArray = new DataType[totalNumberOfFields];
            columnDataTypes.toArray(columnDataTypesArray);

            // regular columns
            ConstraintReference[] constraints = processRegularColumns(columnFields, columnNamesArray, columnDataTypesArray, i);

            VmsTable vmsTableAnnotation = tableClass.getAnnotation(VmsTable.class);
            String vmsTableName = vmsTableAnnotation.name();
            String vms = entityToVirtualMicroservice.get( tableClass );
            VmsDataModel schema = new VmsDataModel(vms, vmsTableName, pkFieldsStr, columnNamesArray, columnDataTypesArray, foreignKeyReferences, constraints);
            dataModelMap.put(vmsTableName, schema);
        }
        return dataModelMap;
    }

    /**
     * non-foreign key column constraints are inherent to the table, not referring to other tables
     */
    private static ConstraintReference[] processRegularColumns(List<Field> columnFields, String[] columnNames, DataType[] columnDataTypes, int columnPosition) throws UnsupportedConstraint {
        if(columnFields == null) {
            return null;
        }

        ConstraintReference[] constraints = null;

        // iterating over non-pk and non-fk columns;
        for (Field field : columnFields) {

            Class<?> attributeType = field.getType();
            columnDataTypes[columnPosition] = DataTypeUtils.getColumnDataTypeFromAttributeType(attributeType);

            // get constraints ought to be applied to this column, e.g., non-negative, not null, nullable
            List<Annotation> constraintAnnotations = Arrays.stream(field.getAnnotations())
                    .filter(p -> p.annotationType() == Positive.class ||
                            p.annotationType() == PositiveOrZero.class ||
                            p.annotationType() == NotNull.class ||
                            p.annotationType() == Null.class ||
                            p.annotationType() == Negative.class ||
                            p.annotationType() == NegativeOrZero.class ||
                            p.annotationType() == Min.class ||
                            p.annotationType() == Max.class ||
                            p.annotationType() == NotBlank.class
                    ).toList();

            constraints = new ConstraintReference[constraintAnnotations.size()];
            int nC = 0;
            for (Annotation constraint : constraintAnnotations) {
                String constraintName = constraint.annotationType().getName();
                switch (constraintName) {
                    case "javax.validation.constraints.PositiveOrZero" ->
                            constraints[nC] = new ConstraintReference(ConstraintEnum.POSITIVE_OR_ZERO, columnPosition);
                    case "javax.validation.constraints.Positive" ->
                            constraints[nC] = new ConstraintReference(ConstraintEnum.POSITIVE, columnPosition);
                    case "javax.validation.constraints.Null" ->
                            constraints[nC] = new ConstraintReference(ConstraintEnum.NULL, columnPosition);
                    case "javax.validation.constraints.NotNull" ->
                            constraints[nC] = new ConstraintReference(ConstraintEnum.NOT_NULL, columnPosition);
                    case "javax.validation.constraints.Negative" ->
                            constraints[nC] = new ConstraintReference(ConstraintEnum.NEGATIVE, columnPosition);
                    case "javax.validation.constraints.NegativeOrZero" ->
                            constraints[nC] = new ConstraintReference(ConstraintEnum.NEGATIVE_OR_ZERO, columnPosition);
                    case "javax.validation.constraints.Min" -> {
                        long value = ((Min)constraint).value();
                        if(value == 0){
                            constraints[nC] = new ConstraintReference(ConstraintEnum.POSITIVE_OR_ZERO, columnPosition);
                        } else if(value == 1){
                            constraints[nC] = new ConstraintReference(ConstraintEnum.POSITIVE, columnPosition);
                        } else {
                            constraints[nC] = new ValueConstraintReference(ConstraintEnum.MIN, columnPosition, value);
                        }
                    }
                    case "javax.validation.constraints.Max" -> {
                        long value = ((Max)constraint).value();
                        if(value == 0){
                            constraints[nC] = new ConstraintReference(ConstraintEnum.NEGATIVE_OR_ZERO, columnPosition);
                        } else {
                            constraints[nC] = new ValueConstraintReference(ConstraintEnum.MAX, columnPosition, value);
                        }
                    }
                    case "javax.validation.constraints.NotBlank" ->
                            constraints[nC] = new ConstraintReference(ConstraintEnum.NOT_BLANK, columnPosition);
                    default -> throw new UnsupportedConstraint("Constraint currently " + constraintName + " not supported.");
                }
                nC++;
            }
            columnNames[columnPosition] = field.getName();
            columnPosition++;
        }
        return constraints;
    }

    public static Map<Class<?>, String> mapEntitiesToVirtualMicroservice(Set<Class<?>> vmsClasses, Map<Class<?>, String> entityToTableNameMap) {
        Map<Class<?>, String> entityToVirtualMicroservice = new HashMap<>();
        for(Class<?> clazz : vmsClasses) {
            String vmsName = clazz.getAnnotation(Microservice.class).value();
            // fast way since it is usually one per app
            for(var entry : entityToTableNameMap.entrySet()){
                entityToVirtualMicroservice.put(entry.getKey(), vmsName);
            }
        }
        return entityToVirtualMicroservice;
    }

    private static Map<String, Object> loadMicroserviceClasses(
            Set<Class<?>> vmsClasses,
            Map<String, List<Object>> repositoryClassMap)
            throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException {

        Map<String, Object> loadedMicroserviceInstances = new HashMap<>();
        for(Class<?> clazz : vmsClasses) {
            // mapVmsTransactionInputOutput uses the canonical name
            String clazzName = clazz.getCanonicalName();
            Class<?> cls = Class.forName(clazzName);
            Constructor<?>[] constructors = cls.getDeclaredConstructors();
            Constructor<?> constructor = constructors[0];
            // these are the classes annotated with @Microservice
            Object vmsInstance;
            if(repositoryClassMap.containsKey(clazzName)) {
                vmsInstance = constructor.newInstance(repositoryClassMap.get(clazzName).toArray());
            } else {
                System.out.println("Class " + clazzName + " not found in repository class map. In other words, no repository mapped for "+clazzName);
                vmsInstance = constructor.newInstance();
            }
            loadedMicroserviceInstances.put(clazzName, vmsInstance);
        }
        return loadedMicroserviceInstances;
    }

    /**
     * Map transactions to input and output events
     */
    private static void mapVmsMethodInputOutput(Set<Method> transactionalMethods,
                                                Map<String, Object> loadedVmsInstances,
                                                Map<String, Class<?>> queueToEventMap,
                                                Map<Class<?>, String> eventToQueueMap,
                                                Map<String, String> inputOutputEventDistinction,
                                                Map<String, VmsTransactionMetadata> queueToVmsTransactionMap) {
        for (Method method : transactionalMethods) {

            String className = method.getDeclaringClass().getCanonicalName();
            Object obj = loadedVmsInstances.get(className);

            Class<?> outputType = getOutputType(method);

            List<Class<?>> inputTypes = new ArrayList<>();

            for(int i = 0; i < method.getParameters().length; i++){
                try{
                    Class<?> clazz = method.getParameters()[i].getType();
                    inputTypes.add( clazz);
                } catch(Exception e) {
                    throw new QueueMappingException("All input events must implement IEvent interface.");
                }
            }

            Annotation[] annotations = method.getAnnotations();

            VmsTransactionSignature vmsTransactionSignature;

            Optional<Annotation> optionalInbound = Arrays.stream(annotations).filter(p -> p.annotationType() == Inbound.class ).findFirst();

            if (optionalInbound.isEmpty()){
                throw new QueueMappingException("Error mapping. A transactional method must be mapped to one or more input queues.");
            }

            Optional<Annotation> optionalOutbound = Arrays.stream(annotations).filter( p -> p.annotationType() == Outbound.class ).findFirst();

            String outputQueue = null;
            if(optionalOutbound.isPresent()) {
                // throw new QueueMappingException("Outbound annotation not found in transactional method");
                outputQueue = ((Outbound) optionalOutbound.get()).value();
                String queueName = eventToQueueMap.get(outputType);
                if (queueName == null) {
                    eventToQueueMap.put(outputType, outputQueue);

                    // why: to make sure we always map one type to one queue
                    if(queueToEventMap.get(outputQueue) == null){
                        queueToEventMap.put(outputQueue, outputType);
                    } else {
                        throw new QueueMappingException(
                                "Error mapping: An output queue cannot be mapped to two (or more) types.");
                    }

                    inputOutputEventDistinction.put(outputQueue, "output");

                } else if(queueToEventMap.get(queueName) != outputType) {
                    throw new QueueMappingException(
                            "Error mapping: A payload type cannot be mapped to two (or more) output queues.");
                }
            }

            // In the first design, the microservice cannot have two
            // different operations reacting to the same payload
            // In other words, one payload to an operation mapping.
            // But one can achieve it by having two operations reacting
            // to the same input payload
            String[] inputQueues = ((Inbound) optionalInbound.get()).values();

            // check whether the input queue contains commas
            if(Arrays.stream(inputQueues).anyMatch(p->p.contains(","))){
                throw new IllegalStateException("Cannot contain comma in input queue definition");
            }

            Optional<Annotation> optionalTransactional = Arrays.stream(annotations).filter(p -> p.annotationType() == Transactional.class ).findFirst();

            // default
            TransactionTypeEnum transactionType = TransactionTypeEnum.RW;
            if(optionalTransactional.isPresent()) {
                Annotation ann = optionalTransactional.get();
                transactionType = ((Transactional) ann).type();
            }

            // default
            ExecutionModeEnum executionMode = ExecutionModeEnum.SINGLE_THREADED;

            Optional<Annotation> optionalPartitionBy = Arrays.stream(annotations).filter(p -> p.annotationType() == PartitionBy.class ).findFirst();
            if(optionalPartitionBy.isPresent()) {
                executionMode = ExecutionModeEnum.PARTITIONED;
                Class<?> inputClazz = ( (PartitionBy)optionalPartitionBy.get() ).clazz();
                String partitionMethodStr = ( (PartitionBy)optionalPartitionBy.get() ).method();
                try {
                    Method partitionMethod = inputClazz.getMethod(partitionMethodStr);
                    vmsTransactionSignature = new VmsTransactionSignature(obj, method, transactionType, executionMode, partitionMethod, inputQueues, outputQueue);
                } catch (NoSuchMethodException e) {
                    // leave as single threaded
                    vmsTransactionSignature = new VmsTransactionSignature(obj, method, transactionType, executionMode, inputQueues, outputQueue);
                }
            } else {

                Optional<Annotation> optionalParallel = Arrays.stream(annotations).filter(p -> p.annotationType() == Parallel.class).findFirst();
                if (optionalParallel.isPresent()) executionMode = ExecutionModeEnum.PARALLEL;

                vmsTransactionSignature = new VmsTransactionSignature(obj, method, transactionType, executionMode, inputQueues, outputQueue);
            }

            for (int i = 0; i < inputQueues.length; i++) {

                if (queueToEventMap.get(inputQueues[i]) == null) {
                    queueToEventMap.put(inputQueues[i], inputTypes.get(i));

                    // in order to build the event schema
                    eventToQueueMap.put(inputTypes.get(i), inputQueues[i]);

                    inputOutputEventDistinction.put(inputQueues[i],"input");

                } else if (queueToEventMap.get(inputQueues[i]) != inputTypes.get(i)) {
                    throw new QueueMappingException("Error mapping: An input queue cannot be mapped to two or more payload types.");
                }

                VmsTransactionMetadata transactionMetadata = queueToVmsTransactionMap.get(inputQueues[i]);
                if (transactionMetadata == null) {
                    transactionMetadata = new VmsTransactionMetadata();
                    transactionMetadata.signatures.add( new IdentifiableNode<>(i, vmsTransactionSignature) );
                    queueToVmsTransactionMap.put(inputQueues[i], transactionMetadata);
                } else {
                    transactionMetadata.signatures.add(new IdentifiableNode<>(i, vmsTransactionSignature));
                }

                if(vmsTransactionSignature.inputQueues().length > 1){
                    transactionMetadata.numTasksWithMoreThanOneInput++;
                }

                switch (vmsTransactionSignature.transactionType()){
                    case RW -> transactionMetadata.numReadWriteTasks++;
                    case R -> transactionMetadata.numReadTasks++;
                    case W -> transactionMetadata.numWriteTasks++;
                }
            }
        }
    }

    private static Class<?> getOutputType(Method method) {
        Class<?> outputType;
        try{
            outputType = method.getReturnType();
        } catch(Exception e) {
            throw new QueueMappingException("All output events must implement IEvent interface.");
        }
        // output type cannot be String or primitive
        if((outputType.isPrimitive() && !outputType.getSimpleName().equals("void")) || outputType.isArray() || outputType.isInstance(String.class)){
            throw new IllegalStateException("Output type cannot be String, array, annotation, or primitive");
        }
        return outputType;
    }

    /**
     * Event types are not being used in ser/des of events
     * So it is ok to just return null for now.
     * Later, the event schema can be removed from the presentation payload
     */
    private static DataType getEventDataTypeFromAttributeType(Class<?> attributeType){
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
        else if (attributeCanonicalName.equalsIgnoreCase("long") || attributeType == Long.class){
            return DataType.LONG;
        }
        else if (attributeType == Date.class){
            return DataType.DATE;
        }
        else if(attributeType == String.class){
            return DataType.STRING;
        }
        else if(attributeType == int[].class){
            return DataType.INT_ARRAY;
        }
        else if(attributeType == float[].class){
            return DataType.FLOAT_ARRAY;
        }
        else if(attributeType == String[].class){
            return DataType.STRING_ARRAY;
        }
        else {
            LOGGER.log(DEBUG, attributeCanonicalName + " will be recognized as a complex data type");
            return DataType.COMPLEX;
        }
    }

    public static Map<String, SelectStatement> loadStaticQueries(Method[] queryMethods){
        Map<String, SelectStatement> res = new HashMap<>(queryMethods.length);
        for(Method queryMethod : queryMethods){
            try {
                Optional<Annotation> annotation = Arrays.stream(queryMethod.getAnnotations())
                        .filter( a -> a.annotationType() == Query.class).findFirst();

                if(annotation.isEmpty()) continue;

                String queryString = ((Query)annotation.get()).value();

                // build the query now. simple parser only
                SelectStatement selectStatement = Parser.parse(queryString);
                selectStatement.SQL.append(queryString);

                res.put(queryMethod.getName(), selectStatement);

            } catch(Exception e){
                throw new RuntimeException("Error on processing the query annotation: "+e.getMessage());
            }
        }
        return res;
    }

}
