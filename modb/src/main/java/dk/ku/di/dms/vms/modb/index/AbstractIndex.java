package dk.ku.di.dms.vms.modb.index;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;

import java.util.HashSet;

/**
 * Base implementation of an index
 * @param <K> extends {@link IKey}
 * Inspiration from:
 * <a href="https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/core/memory/MemorySegment.java">...</a>
 * <a href="https://stackoverflow.com/questions/24026918/java-nio-bytebuffer-allocatedirect-size-limit-over-the-int">...</a>
 */
public abstract class AbstractIndex<K> implements ReadOnlyIndex<K> {

    protected final int[] columns;

    // to speed up queries, so the filters can be built on flight
    protected final HashSet<Integer> columnsHash;

    private final IIndexKey key;

    // respective table of this index
    protected final Schema schema;

    public AbstractIndex(Schema schema, int[] columnsIndex) {
        this.schema = schema;
        this.columns = columnsIndex;
        this.key = KeyUtils.buildIndexKey(columnsIndex);
        this.columnsHash = new HashSet<>(columns.length);
        for(int i : columnsIndex) this.columnsHash.add(i);
    }

    // use bitwise comparison to find whether a given index exists for such columns
    // https://stackoverflow.com/questions/8504288/java-bitwise-comparison-of-a-byte/8504393
    @Override
    public final int hashCode(){
        return this.key().hashCode();
    }

    @Override
    public final int[] columns(){
        return this.columns;
    }

    @Override
    public final boolean containsColumn(int columnPos) {
        return this.columnsHash.contains(columnPos);
    }

    @Override
    public final IIndexKey key(){
        return this.key;
    }

    @Override
    public final Schema schema(){
        return this.schema;
    }

}
