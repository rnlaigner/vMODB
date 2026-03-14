package dk.ku.di.dms.vms.modb.query.execution.operators.count;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyBufferIndex;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * No projecting any other column for now
 * To make reusable, internal state must be made ephemeral
 */
public class IndexCountDistinct extends AbstractCount {

    private static class EphemeralState {
        private int count;
        // hashed by the values in the distinct clause
        private final Map<Integer, Integer> valuesSeen;

        private EphemeralState() {
            this.count = 0;
            this.valuesSeen = new HashMap<>();
        }
    }

    public IndexCountDistinct(ReadOnlyBufferIndex<IKey> index) {
        super(index, Integer.BYTES);
    }

    /**
     * Can be reused across different distinct columns
     */
    public MemoryRefNode run(int distinctColumnIndex, FilterContext filterContext, IKey... keys){
        EphemeralState state = new EphemeralState();
        Iterator<IKey> iterator = this.index.iterator(keys);
        while(iterator.hasNext()){
            IKey key = iterator.next();
            if(this.index.checkCondition(key, filterContext)){
                Object val = this.index.record(key)[distinctColumnIndex];
                if(!state.valuesSeen.containsKey(val.hashCode())) {
                    state.count++;
                    state.valuesSeen.put(val.hashCode(), 1);
                }
            }
            iterator.next();
        }
        this.append(state.count);
        return memoryRefNode;
    }

}
