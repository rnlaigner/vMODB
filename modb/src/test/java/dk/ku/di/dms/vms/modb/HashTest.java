package dk.ku.di.dms.vms.modb;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import org.junit.Test;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class HashTest {

    @Test
    public void testPairKeyHash(){
        int max_ware = 1;
        int capacity = MemoryUtils.nextPowerOfTwo(max_ware*100_000);
        int mask = capacity - 1;
        int p = Integer.numberOfTrailingZeros(capacity);
        int p2 = Integer.numberOfTrailingZeros(100_000);
        int seed = 0x9E3779B9;
        Map<Integer, Integer> collisions1 = new TreeMap<>();
        Map<Integer, Integer> collisions2 = new TreeMap<>();

        for(int i = 1; i <= 100_000; i++){
            for(int j = 1; j <= max_ware; j++){
//                int hash = 31 * i + j;
                int hash = Objects.hash(i,j);
                int h = hash;
                h ^= h >>> 16;
                h *= 0x85ebca6b;
                h ^= h >>> 13;
                h *= 0xc2b2ae35;
                h ^= h >>> 16;

                // long idx1  = h & mask;
                int idx1 = (h * 0x9E3779B9) >>> (32 - p);

                if(collisions1.containsKey(idx1)){
                    collisions1.put(idx1, collisions1.get(idx1)+1);
                } else {
                    collisions1.put(idx1,1);
                }

                int idx2 = (hash * 0x9E3779B9) >>> (32 - p);

                if(collisions2.containsKey(idx2)){
                    collisions2.put(idx2, collisions2.get(idx2)+1);
                } else {
                    collisions2.put(idx2,1);
                }
            }
        }
        assert true;
        System.out.println("HEEEY");
    }

    @Test
    public void testTripleKeyHash(){
        int max_ware = 2;
        long capacity = MemoryUtils.nextPowerOfTwo(max_ware*10*100_000);
        long mask = capacity - 1;

        Map<Long, Integer> collisions1 = new TreeMap<>();
        Map<Long, Integer> collisions2 = new TreeMap<>();

        for(int i = 1; i <= 100_000; i++){
            for(int k = 1; k <= 10; k++) {
                for (int j = 1; j <= max_ware; j++) {
                    int hash = Objects.hash(i,k,j);
                    int h = hash;
                    h ^= h >>> 16;
                    h *= 0x85ebca6b;
                    h ^= h >>> 13;
                    h *= 0xc2b2ae35;
                    h ^= h >>> 16;

                    long idx1 = h & mask;
                    if (collisions1.containsKey(idx1)) {
                        collisions1.put(idx1, collisions1.get(idx1) + 1);
                    } else {
                        collisions1.put(idx1, 1);
                    }

                    int p = Long.numberOfTrailingZeros(capacity);
                    long idx2 = (((hash * 2654435761L) >>> (32 - p)));
                    if (collisions2.containsKey(idx2)) {
                        collisions2.put(idx2, collisions2.get(idx2) + 1);
                    } else {
                        collisions2.put(idx2, 1);
                    }
                }
            }
        }
        assert true;
        System.out.println("HEEEY");
    }

}