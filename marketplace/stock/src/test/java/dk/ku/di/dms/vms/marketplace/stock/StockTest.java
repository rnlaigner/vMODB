package dk.ku.di.dms.vms.marketplace.stock;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.events.ProductUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.marketplace.common.inputs.CustomerCheckout;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;

import static dk.ku.di.dms.vms.marketplace.common.Constants.PRODUCT_UPDATED;
import static dk.ku.di.dms.vms.marketplace.common.Constants.RESERVE_STOCK;
import static java.lang.Thread.sleep;

public class StockTest {

    private static VmsApplication getVmsApplication() throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build("localhost", Constants.STOCK_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.stock",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        return VmsApplication.build(options);
    }

    /**
     *  Add sellers first to avoid foreign key constraint violation
     */
    private static void insertStockItems(VmsApplication vms) {
        insertStockItems(vms, 10, 1);
    }

    @SuppressWarnings("unchecked")
    private static void insertStockItems(VmsApplication vms, int numSellers, int productId) {
        var stockTable = vms.getTable("stock_items");
        var stockRepository = (AbstractProxyRepository<StockItem.StockId, StockItem>) vms.getRepositoryProxy("stock_items");

        for(int i = 1; i <= numSellers; i++){
            insertStockItems(stockTable, stockRepository, i, productId);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void insertStockItems(Table table, AbstractProxyRepository repository, int sellerId, int productId){
        var stock = new StockItem( sellerId, productId, 100000, 0, 0, 0,  "test", "1" );
        Object[] obj = repository.extractFieldValuesFromEntityObject(stock);
        IKey key = KeyUtils.buildRecordKey( table.schema().getPrimaryKeyColumns(), obj );
        table.underlyingPrimaryKeyIndex().insert(key, obj);
    }

    private static void generateProductUpdated(VmsApplication vms, int sellerId, int productId, int tid, int previousTid) {
        ProductUpdated productUpdated = new ProductUpdated(
                sellerId, productId,
                "test","test","test","test",1,1,"test",
                String.valueOf(tid));
        InboundEvent inboundEvent = new InboundEvent(tid, previousTid, 1,
                PRODUCT_UPDATED, ProductUpdated.class, productUpdated);
        vms.internalChannels().transactionInputQueue().add(inboundEvent);
    }

    @Test
    public void testIndependentPartitionExecution() throws Exception {
        var vms = getVmsApplication();
        vms.start();

        insertStockItems(vms);

        for(int i = 1; i <= 10; i++) {
            generateProductUpdated(vms, i, 1, i, i - 1);
        }

        sleep(2000);

        assert vms.lastTidFinished() == 10;
    }

    @Test
    public void testIndependentPartitionExecutionForDifferentProductIds() throws Exception {
        var vms = getVmsApplication();
        vms.start();

        // insert default, 10 sellers with product ID 1
        insertStockItems(vms);

        // insert product ID 2
        insertStockItems(vms, 10, 2);

        for(int i = 1; i <= 5; i++) {
            generateProductUpdated(vms, i, 1, i, i - 1);
        }
        for(int i = 6; i <= 10; i++) {
            generateProductUpdated(vms, i, 2, i, i - 1);
        }

        sleep(2000);

        assert vms.lastTidFinished() == 10;
    }

    /**
     * this test will force the scheduler to execute the five first transactions sequentially
     * and the next five in the same way
     * possible improvement: the case where some partitioned tasks intersect
     *  in this case, we can allow higher partitioned TIDs to proceed.
     *  in the scheduler, create a waiting queue of partitioned tasks
     *  if the partition is busy, just keeps triggering the subsequent tasks
     *  caution: cannot allow single thread or parallel execution
     *  until all partitioned tasks have executed.
     *  However, this would lead to a complicated code.
     *  It is easier to order the TIDs optimally in the coordinator.
     */
    @Test
    public void testCollisionPartitionExecution() throws Exception {
        var vms = getVmsApplication();
        vms.start();

        // insert default, 10 sellers with product ID 1
        insertStockItems(vms);

        // insert product ID 2
        insertStockItems(vms, 10, 2);

        for(int i = 1; i <= 5; i++) {
            generateProductUpdated(vms, 1, 1, i, i - 1);
        }
        for(int i = 6; i <= 10; i++) {
            generateProductUpdated(vms, 1, 2, i, i - 1);
        }

        sleep(2000);

        assert vms.lastTidFinished() == 10;
    }

    private static final BiFunction<Integer, String, CustomerCheckout> customerCheckoutFunction = (customerId, instanceId) -> new CustomerCheckout(
            customerId, "test", "test", "test", "test","test",
            "test", "test","test","test","test",
            "test", "test", "test", 1, instanceId
    );

    private static void generateReserveStock(VmsApplication vms, int sellerId, int productId, int tid, int previousTid){
        var instanceId = String.valueOf(sellerId);
        ReserveStock reserveStockEvent = new ReserveStock(
                new Date(), customerCheckoutFunction.apply( 1, instanceId ),
                List.of(
                        new CartItem(sellerId,productId,"test",
                                1.0f, 1.0f, 1, 1.0f, "1")
                ),
                instanceId
        );

        InboundEvent inboundEvent = new InboundEvent(tid, previousTid, 1,
                RESERVE_STOCK, ReserveStock.class, reserveStockEvent);
        vms.internalChannels().transactionInputQueue().add(inboundEvent);
    }

    @Test
    public void testMixedSingleThreadPartitionedExecution() throws Exception {

        var vms = getVmsApplication();
        vms.start();

        insertStockItems(vms, 15, 1);

        for(int i = 1; i <= 5; i++) {
            generateProductUpdated(vms, i, 1, i, i - 1);
        }

        for(int i = 6; i <= 10; i++) {
            generateReserveStock(vms, i, 1, i, i -1);
        }

        for(int i = 11; i <= 15; i++) {
            generateProductUpdated(vms, i, 1, i, i - 1);
        }

        sleep(2000);

        assert vms.lastTidFinished() == 15;

    }

}
