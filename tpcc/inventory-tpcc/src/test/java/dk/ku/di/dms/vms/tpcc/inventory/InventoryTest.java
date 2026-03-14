package dk.ku.di.dms.vms.tpcc.inventory;

import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Item;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Stock;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IStockRepository;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class InventoryTest {

    private static final int NUM_ITEMS = 10;

    private static VmsApplication getVmsApplication() throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build("localhost", 8002, new String[]{
                "dk.ku.di.dms.vms.tpcc.inventory",
                "dk.ku.di.dms.vms.tpcc.common"
        });
        return VmsApplication.build(options);
    }

    static VmsApplication VMS;

    @BeforeClass
    public static void setUp() throws Exception {
        VMS = getVmsApplication();
        VMS.start();
        insertStock();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        VMS.close();
        VMS = null;
    }

    @SuppressWarnings("unchecked")
    private static void insertStock() {
        var itemRepository = (AbstractProxyRepository<Integer, Item>) VMS.getRepositoryProxy("item");
        var stockRepository = (AbstractProxyRepository<Stock.StockId, Stock>) VMS.getRepositoryProxy("stock");
        VMS.getTransactionManager().beginTransaction(0, 0, 0, false);
        for (int j = 1; j <= NUM_ITEMS; j++) {
            Item item = new Item(j, j, 1.0f, "test_i_name", "test_i_data");
            Stock stock = new Stock(j, 1, j, Map.of(), 0, 0, 0, "test_s_data");
            itemRepository.insert(item);
            stockRepository.insert(stock);
            Assert.assertTrue(itemRepository.exists(j));
            Assert.assertTrue(stockRepository.exists(new Stock.StockId(j, 1)));
        }
        VMS.getTransactionManager().commit();
    }

    @Test
    public void testStockLevelQuery() {
        VMS.getTransactionManager().beginTransaction(0, 0, 0, true);
        IStockRepository stockRepository = (IStockRepository) VMS.getRepositoryProxy("stock");
        int[] itemIds = stockRepository.getStockCount(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 1, 10);
        Assert.assertEquals(9, itemIds.length);
        Assert.assertArrayEquals( new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, itemIds );
    }

}
