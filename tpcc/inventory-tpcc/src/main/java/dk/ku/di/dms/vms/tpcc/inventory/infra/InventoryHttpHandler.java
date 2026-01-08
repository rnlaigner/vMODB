package dk.ku.di.dms.vms.tpcc.inventory.infra;

import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Item;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Stock;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IItemRepository;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IStockRepository;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.System.Logger.Level.INFO;

public final class InventoryHttpHandler extends DefaultHttpHandler {

    private final IItemRepository itemRepository;

    private final IStockRepository stockRepository;

    public InventoryHttpHandler(ITransactionManager transactionManager,
                                IItemRepository itemRepository,
                                IStockRepository stockRepository) {
        super(transactionManager);
        this.itemRepository = itemRepository;
        this.stockRepository = stockRepository;
    }

    @Override
    public void patch(String uri, String body) {
        final String[] uriSplit = uri.split("/");
        String op = uriSplit[uriSplit.length - 1];
        if(op.contentEquals("reset")){
            // path: /inventory/reset
            this.transactionManager.reset();
            return;
        }
        // path: /inventory/cleanup
        LOGGER.log(INFO, "Inventory init cleanup");

        LOGGER.log(INFO, "Warehouse GC triggered.");
        System.gc();
        LOGGER.log(INFO, "Warehouse GC finished.");

        List<Item> items = this.itemRepository.getAll();
        List<Stock> stockItems = this.stockRepository.getAll();
        LOGGER.log(INFO, "Inventory init reset");
        this.transactionManager.reset();
        LOGGER.log(INFO, "Inventory tables reset");
        this.transactionManager.beginTransaction(0, 0, 0,false);
        for(Stock stockItem : stockItems){
            stockItem.s_ytd = 0;
            stockItem.s_order_cnt = 0;
            stockItem.s_remote_cnt = 0;
            stockItem.s_quantity = ThreadLocalRandom.current().nextInt(10, 100);
        }
        this.itemRepository.insertAll(items);
        this.stockRepository.insertAll(stockItems);
        this.transactionManager.commit();
        LOGGER.log(INFO, "Inventory finished cleanup");
    }

    @Override
    public Object getAsJson(String uri) throws RuntimeException {
        String[] uriSplit = uri.split("/");
        String table = uriSplit.length > 1 ? uriSplit[1] : "";
        switch (table){
            case "item" -> {
                int itemId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
                this.transactionManager.beginTransaction(0, 0, 0, true);
                return this.itemRepository.lookupByKey(itemId);
            }
            case "stock" -> {
                int wareId = Integer.parseInt(uriSplit[uriSplit.length - 2]);
                int itemId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
                this.transactionManager.beginTransaction(0, 0, 0, true);
                return this.stockRepository.lookupByKey(new Stock.StockId( itemId, wareId ));
            }
            case null, default -> {
                LOGGER.log(System.Logger.Level.WARNING, "URI not recognized: "+uri);
                return "";
            }
        }
    }

    @Override
    public void post(String uri, String payload) {
        String[] uriSplit = uri.split("/");
        String table = uriSplit[uriSplit.length - 1];
        switch (table){
            case "item" -> {
                Item item = SERDES.deserialize(payload, Item.class);
                this.transactionManager.beginTransaction(0, 0, 0, false);
                this.itemRepository.upsert(item);
            }
            case "stock" -> {
                Stock stock = SERDES.deserialize(payload, Stock.class);
                this.transactionManager.beginTransaction(0, 0, 0, false);
                this.stockRepository.upsert(stock);
            }
        }
    }

}
