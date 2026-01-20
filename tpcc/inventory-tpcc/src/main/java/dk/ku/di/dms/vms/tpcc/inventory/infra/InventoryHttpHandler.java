package dk.ku.di.dms.vms.tpcc.inventory.infra;

import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import dk.ku.di.dms.vms.tpcc.common.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Item;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Stock;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IItemRepository;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IStockRepository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils.makeAlphaString;
import static dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils.randomNumber;
import static java.lang.System.Logger.Level.*;

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
        this.transactionManager.beginTransaction(Long.MAX_VALUE, 0, 0,false);
        List<Item> items = this.itemRepository.getAll();
        List<Stock> stockItems = this.stockRepository.getAll();
        LOGGER.log(INFO, "Inventory init reset");
        this.transactionManager.reset();

        LOGGER.log(INFO, "Warehouse GC triggered.");
        System.gc();
        LOGGER.log(INFO, "Warehouse GC finished.");

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
        // this.transactionManager.commit();
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
    public void put(String uri, String payload) {
        final String[] uriSplit = uri.split("/");
        String op = uriSplit[uriSplit.length - 1];
        if(op.contentEquals("load")){
            // path: /inventory/load
            this.transactionManager.rebuildIndexes();
            return;
        }

        int numWare = Integer.parseInt(ConfigUtils.loadProperties().getProperty("num_ware"));
        boolean checkpointing = Boolean.parseBoolean(ConfigUtils.loadProperties().getProperty("checkpointing"));
        this.transactionManager.reset();

        ForkJoinPool pool = ForkJoinPool.commonPool();
        Future<?>[] futures = new Future[numWare+1];

        LOGGER.log(INFO, "Populating inventory VMS...");
        long initTs = System.currentTimeMillis();

        if(checkpointing) {
            // bypass default interfaces
            this.populateDisk(numWare, futures, pool);
        } else {
            this.populateInMemory(numWare, futures, pool);
        }

        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Finished populating stock VMS in "+(endTs-initTs)+" ms");
    }

    @SuppressWarnings("unchecked")
    private void populateDisk(int numWare, Future<?>[] futures, ForkJoinPool pool) {

        final var itemRepo = ((AbstractProxyRepository<Integer, Item>) itemRepository);
        final var itemIndex = itemRepo.getTable().underlyingPrimaryKeyIndex();

        final var stockRepo = ((AbstractProxyRepository<Stock.StockId, Stock>) stockRepository);
        final var stockIndex = stockRepo.getTable().underlyingPrimaryKeyIndex();

        // item
        futures[0] = pool.submit(() -> {
            LOGGER.log(DEBUG, "Creating "+TPCcConstants.NUM_ITEMS+" item records...");
            long internalInitTs = System.currentTimeMillis();
            for (int i_id = 1; i_id <= TPCcConstants.NUM_ITEMS; i_id++) {
                Item item = generateItem(i_id);
                Object[] itemObj = itemRepo.extractFieldValuesFromEntityObject(item);
                IKey itemKey = KeyUtils.buildRecordKey(itemIndex.schema().getPrimaryKeyColumns(), itemObj);
                itemIndex.insert(itemKey, itemObj);
            }
            LOGGER.log(DEBUG, "Finished creating "+TPCcConstants.NUM_ITEMS+" item records in "+(System.currentTimeMillis()-internalInitTs)+" ms");
        });

        // stock
        for(int w_id = 1; w_id <= numWare; w_id++) {
            final int f_w_id = w_id;
            futures[w_id] = pool.submit(() -> {
                LOGGER.log(INFO, "Started creating "+TPCcConstants.NUM_ITEMS+" stock records for warehouse "+f_w_id);
                long internalInitTs = System.currentTimeMillis();
                for (int i_id = 1; i_id <= TPCcConstants.NUM_ITEMS; i_id++) {
                    Stock stock = generateStockItem(f_w_id, i_id);
                    Object[] stockObj = stockRepo.extractFieldValuesFromEntityObject(stock);
                    IKey stockKey = KeyUtils.buildRecordKey( stockIndex.schema().getPrimaryKeyColumns(), stockObj );
                    synchronized (stockIndex) {
                        stockIndex.insert(stockKey, stockObj);
                    }
                }
                LOGGER.log(INFO, "Finished creating "+TPCcConstants.NUM_ITEMS+" stock records for warehouse "+f_w_id+" in "+(System.currentTimeMillis()-internalInitTs)+" ms");
            });
        }
        try {
            for (int w_id = 0; w_id <= numWare; w_id++) {
                futures[w_id].get();
            }
            futures[0] = pool.submit(itemIndex::flush);
            futures[1] = pool.submit(stockIndex::flush);
            for (int i = 0; i < 2; i++) {
                futures[i].get();
            }
            this.transactionManager.rebuildIndexes();
        } catch(ExecutionException | InterruptedException e){
            LOGGER.log(ERROR, "Error:\n"+e);
        }
    }

    private void populateInMemory(int numWare, Future<?>[] futures, ForkJoinPool pool) {
        LOGGER.log(DEBUG, "Creating "+TPCcConstants.NUM_ITEMS+" item records...");
        long initTs = System.currentTimeMillis();
        long lastTid = -numWare-1;
        // no need to set lastTid here because there will be no FK check or query
        this.transactionManager.beginTransaction(lastTid, 0, 0, false);
        // item
        for(int i_id = 1; i_id <= TPCcConstants.NUM_ITEMS; i_id++){
            Item item = generateItem(i_id);
            this.itemRepository.insert(item);
        }
        // this.transactionManager.commit();

        long endTs = System.currentTimeMillis();
        LOGGER.log(DEBUG, "Finished creating "+TPCcConstants.NUM_ITEMS+" item records in "+(endTs-initTs)+" ms");

        // stock
        for(int w_id = 1; w_id <= numWare; w_id++) {
            final int f_w_id = w_id;
            futures[w_id-1] = pool.submit(() -> {
                LOGGER.log(DEBUG, "Started creating "+TPCcConstants.NUM_ITEMS+" stock records for warehouse "+f_w_id);
                transactionManager.beginTransaction(-f_w_id, 0, lastTid, false);
                long internalInitTs = System.currentTimeMillis();
                for (int i_id = 1; i_id <= TPCcConstants.NUM_ITEMS; i_id++) {
                    Stock stock = generateStockItem(f_w_id, i_id);
                    stockRepository.insert(stock);
                }
                // transactionManager.commit();
                LOGGER.log(DEBUG, "Finished creating "+TPCcConstants.NUM_ITEMS+" stock records for warehouse "+f_w_id+" in "+(System.currentTimeMillis()-internalInitTs)+" ms");
            });
        }

        try {
            for (int w_id = 1; w_id <= numWare; w_id++) {
                futures[w_id-1].get();
            }
        } catch(ExecutionException | InterruptedException e){
            LOGGER.log(ERROR, "Error:\n"+e);
        }
    }

    public static Item generateItem(int I_ID) {
        int I_IM_ID = randomNumber(1, 10000);
        String I_NAME = makeAlphaString(14, 24);
        float I_PRICE = (float) ((randomNumber(100, 10000)) / 100.0);
        String I_DATA = makeAlphaString(26, 50);
        return new Item(I_ID, I_IM_ID, I_PRICE, I_NAME, I_DATA);
    }

    public static Stock generateStockItem(int w_id, int S_I_ID) {
        int S_QUANTITY = randomNumber(10, 100);
        Map<Integer, String> S_DIST = new HashMap<>();
        for (int d = 1; d <= TPCcConstants.NUM_DIST_PER_WARE; d++) S_DIST.put(d, makeAlphaString(24, 24));
        int S_YTD = 0;
        int S_ORDER_CNT = 0;
        int S_REMOTE_CNT = 0;
        String S_DATA = makeAlphaString(26, 50);
        return new Stock(S_I_ID, w_id, S_QUANTITY, S_DIST, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA);
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
