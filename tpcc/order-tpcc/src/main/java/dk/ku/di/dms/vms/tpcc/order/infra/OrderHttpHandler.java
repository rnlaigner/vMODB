package dk.ku.di.dms.vms.tpcc.order.infra;

import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.definition.key.composite.PairCompositeKey;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import dk.ku.di.dms.vms.tpcc.common.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.order.OrderService;
import dk.ku.di.dms.vms.tpcc.order.entities.NewOrder;
import dk.ku.di.dms.vms.tpcc.order.entities.Order;
import dk.ku.di.dms.vms.tpcc.order.entities.OrderLine;
import dk.ku.di.dms.vms.tpcc.order.repositories.IHistoryRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.INewOrderRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderLineRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderRepository;

import java.util.Date;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils.makeAlphaString;
import static dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils.randomNumber;
import static java.lang.System.Logger.Level.*;

public final class OrderHttpHandler extends DefaultHttpHandler {

    private final IOrderRepository orderRepository;
    private final INewOrderRepository newOrderRepository;
    private final IOrderLineRepository orderLineRepository;
    private final IHistoryRepository historyRepository;

    public OrderHttpHandler(ITransactionManager transactionManager,
                            IOrderRepository orderRepository,
                            INewOrderRepository newOrderRepository,
                            IOrderLineRepository orderLineRepository,
                            IHistoryRepository historyRepository) {
        super(transactionManager);
        this.orderRepository = orderRepository;
        this.newOrderRepository = newOrderRepository;
        this.orderLineRepository = orderLineRepository;
        this.historyRepository = historyRepository;
    }

    private static final SelectStatement selectStatementOrder = QueryBuilderFactory.select().project("*").from("orders").where("o_id", ExpressionTypeEnum.LESS_THAN_OR_EQUAL, 3000).build();
    private static final SelectStatement selectStatementNewOrder = QueryBuilderFactory.select().project("*").from("new_orders").where("no_o_id", ExpressionTypeEnum.LESS_THAN_OR_EQUAL, 3000).build();
    private static final SelectStatement selectStatementOrderLine = QueryBuilderFactory.select().project("*").from("order_line").where("ol_o_id", ExpressionTypeEnum.LESS_THAN_OR_EQUAL, 3000).build();

    @Override
    public void patch(String uri, String body) {
        final String[] uriSplit = uri.split("/");
        String op = uriSplit[uriSplit.length - 1];
        if(op.contentEquals("reset")){
            // path: /order/reset
            this.transactionManager.reset();
            return;
        }
        // path: /order/cleanup
        LOGGER.log(INFO, "Order init cleanup");

        this.transactionManager.beginTransaction(Long.MAX_VALUE, 0, 0,false);
        List<Order> orders = this.orderRepository.fetchMany(selectStatementOrder, Order.class);
        List<NewOrder> newOrders = this.newOrderRepository.fetchMany(selectStatementNewOrder, NewOrder.class);
        List<OrderLine> orderLines = this.orderLineRepository.fetchMany(selectStatementOrderLine, OrderLine.class);
        this.transactionManager.reset();

        LOGGER.log(INFO, "Order GC triggered.");
        System.gc();
        LOGGER.log(INFO, "Order GC finished.");

        LOGGER.log(INFO, "Order tables reset");

        this.transactionManager.beginTransaction(0, 0, 0,false);
        this.orderRepository.insertAll(orders);
        this.newOrderRepository.insertAll(newOrders);
        this.orderLineRepository.insertAll(orderLines);
        // this.transactionManager.commit();
        LOGGER.log(INFO, "Order finished cleanup");
    }

    @Override
    public Object getAsJson(String uri) throws RuntimeException {
        String[] uriSplit = uri.split("/");
        String table = uriSplit.length > 1 ? uriSplit[1] : "";
        switch (table){
            case "order" -> {
                int orderId = Integer.parseInt(uriSplit[uriSplit.length - 3]);
                int distId = Integer.parseInt(uriSplit[uriSplit.length - 2]);
                int wareId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
                this.transactionManager.beginTransaction(0, 0, 0, true);
                return this.orderRepository.lookupByKey(new Order.OrderId(orderId, distId, wareId));
            }
            case "history" -> {
                int id = Integer.parseInt(uriSplit[uriSplit.length - 1]);
                this.transactionManager.beginTransaction(0, 0, 0, true);
                return this.historyRepository.lookupByKey(id);
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
            // path: /order/load
            this.transactionManager.rebuildIndexes();
            return;
        }

        int numWare = Integer.parseInt(ConfigUtils.loadProperties().getProperty("num_ware"));
        boolean checkpointing = Boolean.parseBoolean(ConfigUtils.loadProperties().getProperty("checkpointing"));
        this.transactionManager.reset();

        ForkJoinPool pool = ForkJoinPool.commonPool();

        LOGGER.log(INFO, "Populating order VMS ("+numWare+" warehouses)...");
        long initTs = System.currentTimeMillis();

        if(checkpointing) {
            // bypass default interfaces
            this.populateDisk(numWare, pool);
        } else {
            this.populateInMemory(numWare, pool);
        }

        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Finished populating order VMS in "+(endTs-initTs)+" ms");
    }

    @SuppressWarnings("unchecked")
    private void populateDisk(int numWare, ForkJoinPool pool) {

        final var orderRepo = ((AbstractProxyRepository<Order.OrderId, Order>) orderRepository);
        final var orderIndex = orderRepo.getTable().underlyingPrimaryKeyIndex();

        final var newOrderRepo = ((AbstractProxyRepository<NewOrder.NewOrderId, NewOrder>) newOrderRepository);
        final var newOrderIndex = newOrderRepo.getTable().underlyingPrimaryKeyIndex();

        final var orderLineRepo = ((AbstractProxyRepository<OrderLine.OrderLineId, OrderLine>) orderLineRepository);
        final var orderLineIndex = orderLineRepo.getTable().underlyingPrimaryKeyIndex();

        Future<?>[] numWareFts = new Future[numWare];

        for(int w_id = 1; w_id <= numWare; w_id++){
            final int f_w_id = w_id;
            numWareFts[w_id-1] = pool.submit(() -> {
                LOGGER.log(INFO, "Started creating 30_000 customer records for warehouse " + f_w_id);
                long internalInitTs = System.currentTimeMillis();
                for (int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                    for (int o_id = 1; o_id <= TPCcConstants.NUM_CUST_PER_DIST; o_id++) {
                        int carrier_id = o_id < 2101 ? randomNumber(1, 10) : -1;
                        int ol_count = randomNumber(5, 15);

                        // order
                        Order order = new Order(o_id, d_id, f_w_id, randomNumber(1, 3000), new Date(), carrier_id, ol_count, 1);
                        Object[] orderObj = orderRepo.extractFieldValuesFromEntityObject(order);
                        IKey orderKey = KeyUtils.buildRecordKey( orderIndex.schema().getPrimaryKeyColumns(), orderObj );
                        synchronized (orderIndex) {
                            orderIndex.insert(orderKey, orderObj);
                        }

                        // new order
                        NewOrder newOrder = new NewOrder(o_id, d_id, f_w_id);
                        Object[] newOrderObj = newOrderRepo.extractFieldValuesFromEntityObject(newOrder);
                        IKey newOrderKey = KeyUtils.buildRecordKey( newOrderIndex.schema().getPrimaryKeyColumns(), orderObj );
                        synchronized (newOrderIndex) {
                            newOrderIndex.insert(newOrderKey, newOrderObj);
                        }

                        Date ol_delivery_d = o_id < 2101 ? new Date() : null;
                        float ol_amount = o_id < 2101 ? 0 : (float) (Math.floor((ThreadLocalRandom.current().nextDouble() * 9999.99) * 100) / 100.0);
                        for (int ol_id = 1; ol_id <= ol_count; ol_id++) {
                            OrderLine orderLine = new OrderLine(o_id, d_id, f_w_id, ol_id, randomNumber(1, TPCcConstants.NUM_ITEMS), f_w_id, ol_delivery_d, 5, ol_amount, makeAlphaString(26, 50));
                            Object[] orderLineObj = orderLineRepo.extractFieldValuesFromEntityObject(orderLine);
                            IKey orderLineKey = KeyUtils.buildRecordKey( orderLineIndex.schema().getPrimaryKeyColumns(), orderLineObj );
                            synchronized (orderLineIndex) {
                                orderLineIndex.insert(orderLineKey, orderLineObj);
                            }
                        }
                    }
                }
                LOGGER.log(INFO, "Finished creating 30_000 customer records for warehouse " + f_w_id + " in " + (System.currentTimeMillis() - internalInitTs) + " ms");
            });
        }
        try {
            for (int w_id = 1; w_id <= numWare; w_id++) {
                numWareFts[w_id-1].get();
            }

            Future<?>[] flushFts = new Future[2];
            flushFts[0] = pool.submit(orderIndex::flush);
            flushFts[1] = pool.submit(orderLineIndex::flush);
            for (int i = 0; i < 2; i++) {
                flushFts[i].get();
            }

            this.transactionManager.rebuildIndexes();
        } catch(ExecutionException | InterruptedException e){
            LOGGER.log(ERROR, "Error:\n"+e);
        }
    }

    private void populateInMemory(int numWare, ForkJoinPool pool) {
        Future<?>[] numWareFts = new Future[numWare];
        // order and order line
        for(int w_id = 1; w_id <= numWare; w_id++){
            final int f_w_id = w_id;
            numWareFts[w_id-1] = pool.submit(() -> {
                LOGGER.log(DEBUG, "Started creating 30_000 order records for warehouse " + f_w_id);
                this.transactionManager.beginTransaction(-f_w_id, 0, -numWare, false);
                long internalInitTs = System.currentTimeMillis();
                for (int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                    var pairCompositeKey = PairCompositeKey.of(d_id, f_w_id);
                    for (int o_id = 1; o_id <= TPCcConstants.NUM_CUST_PER_DIST; o_id++) {
                        int carrier_id = o_id < 2101 ? randomNumber(1, 10) : -1;
                        int ol_count = randomNumber(5, 15);

                        // order
                        Order order = new Order(o_id, d_id, f_w_id, randomNumber(1, 3000), new Date(), carrier_id, ol_count, 1);
                        this.orderRepository.insert(order);

                        // new order
                        NewOrder newOrder = new NewOrder(o_id, d_id, f_w_id);
                        if(OrderService.EXT_NEW_ORDER_IDX) {
                            synchronized (INewOrderRepository.NEW_ORDERS) {
                                try {
                                    INewOrderRepository.NEW_ORDERS.computeIfAbsent(pairCompositeKey, _ -> new TreeMap<>()).put(newOrder.getId(), newOrder);
                                } catch (Exception e) {
                                    LOGGER.log(ERROR, "Error:\n" + e);
                                }
                            }
                        } else {
                             this.newOrderRepository.insert(newOrder);
                        }

                        Date ol_delivery_d = o_id < 2101 ? new Date() : null;
                        float ol_amount = o_id < 2101 ? 0 : (float) (Math.floor((ThreadLocalRandom.current().nextDouble() * 9999.99) * 100) / 100.0);
                        for (int ol_id = 1; ol_id <= ol_count; ol_id++) {
                            OrderLine orderLine = new OrderLine(o_id, d_id, f_w_id, ol_id, randomNumber(1, TPCcConstants.NUM_ITEMS), f_w_id, ol_delivery_d, 5, ol_amount, makeAlphaString(26, 50));
                            this.orderLineRepository.insert(orderLine);
                        }
                    }
                }
                // transactionManager.commit();
                LOGGER.log(DEBUG, "Finished creating 30_000 order records for warehouse " + f_w_id + " in " + (System.currentTimeMillis() - internalInitTs) + " ms");
            });
        }
        try {
            for (int w_id = 1; w_id <= numWare; w_id++) {
                numWareFts[w_id-1].get();
            }
        } catch(ExecutionException | InterruptedException e){
            LOGGER.log(ERROR, "Error:\n"+e);
        }
    }

}
