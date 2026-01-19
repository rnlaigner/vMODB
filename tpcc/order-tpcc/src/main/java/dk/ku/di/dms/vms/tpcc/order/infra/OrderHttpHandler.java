package dk.ku.di.dms.vms.tpcc.order.infra;

import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import dk.ku.di.dms.vms.tpcc.common.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.order.entities.NewOrder;
import dk.ku.di.dms.vms.tpcc.order.entities.Order;
import dk.ku.di.dms.vms.tpcc.order.entities.OrderLine;
import dk.ku.di.dms.vms.tpcc.order.repositories.IHistoryRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.INewOrderRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderLineRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderRepository;

import java.util.Date;
import java.util.List;
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
        LOGGER.log(INFO, "Warehouse init cleanup");

        this.transactionManager.beginTransaction(Long.MAX_VALUE, 0, 0,false);
        List<Order> orders = this.orderRepository.query(selectStatementOrder);
        List<NewOrder> newOrders = this.newOrderRepository.query(selectStatementNewOrder);
        List<OrderLine> orderLines = this.orderLineRepository.query(selectStatementOrderLine);
        this.transactionManager.reset();

        LOGGER.log(INFO, "Warehouse GC triggered.");
        System.gc();
        LOGGER.log(INFO, "Warehouse GC finished.");

        LOGGER.log(INFO, "Warehouse tables reset");

        this.transactionManager.beginTransaction(0, 0, 0,false);
        this.orderRepository.insertAll(orders);
        this.newOrderRepository.insertAll(newOrders);
        this.orderLineRepository.insertAll(orderLines);
        // this.transactionManager.commit();
        LOGGER.log(INFO, "Warehouse finished cleanup");
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
        int numWare = Integer.parseInt(ConfigUtils.loadProperties().getProperty("num_ware"));
        boolean checkpointing = Boolean.parseBoolean(ConfigUtils.loadProperties().getProperty("checkpointing"));
        this.transactionManager.reset();

        ForkJoinPool pool = ForkJoinPool.commonPool();
        Future<?>[] futures = new Future[numWare];

        LOGGER.log(INFO, "Populating order VMS...");
        long initTs = System.currentTimeMillis();

        if(checkpointing) {
            // bypass default interfaces
            populateDisk(numWare, futures, pool);
        } else {
            populateInMemory(numWare, futures, pool);
        }

        try {
            for (int w_id = 1; w_id <= numWare; w_id++) {
                futures[w_id-1].get();
            }
        } catch(ExecutionException | InterruptedException e){
            LOGGER.log(ERROR, "Error:\n"+e);
            return;
        }

        if(checkpointing){
            this.transactionManager.rebuildIndexes();
        }

        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Finished populating order VMS in "+(endTs-initTs)+" ms");
    }

    @SuppressWarnings("unchecked")
    private void populateDisk(int numWare, Future<?>[] futures, ForkJoinPool pool) {

        var orderRepo = ((AbstractProxyRepository<Order.OrderId, Order>) orderRepository);
        var orderTable = orderRepo.getTable();

        var orderLineRepo = ((AbstractProxyRepository<OrderLine.OrderLineId, OrderLine>) orderLineRepository);
        var orderLineTable = orderLineRepo.getTable();

        for(int w_id = 1; w_id <= numWare; w_id++){
            final int f_w_id = w_id;
            futures[w_id-1] = pool.submit(() -> {
                LOGGER.log(DEBUG, "Started creating 30K order records for warehouse " + f_w_id);
                long internalInitTs = System.currentTimeMillis();
                for (int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                    for (int o_id = 1; o_id <= TPCcConstants.NUM_CUST_PER_DIST; o_id++) {
                        int carrier_id = o_id < 2101 ? randomNumber(1, 10) : -1;
                        int ol_count = randomNumber(5, 15);
                        Order order = new Order(o_id, d_id, f_w_id, randomNumber(1, 3000), new Date(), carrier_id, ol_count, 1);
                        Object[] orderObj = orderRepo.extractFieldValuesFromEntityObject(order);
                        IKey orderKey = KeyUtils.buildRecordKey( orderTable.schema().getPrimaryKeyColumns(), orderObj );
                        orderTable.underlyingPrimaryKeyIndex().insert(orderKey, orderObj);
                        Date ol_delivery_d = o_id < 2101 ? new Date() : null;
                        float ol_amount = o_id < 2101 ? 0 : (float) (Math.floor((ThreadLocalRandom.current().nextDouble() * 9999.99) * 100) / 100.0);
                        for (int ol_id = 1; ol_id <= ol_count; ol_id++) {
                            OrderLine orderLine = new OrderLine(o_id, d_id, f_w_id, ol_id, randomNumber(1, TPCcConstants.NUM_ITEMS), f_w_id, ol_delivery_d, 5, ol_amount, makeAlphaString(26, 50));
                            Object[] orderLineObj = orderLineRepo.extractFieldValuesFromEntityObject(orderLine);
                            IKey orderLineKey = KeyUtils.buildRecordKey( orderLineTable.schema().getPrimaryKeyColumns(), orderLineObj );
                            orderLineTable.underlyingPrimaryKeyIndex().insert(orderLineKey, orderLineObj);
                        }
                    }
                }
                LOGGER.log(DEBUG, "Finished creating 30K order records for warehouse " + f_w_id + " in " + (System.currentTimeMillis() - internalInitTs) + " ms");
            });
        }

        orderTable.underlyingPrimaryKeyIndex().flush();
        orderLineTable.underlyingPrimaryKeyIndex().flush();
    }

    private void populateInMemory(int numWare, Future<?>[] futures, ForkJoinPool pool) {
        // order and order line
        for(int w_id = 1; w_id <= numWare; w_id++){
            final int f_w_id = w_id;
            futures[w_id-1] = pool.submit(() -> {
                LOGGER.log(DEBUG, "Started creating 30K order records for warehouse " + f_w_id);
                transactionManager.beginTransaction(-f_w_id, 0, -numWare, false);
                long internalInitTs = System.currentTimeMillis();
                for (int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                    for (int o_id = 1; o_id <= TPCcConstants.NUM_CUST_PER_DIST; o_id++) {
                        int carrier_id = o_id < 2101 ? randomNumber(1, 10) : -1;
                        int ol_count = randomNumber(5, 15);
                        Order order = new Order(o_id, d_id, f_w_id, randomNumber(1, 3000), new Date(), carrier_id, ol_count, 1);
                        orderRepository.insert(order);
                        Date ol_delivery_d = o_id < 2101 ? new Date() : null;
                        float ol_amount = o_id < 2101 ? 0 : (float) (Math.floor((ThreadLocalRandom.current().nextDouble() * 9999.99) * 100) / 100.0);
                        for (int ol_id = 1; ol_id <= ol_count; ol_id++) {
                            OrderLine orderLine = new OrderLine(o_id, d_id, f_w_id, ol_id, randomNumber(1, TPCcConstants.NUM_ITEMS), f_w_id, ol_delivery_d, 5, ol_amount, makeAlphaString(26, 50));
                            orderLineRepository.insert(orderLine);
                        }
                    }
                }
                // transactionManager.commit();
                LOGGER.log(DEBUG, "Finished creating 30K order records for warehouse " + f_w_id + " in " + (System.currentTimeMillis() - internalInitTs) + " ms");
            });
        }
    }

}
