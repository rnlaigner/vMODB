package dk.ku.di.dms.vms.tpcc.order.infra;

import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.tpcc.common.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.order.entities.Order;
import dk.ku.di.dms.vms.tpcc.order.entities.OrderLine;
import dk.ku.di.dms.vms.tpcc.order.repositories.IHistoryRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.INewOrderRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderLineRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderRepository;

import java.util.Date;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils.makeAlphaString;
import static dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils.randomNumber;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

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

        ExecutorService threadPool = Executors.newFixedThreadPool(numWare);
        BlockingQueue<Future<Void>> completionQueue = new ArrayBlockingQueue<>(numWare);
        CompletionService<Void> service = new ExecutorCompletionService<>(threadPool, completionQueue);

        LOGGER.log(INFO, "Creating order records...");
        long initTs = System.currentTimeMillis();

        // order and order line
        for(int w_id = 1; w_id <= numWare; w_id++){
            final int f_w_id = w_id;
            service.submit(() -> {
                LOGGER.log(INFO, "Started creating 30K order records for warehouse " + f_w_id);
                transactionManager.beginTransaction(f_w_id, 0, 0, false);
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
                transactionManager.commit();
                LOGGER.log(INFO, "Finished creating 30K order records for warehouse " + f_w_id + " in " + (System.currentTimeMillis() - internalInitTs) + " ms");
            }, null);
        }

        try {
            for (int w_id = 1; w_id <= numWare; w_id++) {
                completionQueue.take();
            }
        } catch(InterruptedException e){
            threadPool.shutdownNow();
            LOGGER.log(ERROR, "Error:\n"+e);
            return;
        }

        if(checkpointing){
            this.transactionManager.checkpoint(numWare);
        }

//        this.transactionManager.beginTransaction(Long.MAX_VALUE, 0, 0,false);
//        List<Customer> customers = this.customerRepository.getAll();

        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Finished creating order records in "+(endTs-initTs)+" ms");
    }

}
