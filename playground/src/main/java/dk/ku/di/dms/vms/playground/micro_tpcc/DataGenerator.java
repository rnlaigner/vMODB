package dk.ku.di.dms.vms.playground.micro_tpcc;

import dk.ku.di.dms.vms.micro_tpcc.common.entity.*;
import dk.ku.di.dms.vms.micro_tpcc.customer.entity.History;
import dk.ku.di.dms.vms.micro_tpcc.order.entity.NewOrder;
import dk.ku.di.dms.vms.micro_tpcc.order.entity.Order;
import dk.ku.di.dms.vms.micro_tpcc.order.entity.OrderLine;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.control.Presentation;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static dk.ku.di.dms.vms.modb.common.schema.Constants.BATCH_OF_TRANSACTION_EVENTS;
import static java.net.StandardSocketOptions.*;

public class DataGenerator {

    private static final Logger logger = LoggerFactory.getLogger("DataLoader");

    private static final IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

    private final CountDownLatch latch;

    public DataGenerator(){
        this.latch = new CountDownLatch(7);
    }

    /**
     * TODO finish
     * <a href="https://www.jetbrains.com/help/idea/debug-a-java-application-using-a-dockerfile.html#create-remote-debug-config">How to debug inside container.</a>
     */
    public void start(String... args) throws IOException, ExecutionException, InterruptedException {

        for(String i : args){
            if(i.equalsIgnoreCase("warehouse")){
                loadWarehouses(Constants.DEFAULT_NUM_WARE);
            }
            if(i.equalsIgnoreCase("warehouse")){
                loadDistricts(Constants.DEFAULT_NUM_WARE, Constants.DIST_PER_WARE);
            }
        }

    }

    public void start() throws IOException, ExecutionException, InterruptedException {

        loadWarehouses(Constants.DEFAULT_NUM_WARE);
        loadDistricts(Constants.DEFAULT_NUM_WARE, Constants.DIST_PER_WARE);
        loadCustomers(Constants.DEFAULT_NUM_WARE, Constants.DIST_PER_WARE, Constants.CUST_PER_DIST);
        loadHistory(Constants.DEFAULT_NUM_WARE, Constants.DIST_PER_WARE, Constants.CUST_PER_DIST);

        loadItems(Constants.MAX_ITEMS);
        loadStock(Constants.DEFAULT_NUM_WARE, Constants.MAX_ITEMS);

        loadOrders(Constants.DEFAULT_NUM_WARE, Constants.DIST_PER_WARE, Constants.CUST_PER_DIST, Constants.MAX_ITEMS);

    }

    private class BulkDataLoaderProtocol implements Runnable {

        private final List<?> objects;

        private final Class<?> clazz;
        private Status status;

        private final String table;
        private final AsynchronousSocketChannel channel;
        private final SocketAddress vms;

        private final IVmsSerdesProxy serdes;

        public BulkDataLoaderProtocol(String table, List<?> objects, Class<?> clazz, SocketAddress vms, IVmsSerdesProxy serdes) throws IOException, ExecutionException, InterruptedException {
            this.objects = objects;
            this.clazz = clazz;
            this.vms = vms;
            this.table = table;
            this.channel = AsynchronousSocketChannel.open();
            this.channel.setOption(TCP_NODELAY, Boolean.TRUE);
            this.channel.setOption(SO_KEEPALIVE, Boolean.TRUE);
            // avoid having to actually create and call the thread
            this.channel.connect(vms).get();
            // connect
            this.status = Status.CONNECTED;
            this.serdes = serdes;
        }

        @Override
        public void run() {

            ByteBuffer writeBuffer;
            int bufferSize = 1024;
            // get send buffer size so we can obtain a bb accordingly
            try {
                bufferSize = this.channel.getOption(SO_SNDBUF);
            } catch (ClosedChannelException e) {
                // then channel is closed
                throw new RuntimeException(e);
            } catch (IOException e) {
                if (!this.channel.isOpen()) {
                    throw new RuntimeException(e);
                }
            }

            writeBuffer = MemoryManager.getTemporaryDirectBuffer(bufferSize);

            // send presentation
            Presentation.writeClient(writeBuffer, table);
            writeBuffer.flip();

            try {
                this.channel.write(writeBuffer).get();
            } catch (InterruptedException | ExecutionException e) {
                // keep trying while channel is opened?
                throw new RuntimeException(e);
            }

            writeBuffer.clear();

            // start streaming
            this.status = Status.STREAMING;

            int n = objects.size();
            int i = 0;
            int remaining = bufferSize;
            int totalObjectSize;
            // while we can fulfill the buffer, send them

            String objStr = serdes.serialize( objects.get(i), clazz );

            byte[] bytes = objStr.getBytes();
            int nextObjectSize = bytes.length;

            while (i < n) {

                writeBuffer.put(BATCH_OF_TRANSACTION_EVENTS);
                // first int is to write the number of objects
                writeBuffer.position(1 + Integer.BYTES);

                int count = 0;
                totalObjectSize = (nextObjectSize + Integer.BYTES);

                while(i < n && remaining >= totalObjectSize){
                    writeBuffer.putInt(nextObjectSize);
                    writeBuffer.put(bytes);
                    i++;
                    count++;
                    remaining = remaining - totalObjectSize;

                    if(i < n) {
                        // update nextObjectSize
                        objStr = serdes.serialize( objects.get(i), clazz );
                        bytes = objStr.getBytes();
                        nextObjectSize = bytes.length;
                    }
                }

                // we have to seal anyway this buffer
                writeBuffer.mark();
                writeBuffer.putInt(1, count);
                writeBuffer.reset();
                writeBuffer.flip();

                try {
                    this.channel.write( writeBuffer ).get();
                } catch (InterruptedException | ExecutionException e) {
                    logger.warn("Error on sending bulk data to VMS!");
                } finally {
                    writeBuffer.clear();
                }

            }

            // close connection
            try {
                this.channel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                this.status = Status.DONE;
                latch.countDown();
            }

        }

        private enum Status {
            CONNECTED,
            STREAMING,
            DONE
        }

    }

    public void loadWarehouses(int num_ware) throws IOException, ExecutionException, InterruptedException {

        List<Warehouse> warehouses = new ArrayList<>(num_ware);
        // creating warehouse(s)
        for (int i = 1; i <= num_ware; i++) {
            warehouses.add(new Warehouse(i, ((double) Utils.randomNumber(10, 20) / 100.0), 3000000.00));
        }

        BulkDataLoaderProtocol protocol = new BulkDataLoaderProtocol("warehouse", warehouses, Warehouse.class, new InetSocketAddress("localhost", 8081), serdes);
        Thread thread = new Thread(protocol);
        thread.start();

    }

    public void loadDistricts(int num_ware, int distPerWare) throws IOException, ExecutionException, InterruptedException {

        List<District> districts = new ArrayList<>(num_ware * distPerWare);
        int d_next_o_id = Constants.ORD_PER_DIST + 1;

        // creating districts
        for (int i = 1; i <= num_ware; i++) {
            for (int j = 1; j <= distPerWare; j++) {
                districts.add( new District(
                        j, // district
                        i, // warehouse
                        ((Utils.randomNumber(10, 20)) / 100.0),
                        30000.0,
                        d_next_o_id) );
            }
        }

        BulkDataLoaderProtocol protocol = new BulkDataLoaderProtocol("district", districts, District.class, new InetSocketAddress("localhost", 8081), serdes);
        Thread thread = new Thread(protocol);
        thread.start();
    }

    public void loadCustomers(int num_ware, int distPerWare, int custPerDist) throws IOException, ExecutionException, InterruptedException {

        List<Customer> customers = new ArrayList<>(num_ware * distPerWare * custPerDist);

        for (int c_w_id = 1; c_w_id <= num_ware; c_w_id++) {
            for (int c_d_id = 1; c_d_id <= distPerWare; c_d_id++) {
                for (int c_id = 1; c_id <= custPerDist; c_id++) {

                    String c_first = Utils.makeAlphaString(8, 16);

                    String c_last;
                    if (c_id <= 1000) {
                        c_last = Utils.lastName(c_id - 1);
                    } else {
                        c_last = Utils.lastName(Utils.nuRand(255, 0, 999));
                    }

                    Date c_since = new Date();

                    String c_credit = Utils.randomNumber(0, 10) > 1 ? "GC" : "BC";

                    customers.add( new Customer(
                            c_id,
                            c_d_id,
                            c_w_id,
                            ((Utils.randomNumber(0, 50)) / 100.0f),
                            c_first,
                            c_last,
                            c_since,
                            c_credit,
                            -10.0f,
                            10.0f) );

                }

            }

        }

        BulkDataLoaderProtocol protocol = new BulkDataLoaderProtocol("customer", customers, Customer.class, new InetSocketAddress("localhost", 1082), serdes);
        Thread thread = new Thread(protocol);
        thread.start();

    }

    public void loadHistory(int num_ware, int distPerWare, int custPerDist) throws IOException, ExecutionException, InterruptedException {

        List<History> historyRecords = new ArrayList<>(num_ware * distPerWare * custPerDist);
        int hist_id = 0;
        for (int i = 1; i <= num_ware; i++) {
            for (int j = 1; j <= distPerWare; j++) {
                for (int c_id = 1; c_id <= custPerDist; c_id++) {

                    hist_id++;

                    historyRecords.add(
                            new History(hist_id,
                                    c_id,
                                    j,
                                    i,
                                    j,
                                    i,
                                    new Date(),
                                    10.0f,
                                    Utils.makeAlphaString(12, 24)
                            )
                    );

                }
            }

        }

        BulkDataLoaderProtocol protocol = new BulkDataLoaderProtocol("history", historyRecords, History.class, new InetSocketAddress("localhost", 1083), serdes);
        Thread thread = new Thread(protocol);
        thread.start();

    }

    public void loadItems(int max_items) throws IOException, ExecutionException, InterruptedException {

        List<Item> items = new ArrayList<>(max_items);
        int pos;
        int[] orig = new int[max_items + 1];

        for (int i = 0; i < max_items / 10; i++) {
            orig[i] = 0;
        }
        for (int i = 0; i < max_items / 10; i++) {
            do {
                pos = Utils.randomNumber(0, max_items);
            } while (orig[pos] != 0);
            orig[pos] = 1;
        }

        // creating items
        for (int i = 1; i <= max_items; i++) {

            int i_im_id = Utils.randomNumber(1, 10000);

            String i_name = Utils.makeAlphaString(14, 24);

            String i_data = Utils.makeAlphaString(26, 50);
            if (orig[i] != 0) {
                pos = Utils.randomNumber(0, i_data.length() - 8);
                i_data = i_data.substring(0, pos) + "original" + i_data.substring(pos + 8);
            }

            float i_price = ((Utils.randomNumber(100, 10000)) / 100.0f);

            items.add(new Item(i, i_im_id, i_name, i_price, i_data));
        }

        BulkDataLoaderProtocol protocol = new BulkDataLoaderProtocol("item", items, Item.class, new InetSocketAddress("localhost", 1084), serdes);
        Thread thread = new Thread( protocol );
        thread.start();

    }

    public void loadStock(int num_ware, int max_items) throws IOException, ExecutionException, InterruptedException {


        List<Stock> stock = new ArrayList<>(num_ware * max_items);

        // creating stock items
        for (int i = 1; i <= num_ware; i++) {

            // creating stock for each item
            for (int j = 1; j <= max_items; j++) {

                int s_quantity = Utils.randomNumber(Constants.MIN_STOCK_QTY, Constants.MAX_STOCK_QTY);
                String s_data = Utils.makeAlphaString(26, 50);
                String s_dist = Utils.makeAlphaString(24, 24);

                stock.add(new Stock(j, i, s_quantity, s_data, s_dist));
            }

        }

        BulkDataLoaderProtocol protocol = new BulkDataLoaderProtocol("stock", stock, Stock.class, new InetSocketAddress("localhost", 1085), serdes);
        Thread thread = new Thread( protocol );
        thread.start();

    }

    public void loadOrders(int num_ware, int distPerWare, int custPerDist, int max_items) throws IOException, ExecutionException, InterruptedException {

        int o_c_id;
        int o_carrier_id;
        int o_ol_cnt;
        Date date;

        int ol_i_id;
        int ol_supply_w_id;
        int ol_quantity;
        float ol_amount;
        String ol_dist_info;

        List<Order> orders = new ArrayList<>(num_ware * distPerWare * custPerDist);
        List<NewOrder> newOrders = new ArrayList<>(num_ware * distPerWare * custPerDist);
        List<OrderLine> orderLines = new ArrayList<>(num_ware * distPerWare * custPerDist * max_items);

        // for each warehouse
        for (int w_id = 1; w_id <= num_ware; w_id++) {
            // for each district
            for (int d_id = 1; d_id <= Constants.DIST_PER_WARE; d_id++) {
                // generate orders
                for (int o_id = 1; o_id <= Constants.ORD_PER_DIST; o_id++) {

                    o_c_id = Utils.randomNumber(1, Constants.CUST_PER_DIST);
                    o_carrier_id = Utils.randomNumber(1, 10);
                    o_ol_cnt = Utils.randomNumber(Constants.MIN_NUM_ITEMS, Constants.MAX_NUM_ITEMS);

                    date = new Date(System.currentTimeMillis());

                    if (o_id <= 2100) {
                        orders.add(new Order(o_id, d_id, w_id, o_c_id, date, o_carrier_id, o_ol_cnt, 1));
                    } else {
                        orders.add(new Order(o_id, d_id, w_id, o_c_id, date, 0, o_ol_cnt, 1));
                        newOrders.add(new NewOrder(o_id, d_id, w_id));
                    }


                    // order lines
                    for (int ol = 1; ol <= o_ol_cnt; ol++) {

                        ol_i_id = Utils.randomNumber(1, max_items);
                        ol_supply_w_id = w_id;
                        ol_quantity = Constants.DEFAULT_ITEM_QTY;
                        ol_dist_info = Utils.makeAlphaString(24, 24);

                        if (o_id <= 2100) {
                            ol_amount = (Utils.randomNumber(10, 10000)) / 100.0f;
                            orderLines.add(new OrderLine(o_id, d_id, w_id, ol, ol_i_id, ol_supply_w_id, date, ol_quantity, ol_amount, ol_dist_info));
                        } else {
                            ol_amount = 0.0f;
                            orderLines.add(new OrderLine(o_id, d_id, w_id, ol, ol_i_id, ol_supply_w_id, null, ol_quantity, ol_amount, ol_dist_info));
                        }

                    }

                }
            }

        }

        BulkDataLoaderProtocol protocol = new BulkDataLoaderProtocol("orders", orders, Order.class, new InetSocketAddress("localhost", 1086), serdes);
        Thread thread = new Thread( protocol );
        thread.start();

        BulkDataLoaderProtocol protocol1 = new BulkDataLoaderProtocol("order_line", orderLines, OrderLine.class, new InetSocketAddress("localhost", 1086), serdes);
        Thread thread1 = new Thread( protocol1 );
        thread1.start();

        BulkDataLoaderProtocol protocol2 = new BulkDataLoaderProtocol("new_orders", newOrders, NewOrder.class, new InetSocketAddress("localhost", 1086), serdes);
        Thread thread2 = new Thread( protocol2 );
        thread2.start();

    }

}
