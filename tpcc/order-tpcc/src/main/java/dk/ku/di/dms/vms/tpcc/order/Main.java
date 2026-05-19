package dk.ku.di.dms.vms.tpcc.order;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.order.infra.OrderHttpHandler;
import dk.ku.di.dms.vms.tpcc.order.repositories.IHistoryRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.INewOrderRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderLineRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderRepository;

import java.util.Properties;

/**
 * Port of the TPC-C order-related code as a virtual micro service
 */
public final class Main {
    public static void main( String[] args ) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        String numWareStr = prop.getProperty("num_ware");
        int num_ware = Integer.parseInt(numWareStr);
        // num orders fixed = 30k * num_ware
        int numOrders = num_ware * 30_000;
        // based on 20k tx/s and 10s run
        numOrders += (20_000 * 10);
        int numOrderLine = numOrders * 10;

        prop.setProperty("max_records.orders", String.valueOf(numOrders));
        prop.setProperty("max_records.new_orders", String.valueOf(numOrders));
        prop.setProperty("max_records.order_line", String.valueOf(numOrderLine));
        prop.setProperty("max_records.history", "500000");

        prop.setProperty("table.orders.chaining", "true");
        prop.setProperty("table.new_orders.chaining", "true");
        prop.setProperty("table.order_line.chaining", "true");
        prop.setProperty("table.history.chaining", "true");

        // should sort only d_id and w_id
        // prop.setProperty("table.new_orders.sorted", "true");

        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                8003, new String[]{
                        "dk.ku.di.dms.vms.tpcc.order",
                        "dk.ku.di.dms.vms.tpcc.common"
                });
        return VmsApplication.build(options, (x,y) -> new OrderHttpHandler(x,
                (IOrderRepository) y.apply("orders"),
                (INewOrderRepository) y.apply("new_orders"),
                (IOrderLineRepository) y.apply("order_line"),
                (IHistoryRepository) y.apply("history")
                )
        );
    }
}
