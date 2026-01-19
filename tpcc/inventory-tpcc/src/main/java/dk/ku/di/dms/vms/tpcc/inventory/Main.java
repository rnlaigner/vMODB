package dk.ku.di.dms.vms.tpcc.inventory;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.inventory.infra.InventoryHttpHandler;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IItemRepository;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IStockRepository;

import java.util.Properties;

/**
 * Port of the TPC-C inventory-related code as a virtual micro service
 */
public final class Main {
    public static void main(String[] args) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        int num_ware = Integer.parseInt(prop.getProperty("num_ware"));
        // fixed
        prop.setProperty("max_records.item", "100000");
        // variable
        int numStockItems = num_ware * 100_000;
        prop.setProperty("max_records.stock", String.valueOf(numStockItems));
        prop.setProperty("table.stock.chaining", "true");

        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                8002, new String[]{
                        "dk.ku.di.dms.vms.tpcc.inventory",
                        "dk.ku.di.dms.vms.tpcc.common"
                });
        return VmsApplication.build(options, (x,y) -> new InventoryHttpHandler(x,
                (IItemRepository) y.apply("item"),
                (IStockRepository) y.apply("stock")
        ));
    }
}
