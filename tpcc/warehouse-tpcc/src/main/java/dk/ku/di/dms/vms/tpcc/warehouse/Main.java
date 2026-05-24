package dk.ku.di.dms.vms.tpcc.warehouse;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.warehouse.infra.WarehouseHttpHandler;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IWarehouseRepository;

import java.util.Properties;

/**
 * Port of the TPC-C warehouse-related code as a virtual micro service
 */
public final class Main {
    public static void main( String[] args ) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        String numWareStr = prop.getProperty("num_ware");
        int num_ware = Integer.parseInt(numWareStr);
        // fixed
        if(num_ware <= 16) {
            prop.setProperty("max_records.warehouse", numWareStr);
        } else {
            prop.setProperty("max_records.warehouse", String.valueOf(num_ware * 2));
        }
        // variable
        int numDistrict = num_ware * 10;
        prop.setProperty("max_records.district", String.valueOf(numDistrict));
        int numCustomers = num_ware * 30_000;
        prop.setProperty("max_records.customer", String.valueOf(numCustomers));
        prop.setProperty("table.customer.chaining", "true");

        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                8001, new String[]{
                        "dk.ku.di.dms.vms.tpcc.warehouse",
                        "dk.ku.di.dms.vms.tpcc.common"
                });
        return VmsApplication.build(options,
                (x,y) -> new WarehouseHttpHandler(x,
                        (IWarehouseRepository) y.apply("warehouse"),
                        (IDistrictRepository) y.apply("district"),
                        (ICustomerRepository) y.apply("customer")
                ));
    }

}