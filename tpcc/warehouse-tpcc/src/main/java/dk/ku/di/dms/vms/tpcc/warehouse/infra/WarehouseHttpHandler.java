package dk.ku.di.dms.vms.tpcc.warehouse.infra;

import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Customer;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.District;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Warehouse;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IWarehouseRepository;

import java.util.List;

import static java.lang.System.Logger.Level.INFO;

public final class WarehouseHttpHandler extends DefaultHttpHandler {

    private final IWarehouseRepository warehouseRepository;

    private final IDistrictRepository districtRepository;

    private final ICustomerRepository customerRepository;

    public WarehouseHttpHandler(ITransactionManager transactionManager,
                                IWarehouseRepository warehouseRepository,
                                IDistrictRepository districtRepository,
                                ICustomerRepository customerRepository) {
        super(transactionManager);
        this.warehouseRepository = warehouseRepository;
        this.districtRepository = districtRepository;
        this.customerRepository = customerRepository;
    }

    @Override
    public void patch(String uri, String body) {
        final String[] uriSplit = uri.split("/");
        String op = uriSplit[uriSplit.length - 1];
        if(op.contentEquals("reset")){
            // path: /warehouse/reset
            this.transactionManager.reset();
            return;
        }
        // path: /warehouse/cleanup
        LOGGER.log(INFO, "Warehouse init cleanup");

        LOGGER.log(INFO, "Warehouse GC triggered.");
        System.gc();
        LOGGER.log(INFO, "Warehouse GC finished.");

        List<Warehouse> warehouses = this.warehouseRepository.getAll();
        List<District> districts = this.districtRepository.getAll();
        List<Customer> customers = this.customerRepository.getAll();
        this.transactionManager.reset();
        LOGGER.log(INFO, "Warehouse tables reset");
        this.transactionManager.beginTransaction(0, 0, 0,false);
        for(District district : districts){
            district.d_next_o_id = 3000;
        }
        this.warehouseRepository.insertAll(warehouses);
        this.districtRepository.insertAll(districts);
        this.customerRepository.insertAll(customers);
        this.transactionManager.commit();
        LOGGER.log(INFO, "Warehouse finished cleanup");
    }

    @Override
    public Object getAsJson(String uri) throws RuntimeException {
        String[] uriSplit = uri.split("/");
        String table;
        switch (uriSplit.length){
            case 3 -> table = uriSplit[uriSplit.length - 2];
            case 4 -> table = uriSplit[uriSplit.length - 3];
            case 5 -> table = uriSplit[uriSplit.length - 4];
            default -> table = "";
        }
        switch (table){
            case "warehouse" -> {
                int wareId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
                this.transactionManager.beginTransaction(0, 0, 0, true);
                return this.warehouseRepository.lookupByKey(wareId);
            }
            case "district" -> {
                int distId = Integer.parseInt(uriSplit[uriSplit.length - 2]);
                int wareId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
                this.transactionManager.beginTransaction(Long.MAX_VALUE, 0, Long.MAX_VALUE, true);
                return this.districtRepository.lookupByKey(new District.DistrictId( distId, wareId ));
            }
            case "customer" -> {
                int cId = Integer.parseInt(uriSplit[uriSplit.length - 3]);
                int distId = Integer.parseInt(uriSplit[uriSplit.length - 2]);
                int wareId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
                this.transactionManager.beginTransaction(0, 0, 0, true);
                return this.customerRepository.lookupByKey(new Customer.CustomerId( cId, distId, wareId ));
            }
            case null, default -> {
                LOGGER.log(System.Logger.Level.WARNING, "URI not recognized: "+uri);
                return "{ \"message\":\" URI not recognized = "+uri+"\" }";
            }
        }
    }

    @Override
    public void post(String uri, String payload) {
        String[] uriSplit = uri.split("/");
        String table = uriSplit[uriSplit.length - 1];
        switch (table){
            case "warehouse" -> {
                Warehouse warehouse = SERDES.deserialize(payload, Warehouse.class);
                this.transactionManager.beginTransaction(0, 0, 0, false);
                this.warehouseRepository.upsert(warehouse);
            }
            case "district" -> {
                District district = SERDES.deserialize(payload, District.class);
                this.transactionManager.beginTransaction(0, 0, 0, false);
                this.districtRepository.upsert(district);
            }
            case "customer" -> {
                Customer customer = SERDES.deserialize(payload, Customer.class);
                this.transactionManager.beginTransaction(0, 0, 0, false);
                this.customerRepository.upsert(customer);
            }
        }
    }

}
