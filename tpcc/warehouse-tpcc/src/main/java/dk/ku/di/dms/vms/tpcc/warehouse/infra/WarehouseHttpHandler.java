package dk.ku.di.dms.vms.tpcc.warehouse.infra;

import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import dk.ku.di.dms.vms.tpcc.common.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Customer;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.District;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Warehouse;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IWarehouseRepository;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import static dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils.*;
import static java.lang.System.Logger.Level.*;

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

        this.transactionManager.beginTransaction(Long.MAX_VALUE, 0, 0,false);
        List<Warehouse> warehouses = this.warehouseRepository.getAll();
        List<District> districts = this.districtRepository.getAll();
        List<Customer> customers = this.customerRepository.getAll();
        this.transactionManager.reset();

        LOGGER.log(INFO, "Warehouse GC triggered.");
        System.gc();
        LOGGER.log(INFO, "Warehouse GC finished.");

        LOGGER.log(INFO, "Warehouse tables reset");

        this.transactionManager.beginTransaction(0, 0, 0,false);
        for(District district : districts){
            district.d_next_o_id = 3001;
        }
        this.warehouseRepository.insertAll(warehouses);
        this.districtRepository.insertAll(districts);
        this.customerRepository.insertAll(customers);
        // this.transactionManager.commit();

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
    public void put(String uri, String payload) {
        final String[] uriSplit = uri.split("/");
        String op = uriSplit[uriSplit.length - 1];
        if(op.contentEquals("load")){
            // path: /warehouse/load
            this.transactionManager.rebuildIndexes();
            return;
        }

        int numWare = Integer.parseInt(ConfigUtils.loadProperties().getProperty("num_ware"));
        boolean checkpointing = Boolean.parseBoolean(ConfigUtils.loadProperties().getProperty("checkpointing"));
        this.transactionManager.reset();

        // warehouse
        LOGGER.log(INFO, "Populating warehouse VMS ("+numWare+" warehouses)...");

        ForkJoinPool pool = ForkJoinPool.commonPool();

        long initTs = System.currentTimeMillis();

        if(checkpointing) {
            // bypass default interfaces
            this.populateDisk(numWare, pool);
        } else {
            this.populateInMemory(numWare, pool);
        }

        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Finished populating warehouse VMS in "+(endTs-initTs)+" ms");
    }

    private void populateInMemory(int numWare, ForkJoinPool pool) {
        Future<?>[] numWareFts = new Future[numWare];
        for (int w_id = 1; w_id <= numWare; w_id++) {
            final int f_w_id = w_id;
            numWareFts[w_id - 1] = pool.submit(() -> {
                LOGGER.log(DEBUG, "Started creating 30_000 customer records for warehouse " + f_w_id);
                long internalInitTs = System.currentTimeMillis();
                transactionManager.beginTransaction(-f_w_id, 0, 0, false);
                Warehouse warehouse = generateWarehouse(f_w_id);
                this.warehouseRepository.insert(warehouse);
                for (int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                    District district = generateDistrict(d_id, f_w_id);
                    districtRepository.insert(district);
                    for (int c_id = 1; c_id <= TPCcConstants.NUM_CUST_PER_DIST; c_id++) {
                        Customer customer = generateCustomer(c_id, d_id, f_w_id);
                        customerRepository.insert(customer);
                    }
                }
                // bypass GC of this big writeSet at experiment startup time
                // transactionManager.commit();
                LOGGER.log(DEBUG, "Finished creating 30_000 customer records for warehouse " + f_w_id + " in " + (System.currentTimeMillis() - internalInitTs) + " ms");
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

    @SuppressWarnings("unchecked")
    private void populateDisk(int numWare, ForkJoinPool pool) {

        final var wareRepo = ((AbstractProxyRepository<Integer, Warehouse>) warehouseRepository);
        final var wareIndex = wareRepo.getTable().underlyingPrimaryKeyIndex();

        final var distRepo = ((AbstractProxyRepository<District.DistrictId, District>) districtRepository);
        final var distIndex = distRepo.getTable().underlyingPrimaryKeyIndex();

        final var custRepo = ((AbstractProxyRepository<Customer.CustomerId, Customer>) customerRepository);
        final var custIndex = custRepo.getTable().underlyingPrimaryKeyIndex();

        Future<?>[] numWareFts = new Future[numWare];

        for (int w_id = 1; w_id <= numWare; w_id++) {
            final int f_w_id = w_id;
            // coordinate accesses to primary index given it is designed for single-thread access
            numWareFts[w_id - 1] = pool.submit(() -> {
                LOGGER.log(INFO, "Started creating 30_000 customer records for warehouse " + f_w_id);
                long internalInitTs = System.currentTimeMillis();
                Warehouse warehouse = generateWarehouse(f_w_id);
                Object[] warObj = wareRepo.extractFieldValuesFromEntityObject(warehouse);
                IKey wareKey = KeyUtils.buildRecordKey( wareIndex.schema().getPrimaryKeyColumns(), warObj );
                synchronized (wareIndex) {
                    wareIndex.insert(wareKey, warObj);
                }
                for (int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                    District district = generateDistrict(d_id, f_w_id);
                    Object[] distObj = distRepo.extractFieldValuesFromEntityObject(district);
                    IKey distKey = KeyUtils.buildRecordKey( distIndex.schema().getPrimaryKeyColumns(), distObj );
                    synchronized (distIndex) {
                        distIndex.insert(distKey, distObj);
                    }
                    for (int c_id = 1; c_id <= TPCcConstants.NUM_CUST_PER_DIST; c_id++) {
                        Customer customer = generateCustomer(c_id, d_id, f_w_id);
                        Object[] custObj = custRepo.extractFieldValuesFromEntityObject(customer);
                        IKey custKey = KeyUtils.buildRecordKey( custIndex.schema().getPrimaryKeyColumns(), custObj );
                        synchronized (custIndex) {
                            custIndex.insert(custKey, custObj);
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

            Future<?>[] flushFts = new Future[3];
            flushFts[0] = pool.submit(wareIndex::flush);
            flushFts[1] = pool.submit(distIndex::flush);
            flushFts[2] = pool.submit(custIndex::flush);
            for (int i = 0; i < 3; i++) {
                flushFts[i].get();
            }

            this.transactionManager.rebuildIndexes();
        } catch(ExecutionException | InterruptedException e){
            LOGGER.log(ERROR, "Error:\n"+e);
        }
    }

    public static Warehouse generateWarehouse(int W_ID)
    {
        String W_NAME = makeAlphaString(6, 10);
        String W_STREET_1 = makeAlphaString(10, 20);
        String W_STREET_2 = makeAlphaString(10, 20);
        String W_CITY = makeAlphaString(10, 20);
        String W_STATE = makeAlphaString(2, 2);
        String W_ZIP = makeAlphaString(9, 9);
        float W_TAX = (float)((float) randomNumber(10, 20) / 100.0);
        float W_YTD = 3000000;
        return new Warehouse(W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD);
    }

    public static District generateDistrict(int D_ID, int D_W_ID)
    {
        String D_NAME = makeAlphaString(6, 10);
        String D_STREET_1 = makeAlphaString(10, 20);
        String D_STREET_2 = makeAlphaString(10, 20);
        String D_CITY = makeAlphaString(10, 20);
        String D_STATE = makeAlphaString(2, 2);
        String D_ZIP = makeAlphaString(9, 9);
        float D_TAX = (float) (((float) randomNumber(10, 20)) / 100.0);
        float D_YTD = (float) 30000.0;
        int D_NEXT_O_ID = 3001;
        return new District(D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID);
    }

    public static Customer generateCustomer(int c_id, int c_d_id, int c_w_id) {
        String C_FIRST = makeAlphaString(8, 16);
        String C_MIDDLE = "OE";
        String C_LAST;
        if (c_id <= 1000) {
            C_LAST = lastName(c_id - 1);
        } else {
            C_LAST = lastName(nuRand(255, 157, 0, 999));
        }

        String C_STREET_1 = makeAlphaString(10, 20);
        String C_STREET_2 = makeAlphaString(10, 20);
        String C_CITY = makeAlphaString(10, 20);
        String C_STATE = makeAlphaString(2, 2);
        String C_ZIP = makeAlphaString(9, 9);
        String C_PHONE = makeNumberString(16, 16);
        Date C_SINCE = new Date();

        String C_CREDIT;
        if (randomNumber(0, 1) == 1)
            C_CREDIT = "GC";
        else
            C_CREDIT = "BC";

        int C_CREDIT_LIM = 50000;
        float C_DISCOUNT = (float) (((float) randomNumber(0, 50)) / 100.0);
        float C_BALANCE = -10.0f;

        int C_YTD_PAYMENT = 10;
        int C_PAYMENT_CNT = 1;
        int C_DELIVERY_CNT = 0;
        String C_DATA = makeAlphaString(300, 500);

        return new Customer(c_id, c_d_id, c_w_id, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA);
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
