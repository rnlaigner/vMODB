package dk.ku.di.dms.vms.tpcc.warehouse;

import dk.ku.di.dms.vms.modb.api.annotations.VmsPreparedStatement;
import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.common.events.OrderStatusIn;
import dk.ku.di.dms.vms.tpcc.warehouse.dto.CustomerInfoDTO;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Customer;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.District;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Warehouse;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IWarehouseRepository;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;
import java.util.List;

public class WarehouseTest {

    @VmsPreparedStatement("orderStatusCustomerQuery")
    public static final SelectStatement ORDER_STATUS_BASE_QUERY = QueryBuilderFactory.select()
            .project("c_balance").project("c_first").project("c_middle").project("c_last")
            .from("customer")
            .where("c_w_id", ExpressionTypeEnum.EQUALS, ":c_w_id")
            .and("c_d_id", ExpressionTypeEnum.EQUALS, ":c_d_id")
            .and("c_last", ExpressionTypeEnum.EQUALS, ":c_last")
            .orderBy( "c_first" ).build();

    private static VmsApplication getVmsApplication() throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build("localhost", 8001, new String[]{
                "dk.ku.di.dms.vms.tpcc.warehouse",
                "dk.ku.di.dms.vms.tpcc.common"
        });
        return VmsApplication.build(options);
    }

    static VmsApplication VMS;

    @BeforeClass
    public static void setUp() throws Exception {
        VMS = getVmsApplication();
        VMS.start();
    }

    private static void insertCustomers() {
        IWarehouseRepository warehouseRepository = (IWarehouseRepository) VMS.getRepositoryProxy("warehouse");
        IDistrictRepository districtRepository = (IDistrictRepository) VMS.getRepositoryProxy("district");
        ICustomerRepository customerRepository = (ICustomerRepository) VMS.getRepositoryProxy("customer");
        VMS.getTransactionManager().beginTransaction(0, 0, 0, false);
        warehouseRepository.insert(new Warehouse(1, "test", "test", "test", "test", "test", "test", 10, 10));
        districtRepository.insert(new District(1, 1, "test", "test", "test", "test", "test", "test", 10, 10, 1));
        for(int i = 1; i <= 10; i++){
            Customer customer = new Customer(i, 1, 1,
                    "test"+i, "test", "test", "test",
                    "test", "test", "test", "test", "test",
                    new Date(), "test", 1,
                    1, 1, 1, 1, 1, "test" );
            customerRepository.insert(customer);
            Assert.assertTrue(customerRepository.exists(new Customer.CustomerId(i, 1 , 1)));
        }
    }

    private static void generateOrderStatus(VmsApplication vms, int tid, int previousTid) {
        OrderStatusIn orderStatusIn = new OrderStatusIn(1, 1, 1, "test", true);
        InboundEvent inboundEvent = new InboundEvent(tid, previousTid, 1,
                "order-status-in", OrderStatusIn.class, orderStatusIn);
        vms.internalChannels().transactionInputQueue().add(inboundEvent);
    }

    public List<CustomerInfoDTO> issueOrderStatusQuery(OrderStatusIn in) {
        ICustomerRepository customerRepository = (ICustomerRepository) VMS.getRepositoryProxy("customer");
        return customerRepository.fetchMany(ORDER_STATUS_BASE_QUERY.setParam( in.w_id, in.d_id, in.c_last ), CustomerInfoDTO.class);
    }

    @Test
    public void testOrderStatusQueryByName() throws Exception {
        insertCustomers();
        var txCtx = VMS.getTransactionManager().beginTransaction( 1, 0, 10, true );
        // get warehouse service
        WarehouseService warehouseService = VMS.getService("dk.ku.di.dms.vms.tpcc.warehouse.WarehouseService");
        OrderStatusIn orderStatusIn = new OrderStatusIn(1, 1, 1, "test", true);

        var customers = issueOrderStatusQuery(orderStatusIn);
        Assert.assertEquals(10, customers.size());

        var orderStatusOut = warehouseService.processOrderStatus(orderStatusIn);
        Assert.assertEquals(1, orderStatusOut.c_id);
    }


    @Test
    public void testPaymentQueryByName() throws Exception {
        ICustomerRepository customerRepository = (ICustomerRepository) VMS.getRepositoryProxy("customer");
        insertCustomers();
        var txCtx = VMS.getTransactionManager().beginTransaction( 1, 0, 10, true );
        List<Customer> customers = customerRepository.getCustomerByLastName(1, 1, "test");
        Assert.assertEquals(10, customers.size());
    }

}
