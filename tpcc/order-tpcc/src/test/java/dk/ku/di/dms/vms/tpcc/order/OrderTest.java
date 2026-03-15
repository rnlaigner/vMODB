package dk.ku.di.dms.vms.tpcc.order;

import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import dk.ku.di.dms.vms.tpcc.common.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.common.events.delivery.DeliveryIn;
import dk.ku.di.dms.vms.tpcc.common.events.delivery.DeliveryOut;
import dk.ku.di.dms.vms.tpcc.common.events.order_status.OrderStatusOut;
import dk.ku.di.dms.vms.tpcc.order.dto.OrderInfoDto;
import dk.ku.di.dms.vms.tpcc.order.dto.OrderLineInfoDto;
import dk.ku.di.dms.vms.tpcc.order.entities.NewOrder;
import dk.ku.di.dms.vms.tpcc.order.entities.Order;
import dk.ku.di.dms.vms.tpcc.order.entities.OrderLine;
import dk.ku.di.dms.vms.tpcc.order.repositories.INewOrderRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderLineRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderRepository;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

/**
 * Unit tests for Order VMS
 */
public class OrderTest {

    private static final int NUM_ORDERS = 10;

    public static final SelectStatement ORDER_BASE_QUERY = QueryBuilderFactory.select()
            .project("*")
            .from("orders")
            .and("o_c_id", ExpressionTypeEnum.EQUALS, ":c_id")
            .orderBy("o_id").desc().limit(1)
            .build();

    private static VmsApplication getVmsApplication() throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build("localhost", 8003, new String[]{
                "dk.ku.di.dms.vms.tpcc.order",
                "dk.ku.di.dms.vms.tpcc.common"
        });
        return VmsApplication.build(options);
    }

    static VmsApplication VMS;

    @BeforeClass
    public static void setUp() throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        prop.setProperty("table.new_orders.sorted", "true");
        VMS = getVmsApplication();
        VMS.start();
        insertOrders();
    }

    @SuppressWarnings("unchecked")
    private static void insertOrders() {
        var orderRepository = (AbstractProxyRepository<Order.OrderId, Order>) VMS.getRepositoryProxy("orders");
        var newOrderRepository = (AbstractProxyRepository<NewOrder.NewOrderId, NewOrder>) VMS.getRepositoryProxy("new_orders");
        var orderLineRepository = (IOrderLineRepository) VMS.getRepositoryProxy("order_line");
        VMS.getTransactionManager().beginTransaction(0, 0, 0, false);
        for (int i = 1; i <= TPCcConstants.NUM_DIST_PER_WARE; i++) {
            for (int j = 1; j <= NUM_ORDERS; j++) {
                Order order = new Order(j, i, 1, i, new Date(), 1, 1, 1);
                NewOrder newOrder = new NewOrder(j, i, 1);
                orderRepository.insert(order);
                newOrderRepository.insert(newOrder);
                orderLineRepository.insert(new OrderLine(
                        j, i, 1, 1, 1, 1, new Date(), 1, 1, "test"
                ));
                orderLineRepository.insert(new OrderLine(
                        j, i, 1, 2, 2, 1, new Date(), 1, 1, "test"
                ));
                Assert.assertTrue(orderRepository.exists(new Order.OrderId(j, i, 1)));
                Assert.assertTrue(newOrderRepository.exists(new NewOrder.NewOrderId(j, i, 1)));
                Assert.assertEquals(2, orderLineRepository.getAllByOrderId(j, i, 1).size());
            }
        }
        VMS.getTransactionManager().commit();
    }

    @Test
    public void testOrderStatusQueryByName() {
        VMS.getTransactionManager().beginTransaction(0, 0, 0, true);
        IOrderRepository orderRepository = (IOrderRepository) VMS.getRepositoryProxy("orders");
        OrderStatusOut orderStatusOut = new OrderStatusOut(1, 1, 1);
        int max_o_id = orderRepository.fetchOne(ORDER_BASE_QUERY, int.class);
        Assert.assertEquals(NUM_ORDERS, max_o_id);
        OrderInfoDto orderInfoDto = orderRepository.getOrderInfo(max_o_id, orderStatusOut.d_id, orderStatusOut.w_id, orderStatusOut.c_id);
        Assert.assertNotNull(orderInfoDto);
        Assert.assertEquals(NUM_ORDERS, orderInfoDto.o_id);
        var orderLineRepository = (IOrderLineRepository) VMS.getRepositoryProxy("order_line");
        List<OrderLineInfoDto> orderLinesInfo = orderLineRepository.getOrderLinesInfo(max_o_id, orderStatusOut.d_id, orderStatusOut.w_id);
        Assert.assertNotNull(orderLinesInfo);
        Assert.assertEquals(2, orderLinesInfo.size());
    }

    @Test
    public void testStockLevel() {
        VMS.getTransactionManager().beginTransaction(0, 0, 0, true);
        IOrderLineRepository orderLineRepository = (IOrderLineRepository) VMS.getRepositoryProxy("order_line");
        int[] orderIds = IntStream.range(1, 11).toArray();
        int[] itemIds = orderLineRepository.getAllItemsByOrderIds(orderIds, 1, 1);
        Assert.assertEquals(2, itemIds.length);
    }

    @Test
    public void testDeliveryQuery() {
        VMS.getTransactionManager().beginTransaction(0, 0, 0, true);
        INewOrderRepository newOrderRepository = (INewOrderRepository) VMS.getRepositoryProxy("new_orders");
        NewOrder newOrder = newOrderRepository.getFirstNewOrder(1, 1);
        Assert.assertNotNull(newOrder);
        Assert.assertEquals(1, newOrder.no_o_id);
    }

    @Test
    public void testProcessDelivery() {
        VMS.getTransactionManager().beginTransaction(0, 0, 0, false);
        OrderService orderService = VMS.getService("dk.ku.di.dms.vms.tpcc.order.OrderService");
        DeliveryOut out = orderService.processDelivery(new DeliveryIn(1, 1));
        Assert.assertNotNull(out);
        Assert.assertEquals(1, out.w_id);
        Assert.assertArrayEquals( new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, out.customerIds );
        Assert.assertArrayEquals( new float[]{ 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}, out.amounts, 0 );
    }

}
