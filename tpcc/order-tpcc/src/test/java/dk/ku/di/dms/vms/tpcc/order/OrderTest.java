package dk.ku.di.dms.vms.tpcc.order;

import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
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
import java.util.stream.IntStream;

/**
 * Unit tests for Order VMS
 */
public class OrderTest {

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
        VMS = getVmsApplication();
        VMS.start();
        insertOrders(VMS);
    }

    @SuppressWarnings("unchecked")
    private static void insertOrders(VmsApplication vms) {
        var orderRepository = (AbstractProxyRepository<Order.OrderId, Order>) vms.getRepositoryProxy("orders");
        var newOrderRepository = (AbstractProxyRepository<NewOrder.NewOrderId, NewOrder>) vms.getRepositoryProxy("new_orders");
        var orderLineRepository = (IOrderLineRepository) vms.getRepositoryProxy("order_line");
        for(int i = 1; i <= 10; i++){
            Order order = new Order(i, 1, 1, 1, new Date(), 1, 1, 1);
            NewOrder newOrder = new NewOrder(i, 1, 1);
            VMS.getTransactionManager().beginTransaction(0, 0, 0, false);
            orderRepository.insert(order);
            newOrderRepository.insert(newOrder);
            orderLineRepository.insert(new OrderLine(
                i, 1, 1, 1, 1, 1, new Date(), 1, 1, "test"
            ));
            orderLineRepository.insert(new OrderLine(
                i, 1, 1, 2, 2, 1, new Date(), 1, 1, "test"
            ));
            Assert.assertTrue(orderRepository.exists(new Order.OrderId(i, 1 , 1)));
            Assert.assertTrue(newOrderRepository.exists(new NewOrder.NewOrderId(i, 1 , 1)));
            Assert.assertEquals(2, orderLineRepository.getAllByOrderId(i, 1, 1).size());
        }
    }

    @Test
    public void testOrderStatusQueryByName() {
        IOrderRepository orderRepository = (IOrderRepository) VMS.getRepositoryProxy("orders");
        OrderStatusOut orderStatusOut = new OrderStatusOut(1, 1, 1);
        int max_o_id = orderRepository.fetchOne(ORDER_BASE_QUERY, int.class);
        Assert.assertEquals(10, max_o_id);
        OrderInfoDto orderInfoDto = orderRepository.getOrderInfo(max_o_id, orderStatusOut.d_id, orderStatusOut.w_id, orderStatusOut.c_id);
        Assert.assertNotNull(orderInfoDto);
        Assert.assertEquals(10, orderInfoDto.o_id);
        var orderLineRepository = (IOrderLineRepository) VMS.getRepositoryProxy("order_line");
        List<OrderLineInfoDto> orderLinesInfo = orderLineRepository.getOrderLinesInfo(max_o_id, orderStatusOut.d_id, orderStatusOut.w_id);
        Assert.assertNotNull(orderLinesInfo);
        Assert.assertEquals(2, orderLinesInfo.size());
    }

    @Test
    public void testStockLevel() {
        IOrderLineRepository orderLineRepository = (IOrderLineRepository) VMS.getRepositoryProxy("order_line");
        int[] orderIds = IntStream.range(1, 11).toArray();
        int[] itemIds = orderLineRepository.getAllItemsByOrderIds(orderIds, 1, 1);
        Assert.assertEquals(2, itemIds.length);
    }

    @Test
    public void testDelivery() {
        INewOrderRepository newOrderRepository = (INewOrderRepository) VMS.getRepositoryProxy("new_orders");
        NewOrder newOrder = newOrderRepository.getFirstNewOrder(1, 1);
        Assert.assertNotNull(newOrder);
        Assert.assertEquals(1, newOrder.no_o_id);
    }

}
