package dk.ku.di.dms.vms.tpcc.order;

import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.tpcc.common.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.common.events.delivery.DeliveryIn;
import dk.ku.di.dms.vms.tpcc.common.events.delivery.DeliveryOut;
import dk.ku.di.dms.vms.tpcc.common.events.new_order.NewOrderInvOut;
import dk.ku.di.dms.vms.tpcc.common.events.order_status.OrderStatusOut;
import dk.ku.di.dms.vms.tpcc.common.events.payment.PaymentOut;
import dk.ku.di.dms.vms.tpcc.common.events.stock_level.StockLevelOrdOut;
import dk.ku.di.dms.vms.tpcc.common.events.stock_level.StockLevelWareOut;
import dk.ku.di.dms.vms.tpcc.order.dto.OrderLineInfoDto;
import dk.ku.di.dms.vms.tpcc.order.entities.History;
import dk.ku.di.dms.vms.tpcc.order.entities.NewOrder;
import dk.ku.di.dms.vms.tpcc.order.entities.Order;
import dk.ku.di.dms.vms.tpcc.order.entities.OrderLine;
import dk.ku.di.dms.vms.tpcc.order.repositories.IHistoryRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.INewOrderRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderLineRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderRepository;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.*;
import static java.lang.System.Logger.Level.*;

@Microservice("order")
public final class OrderService {

    private static final System.Logger LOGGER = System.getLogger(OrderService.class.getName());

    private final IOrderRepository orderRepository;
    private final INewOrderRepository newOrderRepository;
    private final IOrderLineRepository orderLineRepository;
    private final IHistoryRepository historyRepository;

    public OrderService(IOrderRepository orderRepository, INewOrderRepository newOrderRepository, IOrderLineRepository orderLineRepository, IHistoryRepository historyRepository) {
        this.orderRepository = orderRepository;
        this.newOrderRepository = newOrderRepository;
        this.orderLineRepository = orderLineRepository;
        this.historyRepository = historyRepository;
    }

    @Inbound(values = "delivery-in")
    @Outbound("delivery-out")
    @Transactional(type = RW)
    @PartitionBy(clazz = DeliveryIn.class, method = "getId")
    public DeliveryOut processDelivery(DeliveryIn in) {
        int no_o_id;
        int[] customerIds = new int[10];
        float[] amounts = new float[10];
        for(int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {

            NewOrder newOrder = this.newOrderRepository.getFirstNewOrder(d_id, in.w_id);
            if(newOrder == null) {
                LOGGER.log(ERROR, "New Order Not Found");
                newOrder = this.newOrderRepository.getFirstNewOrder(d_id, in.w_id);
                continue;
            }
            no_o_id = newOrder.no_o_id;
            this.newOrderRepository.delete(newOrder);

            Order order = this.orderRepository.lookupByKey(new Order.OrderId(no_o_id, d_id, in.w_id));
            // put carrier id in the input
            order.o_carrier_id = in.carrier_id;
            this.orderRepository.update(order);

            Date date = new Date();
            List<OrderLine> orderLines = this.orderLineRepository.getAllByOrderId(no_o_id, d_id, in.w_id);

            float ol_amount = 0;

            for(OrderLine orderLine : orderLines) {
                orderLine.ol_delivery_d = date;
                ol_amount += orderLine.ol_amount;
            }

            this.orderLineRepository.updateAll(orderLines);

            customerIds[d_id - 1] = order.o_c_id;
            amounts[d_id - 1] = ol_amount;
        }

        return new DeliveryOut(in.w_id, customerIds, amounts);
    }

    @Inbound(values = "stock-level-ware-out")
    @Outbound("stock-level-ord-out")
    @Transactional(type = R)
    public StockLevelOrdOut processStockLevel(StockLevelWareOut in) {
        int[] orderIds = IntStream.range(in.next_o_id - 20, in.next_o_id).toArray();
        int[] itemIds = this.orderLineRepository.getAllItemsByOrderIds(orderIds, in.d_id, in.w_id);
        return new StockLevelOrdOut(in.w_id, itemIds);
    }

    @Inbound(values = "payment-out")
    @Transactional(type = W)
    @Parallel
    public void processPayment(PaymentOut out){
        History history = new History(out.c_id, out.c_d_id, out.c_w_id, out.d_id, out.w_id, new Date(), out.amount, out.data);
        this.historyRepository.insert(history);
    }

    @Inbound(values = "order-status-out")
    @Transactional(type = R)
    public void processOrderStatus(OrderStatusOut in) {
        Order order = this.orderRepository.getLastOrderByCustomerId(in.c_id, in.d_id, in.w_id);
        if(order == null){
            LOGGER.log(DEBUG, "No order found for customer "+in.c_id+"\n"+in);
            // order = this.orderRepository.getLastOrderByCustomerId(in.c_id, in.d_id, in.w_id);
            return;
        }
        List<OrderLineInfoDto> orderLinesInfo = this.orderLineRepository.getOrderLinesInfo(order.o_id, order.o_d_id, order.o_w_id);
        if(orderLinesInfo.isEmpty()){
            LOGGER.log(ERROR, "Input event OrderStatusOut led to empty order lines info:\n"+in);
        }
    }

    @Inbound(values = "new-order-inv-out")
    @Transactional(type = W)
    @Parallel
    public void processNewOrder(NewOrderInvOut in){

        Order order = new Order(
                in.d_next_o_id,
                in.d_id,
                in.w_id,
                in.c_id,
                new Date(),
                -1, // set in delivery tx
                in.itemsIds.length,
                in.allLocal ? 1 : 0
        );
        NewOrder newOrder = new NewOrder(in.d_next_o_id, in.d_id, in.w_id);

        this.orderRepository.insert(order);
        this.newOrderRepository.insert(newOrder);

        List<OrderLine> orderLinesToInsert = new ArrayList<>(in.itemsIds.length);

        for(int i = 0; i < in.itemsIds.length; i++){
            float ol_amount = (float) (in.qty[i] * in.itemsIds[i] * (1 + in.w_tax + in.d_tax) * (1 - in.c_discount));
            OrderLine orderLine = new OrderLine(
                    in.d_next_o_id,
                    in.d_id,
                    in.w_id,
                    i+1,
                    in.itemsIds[i],
                    in.supWares[i],
                    null,
                    in.qty[i],
                    ol_amount,
                    in.ol_dist_info[i]
            );
            orderLinesToInsert.add(i, orderLine);
        }
        this.orderLineRepository.insertAll(orderLinesToInsert);
    }

}
