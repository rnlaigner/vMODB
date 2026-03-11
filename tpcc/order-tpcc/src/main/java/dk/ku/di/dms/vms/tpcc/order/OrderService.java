package dk.ku.di.dms.vms.tpcc.order;

import dk.ku.di.dms.vms.modb.api.annotations.*;
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

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

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

    @Inbound(values = "stock-level-ware-out")
    @Outbound("stock-level-ord-out")
    @Transactional(type = R)
    public StockLevelOrdOut processStockLevel(StockLevelWareOut in) {
        // TODO: make parser process distinct and push it to query execution
        int[] orderIds = IntStream.range(in.next_o_id - 20, in.next_o_id - 1).distinct().toArray();
        int[] itemIds = this.orderLineRepository.getAllItemsByOrderIds(orderIds, in.d_id, in.w_id);
        return new StockLevelOrdOut(in.w_id, in.d_id, in.threshold, itemIds);
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
    public void processOrderStatus(OrderStatusOut in){
        Order order = this.orderRepository.getLastOrderByCustomerId(in.c_id);
        if(order == null){
            LOGGER.log(DEBUG, "No order found for customer "+in.c_id+"\n"+in);
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
