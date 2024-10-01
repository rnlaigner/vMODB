package dk.ku.di.dms.vms.marketplace.order;

import dk.ku.di.dms.vms.marketplace.common.enums.OrderStatus;
import dk.ku.di.dms.vms.marketplace.common.enums.ShipmentStatus;
import dk.ku.di.dms.vms.marketplace.common.events.*;
import dk.ku.di.dms.vms.marketplace.order.entities.CustomerOrder;
import dk.ku.di.dms.vms.marketplace.order.entities.Order;
import dk.ku.di.dms.vms.marketplace.order.entities.OrderHistory;
import dk.ku.di.dms.vms.marketplace.order.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.order.repositories.ICustomerOrderRepository;
import dk.ku.di.dms.vms.marketplace.order.repositories.IOrderHistoryRepository;
import dk.ku.di.dms.vms.marketplace.order.repositories.IOrderItemRepository;
import dk.ku.di.dms.vms.marketplace.order.repositories.IOrderRepository;
import dk.ku.di.dms.vms.modb.api.annotations.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static java.lang.System.Logger.Level.DEBUG;

@Microservice("order")
public final class OrderService {

    private static final System.Logger LOGGER = System.getLogger(OrderService.class.getName());

    private final IOrderRepository orderRepository;

    private final ICustomerOrderRepository customerOrderRepository;

    private final IOrderItemRepository orderItemRepository;

    private final IOrderHistoryRepository orderHistoryRepository;

    public OrderService(IOrderRepository orderRepository,
                        ICustomerOrderRepository customerOrderRepository,
                        IOrderItemRepository orderItemRepository,
                        IOrderHistoryRepository orderHistoryRepository) {
        this.orderRepository = orderRepository;
        this.customerOrderRepository = customerOrderRepository;
        this.orderItemRepository = orderItemRepository;
        this.orderHistoryRepository = orderHistoryRepository;
    }

    @Inbound(values = {STOCK_CONFIRMED})
    @Outbound(INVOICE_ISSUED)
    @Transactional(type=RW)
    @PartitionBy(clazz = StockConfirmed.class, method = "getCustomerId")
    public InvoiceIssued processStockConfirmed(StockConfirmed stockConfirmed) {
        Date now = new Date();
        LOGGER.log(DEBUG, "APP: Order received a stock confirmed event with TID: "+ stockConfirmed.instanceId);

        // calculate total freight_value
        float total_freight = 0;
        float total_amount = 0;
        for(var item : stockConfirmed.items)
        {
            total_freight += item.FreightValue;
            total_amount += (item.UnitPrice * item.Quantity);
        }

        // total before discounts
        float total_items = total_amount;

        Map<Integer, Float> totalPerItem = new HashMap<>();
        float total_incentive = 0;
        for(var item : stockConfirmed.items)
        {
            float total_item = item.UnitPrice * item.Quantity;
            if (total_item - item.Voucher > 0)
            {
                total_amount -= item.Voucher;
                total_incentive += item.Voucher;
                total_item -= item.Voucher;
            }
            else
            {
                total_amount -= total_item;
                total_incentive += total_item;
                total_item = 0;
            }
            totalPerItem.put(item.ProductId, total_item);
        }

        CustomerOrder customerOrder = this.customerOrderRepository.lookupByKey( stockConfirmed.customerCheckout.CustomerId );
        if (customerOrder == null)
        {
            customerOrder = new CustomerOrder(stockConfirmed.customerCheckout.CustomerId, 1);
            this.customerOrderRepository.insert(customerOrder);
        } else {
            customerOrder.next_order_id++;
            this.customerOrderRepository.update(customerOrder);
        }

        String invoiceNumber = buildInvoiceNumber( stockConfirmed.customerCheckout.CustomerId, now, customerOrder.next_order_id );

        Order order = new Order(
                 customerOrder.customer_id,
                 customerOrder.next_order_id,
                 invoiceNumber,
                 OrderStatus.INVOICED,
                 stockConfirmed.timestamp,
                 null,
                 null,
                 null,
                 null,
                 stockConfirmed.items.size(),
                 total_amount,
                 total_freight,
                 total_incentive,
                 total_amount + total_freight,
                 total_items,
                 now,
                 now
        );
        this.orderRepository.insert(order);

        Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.DATE, 3);

        List<dk.ku.di.dms.vms.marketplace.common.entities.OrderItem> orderItems = new ArrayList<>();
        int item_id = 1;
        for(var item : stockConfirmed.items)
        {
            OrderItem oim = new OrderItem
            (
                customerOrder.customer_id,
                order.order_id,
                item_id,
                item.ProductId,
                item.ProductName,
                item.SellerId,
                item.UnitPrice,
                cal.getTime(),
                item.FreightValue,
                item.Quantity,
                item.UnitPrice * item.Quantity,
                totalPerItem.get(item.ProductId)
            );
            orderItems.add(oim.toCommonOrderItem(item.Voucher));
            this.orderItemRepository.insert(oim);
            item_id++;
        }

        OrderHistory oh = new OrderHistory(
                customerOrder.customer_id,
                customerOrder.next_order_id,
                order.created_at,
                OrderStatus.INVOICED);
        this.orderHistoryRepository.insert(oh);

        return new InvoiceIssued( stockConfirmed.customerCheckout, customerOrder.next_order_id, invoiceNumber,
                now, order.total_invoice, orderItems, stockConfirmed.instanceId);
    }

    private static final DateFormat df = new SimpleDateFormat("MM/dd/yyyy");

    public static String buildInvoiceNumber(int customerId, Date timestamp, int orderId)
    {
        return String.valueOf(customerId) + '-' +
                df.format(timestamp) + '-' +
                orderId;
    }

    //    @Inbound(values = {PAYMENT_CONFIRMED})
//    @Transactional(type=RW)
    public void processPaymentConfirmed(PaymentConfirmed paymentConfirmed){

    }

    @Inbound(values = {SHIPMENT_UPDATED})
    @Transactional(type=RW)
    public void processShipmentNotification(ShipmentUpdated shipmentUpdated){
        LOGGER.log(DEBUG, "APP: Order received a shipment updated event with TID: "+shipmentUpdated.instanceId);

        Date now = new Date();
        for(ShipmentNotification shipmentNotification : shipmentUpdated.shipmentNotifications) {
            Order order = this.orderRepository.lookupByKey( new Order.OrderId(
                    shipmentNotification.customerId, shipmentNotification.orderId
            ) );
            if(order == null) {
                throw new RuntimeException("Cannot find order "+shipmentNotification.customerId+"-"+shipmentNotification.orderId);
            }

            OrderStatus status = OrderStatus.READY_FOR_SHIPMENT;
            if(shipmentNotification.status == ShipmentStatus.DELIVERY_IN_PROGRESS) {
                status = OrderStatus.IN_TRANSIT;
                order.delivered_carrier_date = shipmentNotification.eventDate;
            } else if(shipmentNotification.status == ShipmentStatus.CONCLUDED) {
                status = OrderStatus.DELIVERED;
                order.delivered_customer_date = shipmentNotification.eventDate;
            }

            OrderHistory orderHistory = new OrderHistory(
                    order.customer_id,
                    order.order_id,
                    now,
                    status);

            order.status = status;
            order.updated_at = now;

            this.orderRepository.update(order);
            this.orderHistoryRepository.insert( orderHistory );
        }
    }

}
