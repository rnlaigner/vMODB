package dk.ku.di.dms.vms.marketplace.seller;

import dk.ku.di.dms.vms.marketplace.common.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.common.enums.OrderStatus;
import dk.ku.di.dms.vms.marketplace.common.enums.PackageStatus;
import dk.ku.di.dms.vms.marketplace.common.enums.ShipmentStatus;
import dk.ku.di.dms.vms.marketplace.common.events.DeliveryNotification;
import dk.ku.di.dms.vms.marketplace.common.events.InvoiceIssued;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentNotification;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentUpdated;
import dk.ku.di.dms.vms.marketplace.seller.dtos.OrderSellerView;
import dk.ku.di.dms.vms.marketplace.seller.dtos.SellerDashboard;
import dk.ku.di.dms.vms.marketplace.seller.entities.OrderEntry;
import dk.ku.di.dms.vms.marketplace.seller.repositories.IOrderEntryRepository;
import dk.ku.di.dms.vms.marketplace.seller.repositories.ISellerRepository;
import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.util.ArrayList;
import java.util.List;

import static dk.ku.di.dms.vms.marketplace.common.Constants.INVOICE_ISSUED;
import static dk.ku.di.dms.vms.marketplace.common.Constants.SHIPMENT_UPDATED;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;
import static java.lang.System.Logger.Level.DEBUG;

@Microservice("seller")
public final class SellerService {

    private static final System.Logger LOGGER = System.getLogger(SellerService.class.getName());

    // necessary to force vms loader to load this repository
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final ISellerRepository sellerRepository;

    private final IOrderEntryRepository orderEntryRepository;

    @VmsPreparedStatement("sellerDashboard")
    public static final SelectStatement SELLER_VIEW_BASE = QueryBuilderFactory.select()
            .project("seller_id").sum("total_amount").sum("freight_value")
            .sum("total_incentive").sum("total_invoice").sum("total_items")
            .count("c_o_id").count("seller_id")
            .from("order_entries").where("seller_id", ExpressionTypeEnum.EQUALS, ":sellerId")
            .groupBy( "seller_id" ).build();

    public SellerService(ISellerRepository sellerRepository, IOrderEntryRepository orderEntryRepository){
        this.sellerRepository = sellerRepository;
        this.orderEntryRepository = orderEntryRepository;
    }

    @Inbound(values = INVOICE_ISSUED)
    @Transactional(type=W)
    @Parallel
    public void processInvoiceIssued(InvoiceIssued invoiceIssued){
        LOGGER.log(DEBUG, "APP: Seller received an invoice issued event with TID: "+ invoiceIssued.instanceId);
        List<OrderItem> orderItems = invoiceIssued.getItems();
        List<OrderEntry> entries = new ArrayList<>(orderItems.size());
        for (OrderItem orderItem : orderItems) {
            float totalInvoice = orderItem.total_amount + orderItem.getFreightValue();
            OrderEntry entry = new OrderEntry(
                    invoiceIssued.customer.CustomerId,
                    invoiceIssued.orderId,
                    orderItem.product_id,
                    orderItem.seller_id,
                    -1, // unknown at this point
                    orderItem.product_name,
                    "",
                    orderItem.unit_price,
                    orderItem.quantity,
                    orderItem.total_items,
                    orderItem.total_amount,
                    totalInvoice,
                    orderItem.total_incentive,
                    orderItem.freight_value,
                    null,
                    null,
                    OrderStatus.INVOICED,
                    PackageStatus.created
            );
            entries.add(entry);
        }
        this.orderEntryRepository.insertAll(entries);
    }

    @Inbound(values = SHIPMENT_UPDATED)
    @Transactional(type=RW)
    public void processShipmentUpdate(ShipmentUpdated shipmentUpdated){
        LOGGER.log(DEBUG, "APP: Seller received a shipment update event with TID: "+ shipmentUpdated.instanceId);
        // synchronization must also be present here
        for(ShipmentNotification shipmentNotification : shipmentUpdated.shipmentNotifications) {
            List<OrderEntry> orderEntries = this.orderEntryRepository.getOrderEntriesByCustomerIdAndOrderId(
                    shipmentNotification.customerId, shipmentNotification.orderId );
            for(OrderEntry entry : orderEntries){
                if(entry.delivery_status == PackageStatus.delivered) continue;
                switch (shipmentNotification.status) {
                    case ShipmentStatus.APPROVED -> {
                        entry.order_status = OrderStatus.READY_FOR_SHIPMENT;
                        entry.shipment_date = shipmentNotification.eventDate;
                        entry.delivery_status = PackageStatus.ready_to_ship;
                    }
                    case ShipmentStatus.DELIVERY_IN_PROGRESS -> {
                        entry.order_status = OrderStatus.IN_TRANSIT;
                        entry.delivery_status = PackageStatus.shipped;
                    }
                    case ShipmentStatus.CONCLUDED -> // remove entry from view
                            entry.order_status = OrderStatus.DELIVERED;
                    default -> throw new IllegalStateException("Unexpected value: " + shipmentNotification.status);
                }
                this.orderEntryRepository.update( entry );
            }
        }
        for(DeliveryNotification delivery : shipmentUpdated.deliveryNotifications) {
            OrderEntry orderEntry = this.orderEntryRepository.lookupByKey(
                    new OrderEntry.OrderEntryId( delivery.customerId, delivery.orderId, delivery.sellerId, delivery.productId ) );

            if(orderEntry.delivery_status == PackageStatus.delivered) continue;

            orderEntry.delivery_status = delivery.packageStatus;
            orderEntry.delivery_date = delivery.deliveryDate;
            orderEntry.package_id = delivery.packageId;
            this.orderEntryRepository.update( orderEntry );
        }
    }

    public static final SellerDashboard EMPTY_DASHBOARD = new SellerDashboard(new OrderSellerView(), List.of());

    /**
     * Parallel by default -- http clients trigger it
     */
    public SellerDashboard queryDashboard(int sellerId){
        LOGGER.log(DEBUG, "APP: Seller received a seller dashboard request for ID: "+ sellerId);
        List<OrderEntry> orderEntries = this.orderEntryRepository.getOrderEntriesBySellerId(sellerId);
        if(orderEntries.isEmpty()) return EMPTY_DASHBOARD;
        LOGGER.log(DEBUG, "APP: Seller "+sellerId+" has "+orderEntries.size()+" entries in seller dashboard");
        OrderSellerView view = this.orderEntryRepository.fetchOne(SELLER_VIEW_BASE.setParam(sellerId), OrderSellerView.class);
        // LOGGER.log(DEBUG, view);
        return new SellerDashboard(view, orderEntries);
    }

}
