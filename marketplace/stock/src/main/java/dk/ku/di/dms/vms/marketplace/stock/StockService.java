package dk.ku.di.dms.vms.marketplace.stock;

import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.events.PaymentConfirmed;
import dk.ku.di.dms.vms.marketplace.common.events.ProductUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.marketplace.common.events.StockConfirmed;
import dk.ku.di.dms.vms.modb.api.annotations.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static java.lang.System.Logger.Level.*;

@Microservice("stock")
public final class StockService {

    private static final System.Logger LOGGER = System.getLogger(StockService.class.getName());

    private final IStockRepository stockRepository;

    public StockService(IStockRepository stockRepository) {
        this.stockRepository = stockRepository;
    }

    @Inbound(values = {PRODUCT_UPDATED})
    @Transactional(type=RW)
    @PartitionBy(clazz = ProductUpdated.class, method = "getId")
    public void updateProduct(ProductUpdated productUpdated) {
        LOGGER.log(DEBUG, "APP: Stock received an update product event with version: "+productUpdated.version);
        // can use issue statement for faster update
        StockItem stockItem = this.stockRepository.lookupByKey(new StockItem.StockId(productUpdated.seller_id, productUpdated.product_id));
        if (stockItem == null) {
            throw new RuntimeException("Stock item not found: "+productUpdated.seller_id+"-"+productUpdated.product_id);
        }
        stockItem.version = productUpdated.version;
        stockItem.updated_at = new Date();
        this.stockRepository.update(stockItem);
    }

    /**
     * This case can be optimized by locking the items prior to starting the transaction
     * The task is only submitted to run if all locks have been acquired. This prevents possible conflicts.
     */
    @Inbound(values = {RESERVE_STOCK})
    @Outbound(STOCK_CONFIRMED)
    @Transactional(type=RW)
    public StockConfirmed reserveStock(ReserveStock reserveStock){
        LOGGER.log(DEBUG, "APP: Stock received a reserve stock event with TID: "+reserveStock.instanceId);
        Map<StockItem.StockId, CartItem> cartItemMap = reserveStock.items.stream().collect( Collectors.toMap( (f)-> new StockItem.StockId(f.SellerId, f.ProductId), Function.identity()) );
        List<StockItem> items = this.stockRepository.lookupByKeys(cartItemMap.keySet());
        if(items.isEmpty()) {
            LOGGER.log(ERROR, "APP: No items found in private state.\nTID "+reserveStock.instanceId+"\nCustomer ID "+reserveStock.customerCheckout.CustomerId);
        }
        List<CartItem> unavailableItems = new ArrayList<>(reserveStock.items.size());
        List<CartItem> cartItemsReserved = new ArrayList<>(reserveStock.items.size());
//        List<StockItem> stockItemsReserved = new(); // for bulk update
        var now = new Date();
        StockItem.StockId currId;
        for(StockItem item : items){
            currId = new StockItem.StockId( item.seller_id, item.product_id );
            CartItem cartItem = cartItemMap.get( currId );
            if (item.version.compareTo(cartItem.Version) != 0) {
                LOGGER.log(DEBUG,"The stock item ("+item.seller_id+"-"+item.product_id+") version is incorrect.\n" +
                        "Stock item: "+ item.version+ " Cart item: "+cartItem.Version);
                unavailableItems.add(cartItem);
                continue;
            }
            if(item.qty_reserved + cartItem.Quantity > item.qty_available){
                LOGGER.log(DEBUG,"The stock item ("+item.seller_id+"-"+item.product_id+") no longer available.\n" +
                        "Stock item: "+ item.version+ " Cart item: "+cartItem.Version);
                unavailableItems.add(cartItem);
                continue;
            }
            item.qty_reserved += cartItem.Quantity;
            item.updated_at = now;
            this.stockRepository.update( item );
            cartItemsReserved.add(cartItem);
        }

        if(cartItemsReserved.isEmpty()){
            LOGGER.log(WARNING, "APP: No items were reserved for instanceId = "+reserveStock.instanceId);
            if(!unavailableItems.isEmpty()) {
                LOGGER.log(WARNING, "APP: Unavailable items:\n"+unavailableItems);
            }
        }
        // LOGGER.log(DEBUG, "APP: Stock finished a reserve stock event with TID: "+reserveStock.instanceId);
        // need to find a way to complete the transaction in the case it does not hit all virtual microservices
        return new StockConfirmed( reserveStock.timestamp, reserveStock.customerCheckout, cartItemsReserved, reserveStock.instanceId );
    }

//    @Inbound(values = {PAYMENT_CONFIRMED})
//    @Transactional(type=RW)
    public void processPaymentConfirmed(PaymentConfirmed paymentConfirmed){

    }

}
