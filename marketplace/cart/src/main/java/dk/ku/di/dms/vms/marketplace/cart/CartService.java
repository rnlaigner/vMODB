package dk.ku.di.dms.vms.marketplace.cart;

import dk.ku.di.dms.vms.marketplace.cart.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.cart.entities.ProductReplica;
import dk.ku.di.dms.vms.marketplace.cart.infra.CartUtils;
import dk.ku.di.dms.vms.marketplace.cart.repositories.ICartItemRepository;
import dk.ku.di.dms.vms.marketplace.cart.repositories.IProductReplicaRepository;
import dk.ku.di.dms.vms.marketplace.common.events.PriceUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.ProductUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.marketplace.common.inputs.CustomerCheckout;
import dk.ku.di.dms.vms.modb.api.annotations.*;

import java.util.Date;
import java.util.List;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

@Microservice("cart")
public final class CartService {

    private static final System.Logger LOGGER = System.getLogger(CartService.class.getName());

    private final ICartItemRepository cartItemRepository;
    private final IProductReplicaRepository productReplicaRepository;

    public CartService(ICartItemRepository cartItemRepository,
                       IProductReplicaRepository productReplicaRepository) {
        this.cartItemRepository = cartItemRepository;
        this.productReplicaRepository = productReplicaRepository;
    }

    @Inbound(values = {CUSTOMER_CHECKOUT})
    @Outbound(RESERVE_STOCK)
    @Transactional(type=RW)
    @PartitionBy(clazz = CustomerCheckout.class, method = "getId")
    public ReserveStock checkout(CustomerCheckout checkout) {
        LOGGER.log(DEBUG, "APP: Cart received a checkout request for customer ID "+ checkout.CustomerId +" with TID: "+checkout.instanceId);
        // get cart items from the given customer
        List<CartItem> cartItems = //CartUtils.CART_ITEMS.remove(checkout.CustomerId);
                this.cartItemRepository.getCartItemsByCustomerId(checkout.CustomerId);
        if(cartItems == null || cartItems.isEmpty()) {
            LOGGER.log(ERROR, "APP: No cart items found for customer ID "+checkout.CustomerId+" TID: "+checkout.instanceId);
            // throw new RuntimeException("APP: No cart items found for customer ID "+checkout.CustomerId+" TID: "+checkout.instanceId);
            // this is only supposed to happen if the number of customers is
            // lower than the rate on which checkouts are submitted by the driver
            return new ReserveStock(new Date(), checkout, List.of(), checkout.instanceId);
        }
        this.cartItemRepository.deleteAll(cartItems);
        return new ReserveStock(new Date(), checkout, CartUtils.convertCartItems( cartItems ), checkout.instanceId);
    }

    /**
     * Partitioned execution can lead to abortion if conflicts with checkout
     * @ PartitionBy(clazz = PriceUpdated.class, method = "getId")
      */
    @Inbound(values = {PRICE_UPDATED})
    @Transactional(type=RW)
    public void updateProductPrice(PriceUpdated priceUpdated) {
        LOGGER.log(DEBUG, "APP: Cart received an update price event with version: "+priceUpdated.instanceId);
        // could use issue statement for faster update
        ProductReplica product = this.productReplicaRepository.lookupByKey(
                new ProductReplica.ProductId(priceUpdated.sellerId, priceUpdated.productId));
        if(product == null){
            LOGGER.log(DEBUG,"Cart has no product replica with seller ID "+priceUpdated.sellerId+" : product ID "+priceUpdated.productId);
        } else if (product.version.contentEquals(priceUpdated.version)) {
            product.price = priceUpdated.price;
            this.productReplicaRepository.update(product);
        }
        // update all carts
        List<CartItem> cartItems = this.cartItemRepository.getCartItemsBySellerIdAndProductIdAndVersion(
                priceUpdated.sellerId, priceUpdated.productId, priceUpdated.version);
        if(cartItems.isEmpty()) {
            LOGGER.log(DEBUG,"No cart items retrieved for price update:\n"+priceUpdated);
            return;
        }
        LOGGER.log(DEBUG,cartItems.size()+" cart items retrieved for price update:\n"+priceUpdated);
        for (var cartItem : cartItems) {
            cartItem.voucher += (priceUpdated.price - cartItem.unit_price);
            cartItem.unit_price = priceUpdated.price;
        }
        this.cartItemRepository.updateAll(cartItems);
    }

    /**
     * id being the [seller_id,product_id] is object causality
     * id being seller_id is seller causality
     */
    @Inbound(values = {PRODUCT_UPDATED})
    @Transactional(type=W)
    @PartitionBy(clazz = ProductUpdated.class, method = "getId")
    public void processProductUpdate(ProductUpdated productUpdated) {
        LOGGER.log(DEBUG, "APP: Cart received a product update event with version: "+productUpdated.version);
        ProductReplica product = new ProductReplica(productUpdated.seller_id, productUpdated.product_id, productUpdated.name, productUpdated.sku, productUpdated.category,
                productUpdated.description, productUpdated.price, productUpdated.freight_value, productUpdated.status, productUpdated.version);
        this.productReplicaRepository.upsert(product);
    }

}
