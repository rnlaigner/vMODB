package dk.ku.di.dms.vms.marketplace.product;

import dk.ku.di.dms.vms.marketplace.common.events.PriceUpdated;
import dk.ku.di.dms.vms.marketplace.common.events.ProductUpdated;
import dk.ku.di.dms.vms.marketplace.common.inputs.PriceUpdate;
import dk.ku.di.dms.vms.marketplace.common.inputs.UpdateProduct;
import dk.ku.di.dms.vms.modb.api.annotations.*;

import java.util.Date;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

@Microservice("product")
public final class ProductService {

    private static final System.Logger LOGGER = System.getLogger(ProductService.class.getName());

    private final IProductRepository productRepository;

    public ProductService(IProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @Inbound(values = {UPDATE_PRODUCT})
    @Outbound(PRODUCT_UPDATED)
    @Transactional(type=RW)
    @PartitionBy(clazz = UpdateProduct.class, method = "getId")
    public ProductUpdated updateProduct(UpdateProduct updateProduct) {
        LOGGER.log(DEBUG, "APP:"+Thread.currentThread().threadId()+": Product received a product update event with version: "+updateProduct.version);

        Product oldProduct = this.productRepository.lookupByKey(new Product.ProductId(updateProduct.seller_id, updateProduct.product_id));
        if (oldProduct == null)
        {
            throw new RuntimeException("Product not found "+updateProduct.seller_id +"-"+updateProduct.product_id);
        }

        // can use issue statement for faster update
        Product product = new Product(updateProduct.seller_id, updateProduct.product_id, updateProduct.name, updateProduct.sku, updateProduct.category,
                updateProduct.description, updateProduct.price, updateProduct.freight_value, updateProduct.status, updateProduct.version);

        // keep the old
        product.created_at = oldProduct.created_at;
        product.updated_at = new Date();
        this.productRepository.update(product);

        return new ProductUpdated( updateProduct.seller_id, updateProduct.product_id, updateProduct.name, updateProduct.sku, updateProduct.category, updateProduct.description, updateProduct.price, updateProduct.freight_value, updateProduct.status, updateProduct.version);
    }

    @Inbound(values = {UPDATE_PRICE})
    @Outbound(PRICE_UPDATED)
    @Transactional(type=RW)
    @PartitionBy(clazz = PriceUpdate.class, method = "getId")
    public PriceUpdated updateProductPrice(PriceUpdate priceUpdate) {
        LOGGER.log(DEBUG, "APP: Product received an update price event with version: "+priceUpdate.instanceId);

        // could use issue statement for faster update
        Product product = this.productRepository.lookupByKey(new Product.ProductId(priceUpdate.sellerId, priceUpdate.productId));
        if (product == null)
        {
            throw new RuntimeException("Product not found "+priceUpdate.sellerId +"-"+priceUpdate.productId);
        }

        // check if versions match
        if (product.version.contentEquals(priceUpdate.version)) {
            product.price = priceUpdate.price;
            product.updated_at = new Date();
            this.productRepository.update(product);
        } else {
            LOGGER.log(WARNING,"APP: Product received an update price event with conflicting version: "+product.version+" != "+priceUpdate.version);
        }

        // must send because some cart items may have the old product
        return new PriceUpdated(priceUpdate.sellerId, priceUpdate.productId, priceUpdate.price, priceUpdate.version, priceUpdate.instanceId);
    }

}
