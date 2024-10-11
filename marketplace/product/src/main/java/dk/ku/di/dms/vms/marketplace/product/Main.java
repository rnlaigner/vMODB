package dk.ku.di.dms.vms.marketplace.product;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.product.infra.ProductDbUtils;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import java.util.Properties;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] ignoredArgs) throws Exception {
        Properties properties = ConfigUtils.loadProperties();
        initVms(properties);
    }

    public static VmsApplication initVms(Properties properties) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                Constants.PRODUCT_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.marketplace.product",
                        "dk.ku.di.dms.vms.marketplace.common"
                });
        VmsApplication vms = VmsApplication.build(options,
                (x,y) -> new ProductHttpHandlerJdk2(x, (IProductRepository) y.apply("products")));
        vms.start();
        return vms;
    }

    private static class ProductHttpHandlerJdk2 implements IHttpHandler {

        private final ITransactionManager transactionManager;
        private final IProductRepository repository;

        public ProductHttpHandlerJdk2(ITransactionManager transactionManager,
                                        IProductRepository repository){
            this.transactionManager = transactionManager;
            this.repository = repository;
        }

        @Override
        public void post(String uri, String payload) {
            Product product = ProductDbUtils.deserializeProduct(payload);
            this.transactionManager.beginTransaction(0, 0, 0, false);
            this.repository.upsert(product);
        }

        @Override
        public void patch(String uri, String body) {
            String[] uriSplit = uri.split("/");
            String op = uriSplit[uriSplit.length - 1];
            if(op.contentEquals("reset")){
                // path: /product/reset
                this.transactionManager.reset();
                return;
            }
            this.transactionManager.beginTransaction(0, 0, 0,false);
            var products = this.repository.getAll();
            for(Product product : products){
                product.version = "0";
                this.repository.upsert(product);
            }
        }

        @Override
        public String getAsJson(String uri) {
            String[] split = uri.split("/");
            int sellerId = Integer.parseInt(split[split.length - 2]);
            int productId = Integer.parseInt(split[split.length - 1]);
            this.transactionManager.beginTransaction(0, 0, 0,true);
            Product product = this.repository.lookupByKey(new Product.ProductId(sellerId, productId));
            return product.toString();
        }
    }

}