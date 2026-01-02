package dk.ku.di.dms.vms.marketplace.product;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;

import java.util.List;
import java.util.Properties;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] ignoredArgs) throws Exception {
        Properties properties = ConfigUtils.loadProperties();
        VmsApplication vms = buildVms(properties);
        vms.start();
    }

    public static VmsApplication buildVms(Properties properties) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                Constants.PRODUCT_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.marketplace.product",
                        "dk.ku.di.dms.vms.marketplace.common"
                });
        return VmsApplication.build(options, (x,y) -> new ProductHttpHandler(x, (IProductRepository) y.apply("products")));
    }

    private static class ProductHttpHandler extends DefaultHttpHandler {
        private final IProductRepository repository;

        public ProductHttpHandler(ITransactionManager transactionManager,
                                  IProductRepository repository){
            super(transactionManager);
            this.repository = repository;
        }

        @Override
        public void post(String uri, String payload) {
            Product product = SERDES.deserialize(payload, Product.class);
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
            List<Product> products = this.repository.getAll();
            for(Product product : products){
                product.version = "0";
                this.repository.upsert(product);
            }
        }

        @Override
        public Object getAsJson(String uri) {
            String[] split = uri.split("/");
            int sellerId = Integer.parseInt(split[split.length - 2]);
            int productId = Integer.parseInt(split[split.length - 1]);
            this.transactionManager.beginTransaction(0, 0, 0,true);
            return this.repository.lookupByKey(new Product.ProductId(sellerId, productId));
        }

    }

}