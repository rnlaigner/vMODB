package dk.ku.di.dms.vms.marketplace.stock;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;

import java.lang.System.Logger;
import java.util.List;
import java.util.Properties;

public final class Main {

    private static final Logger LOGGER = System.getLogger(Main.class.getName());

    private static VmsApplication VMS;

    public static void main(String[] ignoredArgs) throws Exception {
        Properties properties = ConfigUtils.loadProperties();
        VMS = buildVms(properties);
        VMS.start();
    }

    private static VmsApplication buildVms(Properties properties) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                Constants.STOCK_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.marketplace.stock",
                        "dk.ku.di.dms.vms.marketplace.common"
                });
        return VmsApplication.build(options,
                (x,y) -> new StockHttpHandler(x, (IStockRepository) y.apply("stock_items")));
    }
    
    private static class StockHttpHandler extends DefaultHttpHandler {

        private final IStockRepository repository;

        public StockHttpHandler(ITransactionManager transactionManager,
                                IStockRepository repository){
            super(transactionManager);
            this.repository = repository;
        }

        @Override
        public void post(String uri, String body) {
            StockItem stockItem = SERDES.deserialize(body, StockItem.class);
            this.transactionManager.beginTransaction(0, 0, 0, false);
            this.repository.upsert(stockItem);
        }

        @Override
        public void patch(String uri, String body) {
            final String[] uriSplit = uri.split("/");
            String op = uriSplit[uriSplit.length - 1];
            // path: /stock/reset
            if(op.contentEquals("reset")){
                this.transactionManager.reset();
                return;
            }
            // path: /stock/cleanup
            long lastTid = VMS.lastTidFinished();
            this.transactionManager.beginTransaction(lastTid, 0, lastTid, false);
            List<StockItem> stockItems = this.repository.getAll();
            this.transactionManager.reset();
            this.transactionManager.beginTransaction(0, 0, 0,false);
            for(StockItem item : stockItems){
                item.qty_available = 10000;
                item.version = "0";
                item.qty_reserved = 0;
                this.repository.insert(item);
            }
        }

        @Override
        public Object getAsJson(String uri) {
            final String[] uriSplit = uri.split("/");
            int sellerId = Integer.parseInt(uriSplit[uriSplit.length - 2]);
            int productId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
            this.transactionManager.beginTransaction(0, 0, 0, true);
            return this.repository.lookupByKey(new StockItem.StockId(sellerId, productId));
        }
    }

}