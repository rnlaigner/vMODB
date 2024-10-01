package dk.ku.di.dms.vms.marketplace.seller;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.seller.dtos.OrderSellerView;
import dk.ku.di.dms.vms.marketplace.seller.dtos.SellerDashboard;
import dk.ku.di.dms.vms.marketplace.seller.entities.OrderEntry;
import dk.ku.di.dms.vms.marketplace.seller.repositories.IOrderEntryRepository;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import java.util.List;
import java.util.Properties;

import static dk.ku.di.dms.vms.marketplace.seller.SellerService.EMPTY_DASHBOARD;
import static dk.ku.di.dms.vms.marketplace.seller.SellerService.SELLER_VIEW_BASE;
import static java.lang.System.Logger.Level.DEBUG;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    private static VmsApplication VMS;

    public static void main(String[] args) throws Exception {
        Properties properties = ConfigUtils.loadProperties();
        VMS = initVms(properties);
    }

    private static VmsApplication initVms(Properties properties) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                Constants.SELLER_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.seller",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        VmsApplication vms = VmsApplication.build(options,
                (x,z) -> new SellerHttpHandlerJdk2(x, (IOrderEntryRepository) z.apply("order_entries")));
        vms.start();
        return vms;
    }

    private static class SellerHttpHandlerJdk2 implements IHttpHandler {

        private final ITransactionManager transactionManager;
        private final IOrderEntryRepository repository;

        public SellerHttpHandlerJdk2(ITransactionManager transactionManager,
                                    IOrderEntryRepository repository){
            this.transactionManager = transactionManager;
            this.repository = repository;
        }

        @Override
        public void patch(String uri, String body) {
            this.transactionManager.reset();
        }

        @Override
        public String getAsJson(String uri) {
            String[] uriSplit = uri.split("/");
            int sellerId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
            long lastTid = VMS.lastTidFinished();
            var txCtx = this.transactionManager.beginTransaction(lastTid, 0, lastTid, true);
            List<OrderEntry> orderEntries = this.repository.getOrderEntriesBySellerId(sellerId);
            if(orderEntries.isEmpty()) return EMPTY_DASHBOARD.toString();
            LOGGER.log(DEBUG, "APP: Seller "+sellerId+" has "+orderEntries.size()+" entries in seller dashboard");
            OrderSellerView view = this.repository.fetchOne(SELLER_VIEW_BASE.setParam(sellerId), OrderSellerView.class);
            return new SellerDashboard(view, orderEntries).toString();
        }
    }

}
