package dk.ku.di.dms.vms.marketplace.seller;

import dk.ku.di.dms.vms.marketplace.common.entities.CustomerCheckout;
import dk.ku.di.dms.vms.marketplace.common.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.common.events.InvoiceIssued;
import dk.ku.di.dms.vms.marketplace.seller.entities.Seller;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static java.lang.Thread.sleep;

public final class SellerTest {

    private static final int MAX_SELLERS = 10;

    @Test
    @SuppressWarnings("unchecked")
    public void testParallelInvoiceIssued() throws Exception {

        VmsApplication vms = getVmsApplication();
        vms.start();

        var sellerTable = vms.getTable("sellers");
        var sellerRepository = (AbstractProxyRepository<Integer, Seller>) vms.getRepositoryProxy("sellers");

        // add sellers first to avoid foreign key constraint violation
        for(int i = 1; i <= MAX_SELLERS; i++){
            var seller = new Seller(i, "test", "test", "test",
                    "test", "test", "test", "test",
                    "test", "test", "test", "test", "test");
            Object[] obj = sellerRepository.extractFieldValuesFromEntityObject(seller);
            IKey key = KeyUtils.buildRecordKey( sellerTable.schema().getPrimaryKeyColumns(), obj );
            sellerTable.underlyingPrimaryKeyIndex().insert(key, obj);
        }

        CustomerCheckout customerCheckout = new CustomerCheckout(
                1, "test", "test", "test", "test","test", "test", "test",
                "CREDIT_CARD","test","test","test", "test", "test", 1,"1");

        for(int i = 1; i <= MAX_SELLERS; i++) {
            InvoiceIssued invoiceIssued = new InvoiceIssued( customerCheckout, i,  "test", new Date(), 100,
                    List.of(new OrderItem(i,1,1, "name",
                            i, 1.0f, new Date(), 1.0f, 1, 1.0f, 1.0f, 0.0f) )
                    , String.valueOf(i));

            InboundEvent inboundEvent = new InboundEvent(i, i-1, 1,
                    "invoice_issued", InvoiceIssued.class, invoiceIssued);
            vms.internalChannels().transactionInputQueue().add(inboundEvent);
        }

        sleep(1000000);

    }

    private static VmsApplication getVmsApplication() throws Exception {
        return VmsApplication.build("localhost", 8087, new String[]{
                "dk.ku.di.dms.vms.marketplace.seller",
                "dk.ku.di.dms.vms.marketplace.common"
        });
    }

}
