package dk.ku.di.dms.vms.marketplace.customer;

import dk.ku.di.dms.vms.marketplace.common.entities.CustomerCheckout;
import dk.ku.di.dms.vms.marketplace.common.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.common.enums.PackageStatus;
import dk.ku.di.dms.vms.marketplace.common.events.DeliveryNotification;
import dk.ku.di.dms.vms.marketplace.common.events.PaymentConfirmed;
import dk.ku.di.dms.vms.marketplace.common.events.ShipmentUpdated;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static java.lang.Thread.sleep;

public class CustomerTest {

    @Test
    public void paymentTest() throws Exception {

        VmsApplication vms = VmsApplication.build("localhost", 8086, new String[]{
                "dk.ku.di.dms.vms.marketplace.customer",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        vms.start();

        CustomerCheckout customerCheckout = new CustomerCheckout(
                1, "test", "test", "test", "test","test", "test", "test",
                "CREDIT_CARD","test","test","test", "test", "test", 1,"1");

        PaymentConfirmed paymentConfirmed = new PaymentConfirmed(customerCheckout, 1, 100f,
                List.of(new OrderItem(1,1,1, "name", 1, 1.0f, new Date(), 1.0f, 1, 1.0f, 1.0f, 0.0f) ),
                new Date(), "1");

        InboundEvent inboundEvent = new InboundEvent( 1, 0, 1,
                "payment_confirmed", PaymentConfirmed.class, paymentConfirmed );
        vms.internalChannels().transactionInputQueue().add( inboundEvent );

        sleep(100000);

    }


    @Test
    public void shipmentTest() throws Exception {

        VmsApplication vms = VmsApplication.build("localhost", 8086, new String[]{
                "dk.ku.di.dms.vms.marketplace.customer",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        vms.start();

        ShipmentUpdated shipmentUpdated = new ShipmentUpdated(
                List.of( new DeliveryNotification( 1, 1, 1, 1, 1, "test", PackageStatus.delivered, new Date() ) ),
                null,
                "1"
        );

        InboundEvent inboundEvent = new InboundEvent( 1, 0, 1,
                "shipment_updated", ShipmentUpdated.class, shipmentUpdated );
        vms.internalChannels().transactionInputQueue().add( inboundEvent );

        sleep(100000);

    }

}
