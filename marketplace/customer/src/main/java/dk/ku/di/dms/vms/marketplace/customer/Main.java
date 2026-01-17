package dk.ku.di.dms.vms.marketplace.customer;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;

public final class Main {

    private static final IVmsSerdesProxy SERDES = VmsSerdesProxyBuilder.build();

    public static void main(String[] args) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                Constants.CUSTOMER_VMS_PORT,
                new String[]{
                "dk.ku.di.dms.vms.marketplace.customer",
                "dk.ku.di.dms.vms.marketplace.common"}
        );
        VmsApplication vms = VmsApplication.build(options, (x,y) ->
                new CustomerHttpHandler(x, (ICustomerRepository) y.apply("customers")
        ));

        vms.start();
    }

    private static class CustomerHttpHandler extends DefaultHttpHandler {
        private final AbstractProxyRepository<Integer, Customer> repository;

        @SuppressWarnings("unchecked")
        public CustomerHttpHandler(ITransactionManager transactionManager,
                                   ICustomerRepository customerRepository){
            super(transactionManager);
            this.repository = (AbstractProxyRepository<Integer, Customer>) customerRepository;
        }

        @Override
        public void put(String uri, String body) {
            Customer customer = Main.SERDES.deserialize(body, Customer.class);
            Object[] obj = this.repository.extractFieldValuesFromEntityObject(customer);
            IKey key = KeyUtils.buildRecordKey( repository.getTable().schema().getPrimaryKeyColumns(), obj );
            this.repository.getTable().underlyingPrimaryKeyIndex().insert(key, obj);
        }
    }

}
