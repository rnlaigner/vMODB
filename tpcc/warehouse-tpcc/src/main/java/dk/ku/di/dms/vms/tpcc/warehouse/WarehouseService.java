package dk.ku.di.dms.vms.tpcc.warehouse;

import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.tpcc.common.events.*;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Customer;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.District;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Warehouse;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IWarehouseRepository;

import java.util.List;
import java.util.Objects;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static java.lang.System.Logger.Level.WARNING;

@Microservice("warehouse")
public final class WarehouseService {

    private static final System.Logger LOGGER = System.getLogger(WarehouseService.class.getName());

    private final IWarehouseRepository warehouseRepository;
    private final IDistrictRepository districtRepository;
    private final ICustomerRepository customerRepository;

    public WarehouseService(IWarehouseRepository warehouseRepository, IDistrictRepository districtRepository, ICustomerRepository customerRepository){
        this.warehouseRepository = warehouseRepository;
        this.districtRepository = districtRepository;
        this.customerRepository = customerRepository;
    }

    @Inbound(values = "payment-in")
    @Outbound("payment-out")
    @Transactional(type = RW)
    @PartitionBy(clazz = PaymentIn.class, method = "getId")
    public PaymentOut processPayment(PaymentIn in) {

        District district = this.districtRepository.lookupByKey(new District.DistrictId(in.d_id, in.w_id));
        district.d_ytd += in.amount;
        Warehouse warehouse = this.warehouseRepository.lookupByKey(in.w_id);
        warehouse.w_ytd += in.amount;

        this.districtRepository.update(district);
        this.warehouseRepository.update(warehouse);

        Customer customer;
        if(in.by_name){
            List<Customer> customers = this.customerRepository.getCustomerByLastName(in.c_d_id, in.c_w_id, in.c_last);
            if(customers.isEmpty()){
                throw new RuntimeException("Empty customer list\nc_d_id: %d c_w_id: %d c_last: %s\n".formatted(in.c_d_id, in.c_w_id, in.c_last));
            }
            int index = customers.size() / 2;
            if (customers.size() % 2 == 0) {
                index -= 1;
            }
            customer = customers.get(index);
            // LOGGER.log(DEBUG, customers);
        } else {
            customer = this.customerRepository.lookupByKey(new Customer.CustomerId(in.c_id, in.c_d_id, in.c_w_id));
            // LOGGER.log(DEBUG, customer);
        }

        if (customer.c_credit.equals("BC")) {
            customer.c_data = "%d %d %d %d %d %f | %s".formatted(customer.c_id, in.c_d_id, in.c_w_id, in.d_id, in.w_id, in.amount, customer.c_data);
            if (customer.c_data.length() > 500) {
                customer.c_data = customer.c_data.substring(0, 500);
            }
        }

        customer.c_balance -= in.amount;
        customer.c_ytd_payment += in.amount;
        customer.c_payment_cnt += 1;
        this.customerRepository.update(customer);

        String h_data = "%s    %s".formatted(warehouse.w_name.length() > 10 ? warehouse.w_name.substring(0, 10) : warehouse.w_name, district.d_name.length() > 10 ? district.d_name.substring(0, 10) : district.d_name);

        return new PaymentOut(in.w_id, in.d_id, in.c_id, in.c_w_id, in.c_d_id, in.amount, h_data);
    }

    @Inbound(values = "order-status-in")
    @Outbound("order-status-out")
    @Transactional(type = R)
    public OrderStatusOut processOrderStatus(OrderStatusIn in) {
        if(in.by_name){
            List<Customer> customers = this.customerRepository.getCustomerByLastName(in.d_id, in.w_id, in.c_last);
            Objects.requireNonNull(customers);
            if(customers.isEmpty()){
                LOGGER.log(WARNING, "No customers retrieved by last name with input:\n"+in);
                customers = this.customerRepository.getCustomerByLastName(in.d_id, in.w_id, in.c_last);
            }
        } else {
            Customer customer = this.customerRepository.lookupByKey(new Customer.CustomerId(in.c_id, in.d_id, in.w_id));
            if(customer == null){
                LOGGER.log(WARNING, "No customer retrieved with input:\n"+in);
                customer = this.customerRepository.lookupByKey(new Customer.CustomerId(in.c_id, in.d_id, in.w_id));
            }
        }
        return new OrderStatusOut(in.w_id, in.d_id, in.c_id);
    }

    @Inbound(values = "new-order-ware-in")
    @Outbound("new-order-ware-out")
    @Transactional(type = RW)
    @PartitionBy(clazz = NewOrderWareIn.class, method = "getId")
    public NewOrderWareOut processNewOrder(NewOrderWareIn in) {

        District district = this.districtRepository.lookupByKey(new District.DistrictId(in.d_id, in.w_id));
        float w_tax = this.warehouseRepository.getWarehouseTax(in.w_id);

        district.d_next_o_id++;
        this.districtRepository.update(district);

        float c_discount = this.customerRepository.getDiscount(in.c_id, in.d_id, in.w_id);

        return new NewOrderWareOut(
                in.w_id,
                in.d_id,
                in.c_id,
                in.itemsIds,
                in.supWares,
                in.qty,
                in.allLocal,
                w_tax,
                district.d_next_o_id,
                district.d_tax,
                c_discount
        );
    }

}