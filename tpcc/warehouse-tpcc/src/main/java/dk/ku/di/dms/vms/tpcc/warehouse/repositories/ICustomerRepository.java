package dk.ku.di.dms.vms.tpcc.warehouse.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Customer;

import java.util.List;

@Repository
public interface ICustomerRepository extends IRepository<Customer.CustomerId, Customer> {

    @Query("select c_discount from customer where c_id = :c_id and c_d_id = :d_id and c_w_id = :d_w_id")
    float getDiscount(int c_id, int d_id, int w_id);

    @Query("select * from customer where c_d_id = :d_id and c_w_id = :w_id and c_last = :c_last order by c_first")
    List<Customer> getCustomerByLastName(int d_id, int w_id, String c_last);

    @Query("select * from customer where c_d_id = :d_id and c_w_id = :w_id order by c_id desc")
    List<Customer> getCustomers(int d_id, int w_id);

}