package dk.ku.di.dms.vms.tpcc.inventory.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Stock;

@Repository
public interface IStockRepository extends IRepository<Stock.StockId, Stock> {

    @Query("select distinct s_i_id from stock where s_i_id IN (:itemIds) and s_w_id = :w_id and s_quantity < :threshold")
    int[] getStockCount(int[] itemIds, int w_id, int threshold);

}