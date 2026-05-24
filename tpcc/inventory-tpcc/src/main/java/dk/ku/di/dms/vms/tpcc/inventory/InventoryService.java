package dk.ku.di.dms.vms.tpcc.inventory;

import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.tpcc.common.events.new_order.NewOrderInvOut;
import dk.ku.di.dms.vms.tpcc.common.events.new_order.NewOrderWareOut;
import dk.ku.di.dms.vms.tpcc.common.events.stock_level.StockLevelOrdOut;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Stock;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IItemRepository;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IStockRepository;

import java.util.ArrayList;
import java.util.List;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils.randomNumber;
import static java.lang.System.Logger.Level.*;

@Microservice("inventory")
public final class InventoryService {

    private static final System.Logger LOGGER = System.getLogger(InventoryService.class.getName());

    private final IItemRepository itemRepository;
    private final IStockRepository stockRepository;

    public InventoryService(IItemRepository itemRepository, IStockRepository stockRepository) {
        this.itemRepository = itemRepository;
        this.stockRepository = stockRepository;
    }

    private static final boolean STOCK_LEVEL_CORRECTNESS_CHECKING = false;

    @Inbound(values = "stock-level-ord-out")
    @Transactional(type = R)
    public void processStockLevel(StockLevelOrdOut in) {
        int threshold = randomNumber(10, 20);
        if(in.itemsIds.length == 0){
            LOGGER.log(ERROR, "Input event StockLevelOrdOut has empty item IDs:\n"+in);
            return;
        }
        int[] itemIds = this.stockRepository.getStockCount(in.itemsIds, in.w_id, threshold);
        if(itemIds.length == 0) {
            LOGGER.log(DEBUG, "Input event StockLevelOrdOut led to empty stock items:\n"+in);
            if(STOCK_LEVEL_CORRECTNESS_CHECKING) {
                // check if they can be queried and whether the filter is correct
                for(int i = 0; i < in.itemsIds.length; i++){
                    var stockItem = this.stockRepository.lookupByKey(new Stock.StockId(in.itemsIds[i], in.w_id));
                    if(stockItem == null) {
                        LOGGER.log(ERROR, "Stock Item Not Found for ID: " + in.itemsIds[i]);
                        continue;
                    }
                    if(stockItem.s_quantity < threshold){
                        LOGGER.log(WARNING, "Found stock item with quantity less than the threshold of " + threshold + "found ");
                    }
                }
            }
        }
    }

    @Inbound(values = "new-order-ware-out")
    @Outbound("new-order-inv-out")
    @Transactional(type = RW)
    @PartitionBy(clazz = NewOrderWareOut.class, method = "getId")
    public NewOrderInvOut processNewOrder(NewOrderWareOut in) {

        float[] prices = this.itemRepository.getPricePerItemId(in.itemsIds);
        String[] ol_dist_info = new String[in.itemsIds.length];
        List<Stock> stockItemsToUpdate = new ArrayList<>(prices.length);

        for(int i = 0; i < in.itemsIds.length; i++){
            Stock stock = this.stockRepository.lookupByKey(new Stock.StockId(in.itemsIds[i], in.supWares[i]));
            if(stock == null) {
                // LOGGER.log(ERROR, "Stock Item Not Found for ID: " + in.itemsIds[i]);
                throw new RuntimeException("Stock Item Not Found for ID: " + in.itemsIds[i]);
                // continue;
            }
            ol_dist_info[i] = stock.getDistInfo(in.d_id);
            int ol_quantity = in.qty[i];
            if(stock.s_quantity > ol_quantity){
                stock.s_quantity = stock.s_quantity - ol_quantity;
            } else {
                stock.s_quantity = stock.s_quantity - ol_quantity + 91;
            }
            stock.s_ytd = stock.s_ytd + ol_quantity;
            stock.s_order_cnt++;
            if(stock.s_w_id != in.w_id){
                stock.s_remote_cnt++;
            }
            stockItemsToUpdate.add(i, stock);
        }

        this.stockRepository.updateAll(stockItemsToUpdate);

        return new NewOrderInvOut(
            in.w_id,
            in.d_id,
            in.c_id,
            in.itemsIds,
            in.supWares,
            in.qty,
            in.allLocal,
            in.w_tax,
            in.d_next_o_id,
            in.d_tax,
            in.c_discount,
            prices,
            ol_dist_info
        );
    }

}
