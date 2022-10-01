package dk.ku.di.dms.vms.micro_tpcc.service;

import dk.ku.di.dms.vms.micro_tpcc.events.ItemNewOrderIn;
import dk.ku.di.dms.vms.micro_tpcc.events.ItemNewOrderOut;
import dk.ku.di.dms.vms.micro_tpcc.repository.IItemRepository;
import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;
import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;

import java.util.List;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;

@Microservice("item")
public class ItemService {

    private static final Class<Tuple<Integer, Float>> fetchType;

    static {
        Tuple<Integer, Float> res = new Tuple<>(0,0F);
        fetchType = (Class<Tuple<Integer, Float>>) res.getClass();
    }

    private final IItemRepository itemRepository;

    public ItemService(IItemRepository itemRepository){
        this.itemRepository = itemRepository;
    }

//    @Inbound(values = "item-new-order-in")
//    @Outbound("item-new-order-out")
//    @Transactional(type=R)
//    public ItemNewOrderOut getItemsById(ItemNewOrderIn itemNewOrderIn){
//
//        SelectStatement selectStatement = QueryBuilderFactory.select()
//                .select("i.i_id, i.i_price")
//                .from("item")
//                .where("s_i_id", ExpressionTypeEnum.IN, itemNewOrderIn.itemsIds()).build();
//
//        List<Tuple<Integer, Float>> res = itemRepository.fetchMany(selectStatement, fetchType );
//
//        return new ItemNewOrderOut(null,null);
//    }

    @Inbound(values = "item-new-order-in")
    @Outbound("item-new-order-out")
    @Transactional(type=R)
    public ItemNewOrderOut getItemsById(ItemNewOrderIn itemNewOrderIn){

        Tuple<int[], float[]> res = itemRepository.getItemsById(itemNewOrderIn.itemsIds());

        return new ItemNewOrderOut(res.getT1(), res.getT2());
    }

}
