package dk.ku.di.dms.vms.micro_tpcc.warehouse;

import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;

public class Main {

    public static void main(String[] args){
        // https://forums.docker.com/t/python-asyncio-container-is-not-receiving-socket-connections/34018
        VmsApplication.start("0.0.0.0", 8082, 8083, new String[]{
                "dk.ku.di.dms.vms.micro_tpcc.common",
                "dk.ku.di.dms.vms.micro_tpcc.warehouse"
        },
            "customer","stock","item"   );
    }

}
