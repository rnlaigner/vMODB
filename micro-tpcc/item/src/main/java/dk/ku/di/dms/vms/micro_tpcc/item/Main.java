package dk.ku.di.dms.vms.micro_tpcc.item;

import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;

public class Main {

    private static final String[] DEFAULT_ARGS =
//              hostname   port   load data
            { "localhost", "1080", "true" };

    public static void main(String[] args) {
        VmsApplication.start("localhost", 8080, 8081, new String[]{
                "dk.ku.di.dms.vms.micro_tpcc.common",
                "dk.ku.di.dms.vms.micro_tpcc.item"
        });
    }

}