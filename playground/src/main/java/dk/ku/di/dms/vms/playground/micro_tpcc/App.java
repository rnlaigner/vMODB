package dk.ku.di.dms.vms.playground.micro_tpcc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class App {

    static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main( String[] args ) throws IOException, ExecutionException, InterruptedException {

        // initialize virtual microservices
        // cannot initialize them here otherwise the events get mixed

        // bulk data load if necessary
        DataGenerator dataLoader = new DataGenerator();

        dataLoader.start("warehouse","district");

        // start generating new order transaction inputs


        // process inputs processed and convert into input that coordinator understands



    }

}
