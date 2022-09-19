package dk.ku.di.dms.vms.playground.app;

import dk.ku.di.dms.vms.modb.api.annotations.*;

@Microservice("example")
public class MicroserviceExample {

    private final RepositoryExample repository;

    public MicroserviceExample(RepositoryExample repository){
        this.repository = repository;
    }

    @Inbound(values = {"in"})
    @Outbound("out")
    @Transactional
    @Terminal
    public OutEventExample methodExample(EventExample in) {
        System.out.println("I am alive. The scheduler has scheduled me successfully!");
        return in != null ? new OutEventExample(in.id) : null;
    }

    @Inbound(values = {"in"})
    @Outbound("out2")
    @Transactional
    public OutEventExample2 methodExample1(EventExample in) {

        System.out.println("methodExample1");

        repository.insert(new EntityExample(in.id, in.id));

        return new OutEventExample2(in.id);
    }

}
