package dk.ku.di.dms.vms.coordinator.transaction;

/**
 * This is purely for parsing transaction requests coming from HTTP clients
 * The name of the event is not always the name of the transaction
 * An internal VMs, for example, is subject to these phenomena
 * (e.g., update shipment transaction and the invoice issued event)
 */
public final class TransactionInput {

    // name of the transaction
    public final String name;

    public final Event event;

    public TransactionInput(String name, Event event) {
        this.name = name;
        this.event = event;
    }

    public static class Event {
        public String name;
        public String payload;

        public Event(String name, String payload) {
            this.name = name;
            this.payload = payload;
        }

        @Override
        public String toString() {
            return "{"
                    + "\"name\":\"" + name + "\""
                    + ",\"payload\":" + payload
                    + "}";
        }
    }

    // for stream operations
    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return "{"
                + "\"name\":\"" + name + "\""
                + ",\"event\":" + event
                + "}";
    }
}
