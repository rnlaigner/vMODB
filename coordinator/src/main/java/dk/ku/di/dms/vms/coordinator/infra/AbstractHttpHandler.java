package dk.ku.di.dms.vms.coordinator.infra;

import dk.ku.di.dms.vms.coordinator.Coordinator;

public abstract class AbstractHttpHandler {

    protected final Coordinator coordinator;

    public AbstractHttpHandler(Coordinator coordinator){
        this.coordinator = coordinator;
    }

    protected byte[] getLastTidCommittedBytes() {
        long lng = this.coordinator.getLastTidCommitted();
        System.out.println("Number of TIDs committed: "+lng);
        return new byte[] {
                (byte) lng,
                (byte) (lng >> 8),
                (byte) (lng >> 16),
                (byte) (lng >> 24),
                (byte) (lng >> 32),
                (byte) (lng >> 40),
                (byte) (lng >> 48),
                (byte) (lng >> 56)};
    }

    protected byte[] getLastTidSubmittedBytes() {
        long lng = this.coordinator.getLastTidSubmitted();
        System.out.println("Last TID submitted: "+lng);
        return new byte[] {
                (byte) lng,
                (byte) (lng >> 8),
                (byte) (lng >> 16),
                (byte) (lng >> 24),
                (byte) (lng >> 32),
                (byte) (lng >> 40),
                (byte) (lng >> 48),
                (byte) (lng >> 56)};
    }

    public byte[] getAsBytes(String uri) {
        String[] uriSplit = uri.split("/");
        if (uriSplit[1].equals("status")) {
            if(uriSplit[2].equals("committed")) {
                return this.getLastTidCommittedBytes();
            }
            return this.getLastTidSubmittedBytes();
        }
        return null;
    }

}
