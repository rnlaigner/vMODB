package dk.ku.di.dms.vms.sdk.core.operational;

public record InboundEvent (
        long tid, long lastTid, long batch, String event, Class<?> clazz, Object input)
{
    @Override
    public String toString() {
        return "{"
                + "\"batch\":" + this.batch
                + ",\"tid\":" + this.tid
                + ",\"lastTid\":" + this.lastTid
                + ",\"event\":\"" + this.event + "\""
                + ",\"clazz\":" + this.clazz
                + ",\"input\":" + this.input
                + "}";
    }
}
