package dk.ku.di.dms.vms.sdk.embed.http;

import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbeddedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.stream.Collectors;

public class HttpHandlerTest {

    @Test
    public void test(){

        VmsRuntimeMetadata vmsRuntimeMetadata = EmbedMetadataLoader.loadRuntimeMetadata("dk.ku.di.dms.vms.sdk.embed");

        VmsEmbeddedInternalChannels channel = new VmsEmbeddedInternalChannels();

        TransactionFacade transactionFacade = EmbedMetadataLoader.loadTransactionFacadeAndInjectIntoRepositories(vmsRuntimeMetadata);

        Map<String, Class<?>> tableNameToEntityClazzMap =
                vmsRuntimeMetadata.entityToTableNameMap().entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        // finish. receive a post and save in memory

        HttpClient client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8001/data"))
                .POST(HttpRequest.BodyPublishers.ofString("TESTE")).build();
        try {
            client.send( request, HttpResponse.BodyHandlers.discarding());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

}
