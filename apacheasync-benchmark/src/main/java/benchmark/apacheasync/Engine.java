package benchmark.apacheasync;

import com.ss.benchmark.httpclient.common.HttpClientEngine;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import javax.net.ssl.SSLContext;

public class Engine implements HttpClientEngine {
    private CloseableHttpAsyncClient client;

    private RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(CONNECT_TIMEOUT)
            .setSocketTimeout(READ_TIMEOUT)
            .build();

    String baseUrl = null;

    @Override
    public void createClient(String host, int port)  {
        this.baseUrl = url(host, port);
        ConnectingIOReactor ioReactor;
        try {
            ioReactor = new DefaultConnectingIOReactor();
        } catch (IOReactorException e) {
            throw new RuntimeException(e);
        }
        // Create connection manager with SSLContext
        PoolingNHttpClientConnectionManager connectionManager = new PoolingNHttpClientConnectionManager(ioReactor);

        // Set maximum connections per route
        connectionManager.setDefaultMaxPerRoute(MAX_CONNECTION_POOL_SIZE_PER_HOST);
        connectionManager.setMaxTotal(MAX_CONNECTION_POOL_SIZE);

         client = HttpAsyncClientBuilder.create().setConnectionManager(connectionManager)
                                        .setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(-1)
                                                                                                          .setSocketTimeout(READ_TIMEOUT)
                                                                                                          .setConnectionRequestTimeout(CONNECT_TIMEOUT)
                                                                                                          .build())
                                                                    .build();

        // Start the client
        client.start();
    }

    @Override
    public String blockingGET(String path) {
        final HttpGet request = new HttpGet(baseUrl + path);
        request.setConfig(requestConfig);
        Future<HttpResponse> responseFuture = client.execute(request, null);
        try {
            HttpResponse httpResponse = responseFuture.get();
            return EntityUtils.toString(httpResponse.getEntity());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public String blockingPOST(String path, String body) {
        final StringEntity stringEntity = new StringEntity(body, "UTF-8");
        final HttpPost request = new HttpPost(baseUrl + path);

        request.addHeader("content-type", "application/json");
        stringEntity.setContentType("application/json");
        request.setEntity(stringEntity);
        request.setConfig(requestConfig);
        Future<HttpResponse> responseFuture = client.execute(request, null);
        try {
            HttpResponse httpResponse = responseFuture.get();
            return EntityUtils.toString(httpResponse.getEntity());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public CompletableFuture<String> nonblockingGET(String path) {
        final CompletableFuture<String> cfResponse = new CompletableFuture<>();

        HttpGet request = new HttpGet(baseUrl + path);
        request.setConfig(requestConfig);

        client.execute(request, new FutureCallback<>() {
            @Override
            public void completed(HttpResponse httpResponse) {
                try {
                    cfResponse.complete(EntityUtils.toString(httpResponse.getEntity()));
                } catch (Exception e) {
                    cfResponse.completeExceptionally(e);
                }
            }

            @Override
            public void failed(Exception e) {
                cfResponse.completeExceptionally(e);
            }

            @Override
            public void cancelled() {
                cfResponse.cancel(true);
            }
        });
        return cfResponse;
    }

    @Override
    public CompletableFuture<String> nonblockingPOST(String path, String body) {
        final CompletableFuture<String> cfResponse = new CompletableFuture<>();

        HttpPost request = new HttpPost(baseUrl + path);
        request.addHeader("content-type", "application/json");
        request.addHeader("Host", "localhost");
        StringEntity stringEntity = new StringEntity(body, "UTF-8");
        stringEntity.setContentType("application/json");
        request.setEntity(stringEntity);

        client.execute(request, new FutureCallback<>() {
            @Override
            public void completed(HttpResponse httpResponse) {
                try {
                    cfResponse.complete(EntityUtils.toString(httpResponse.getEntity()));
                } catch (Exception e) {
                    cfResponse.completeExceptionally(e);
                }
            }

            @Override
            public void failed(Exception e) {
                cfResponse.completeExceptionally(e);
            }

            @Override
            public void cancelled() {
                cfResponse.cancel(true);
            }
        });
        return cfResponse;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
