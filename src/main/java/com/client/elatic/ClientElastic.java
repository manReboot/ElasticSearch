package com.client.elatic;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.IndicesExists;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.codehaus.jackson.map.ObjectMapper;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;


import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.IntStream;

@Slf4j
public class ClientElastic {

    private static String URL = "http://localhost:9200";

    public JestClient jestClient() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(
                new HttpClientConfig.Builder(URL)
                        .multiThreaded(true)
                        .defaultMaxTotalConnectionPerRoute(2)
                        .maxTotalConnection(10)
                        .build());

        return factory.getObject();
    }

    public static RestHighLevelClient getNativeClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
        return client;
    }


    public static void main(String[] args) throws IOException {
        JestClient client = new ClientElastic().jestClient();
        try {
            JestResult result = client.execute(new IndicesExists.Builder("tesla_employees")
                    .build());


            /*Consumer<Integer> methSucces = (n) ->
            {
                ObjectMapper mapper = new ObjectMapper();
                Employee employee = EmployeeBuilder.buildEmp(23, "demo1", 23);
                try {
                    String request = mapper.writeValueAsString(employee);
                    RestHighLevelClient nativeClient = getNativeClient();
                    IndexRequest request1 = new IndexRequest("tesla_employees");
                    request1.source(request, XContentType.JSON);

                    IndexResponse response = nativeClient.index(request1,RequestOptions.DEFAULT);
                    /*ativeClient.indices().create(new CreateIndexRequest("tesla_employees")
                            .mapping( request, XContentType.JSON))
                            .actionGet();
                       */

            log.info("this test end");

            log.info(result.toString());

            /*if (result.getResponseCode() == 200 || result.getResponseCode() == 201) {
                new ClientElastic().createIndex();
            }*/
            new ClientElastic().createIndex();

        } catch (IOException e) {
            log.error("ERROR ", e);
        } catch (Exception e) {
            log.error("ERROR ", e);
        }
    }

    public void createIndex() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        RestHighLevelClient nativeClient = getNativeClient();
        RestClient lowclient = nativeClient.getLowLevelClient();

        IntStream.range(0, 4).forEach(num -> {

            Employee employee = EmployeeBuilder.buildEmp(23 + num + 1, String.format("demo_%d", num), 23 + num + 1);

            String request = null;
            try {
                request = mapper.writeValueAsString(employee);


                lowclient.getNodes().forEach(n -> log.info(n.getVersion()));
                IndexRequest request1 = new IndexRequest("tesla_employees");
                request1.source(request, XContentType.JSON);

                IndexResponse response = nativeClient.index(request1, RequestOptions.DEFAULT);
                log.info(response.status().name());
            } catch (IOException e) {
                log.error("ERROR", e);
            }
        });

        nativeClient.close();
    }

    final static class EmployeeBuilder {

        protected static Employee buildEmp(int age, String name, int workExp) {
            Employee tst = new Employee();
            tst.setAge(age);
            tst.setName(name);
            tst.setExperienceInYears(workExp);

            return tst;
        }
    }

}
