package com.sulei.test.udf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * @author sulei
 * @date 2020/7/22
 * @e-mail 776531804@qq.com
 * <p>
 * <p>
 * 特殊需求：流进来的数据中的某些字段，用来当做调用某个http接口时传入的参数。
 * 这里，rest接口url固定，调rest接口时的方式固定（POST），调用接口时传的参格式固定（json的格式），而json中某个字段的值需要从流进来的数据中某个字段取。
 * 而调用接口的返回值（json），将返回值中某个字段取出来，当做一个新字段拼接到流进来的数据后。这个字段可能是一个数组中多次出现的值，需要用分隔符分开
 */

public class HttpUdf extends ScalarFunction {
    private String url;
    private ObjectMapper mapper;
    private HttpClient httpClient = null;
    private HttpPost httpPostRequest = null;
    private JsonNode requestObject = null;

    public HttpUdf() {
        initJsonHandler();
    }

    public HttpUdf(String url) {
        this();
        this.url = url;
    }

    private void initJsonHandler() {
        this.mapper = new ObjectMapper();
    }

    /**
     * 每条数据应用的udf
     *
     * @param originColumn    原始数据中作为调用http接口所需要的内容的字段名
     * @param requestBody     http接口带的body，json格式
     * @param jsonColumnName  http接口返回值（json）中所需要的字段名
     * @param columnSeparator http接口返回值中所需要的字段可能包含在数组中，所以可能会出现多个，当是多个时候的分隔符
     */
    public String eval(String originColumn, String requestBody, String jsonColumnName, String columnSeparator) throws Exception {
        JsonNode requestNode = getRequestObject(requestBody);
//        ((ObjectNode) requestNode.get("params")).put("userMobile", originColumn);

        String result = httpPostRequest(requestNode.toString());
        JsonNode resultNode = this.mapper.readTree(result);
        List<JsonNode> nodes = resultNode.findValues(jsonColumnName);

        StringBuilder sb = new StringBuilder();
        if (nodes != null && nodes.size() > 0) {
            if (nodes.size() > 1) {
                for (JsonNode node : nodes) {
                    sb.append(node.asText()).append(columnSeparator);
                }
            } else {
                sb.append(nodes.get(0).asText());
            }
        }

        return sb.toString();
    }

    public String eval(String originColumn, String requestBody, String jsonColumnName) throws Exception {
        return eval(originColumn, requestBody, jsonColumnName, "@@");
    }

    private JsonNode getRequestObject(String requestBody) throws IOException {
        if (this.requestObject == null) {
            synchronized (this) {
                if (this.requestObject == null) {
                    this.requestObject = this.mapper.readTree(requestBody);
                }
            }
        }
        return this.requestObject;
    }

    private HttpClient getHttpClient() {
        if (this.httpClient == null) {
            synchronized (this) {
                if (this.httpClient == null) {
                    this.httpClient = HttpClientBuilder.create().build();
                }
            }
        }
        return this.httpClient;
    }

    private HttpPost getHttpPostRequest() {
        if (this.httpPostRequest == null) {
            synchronized (this) {
                if (this.httpPostRequest == null) {
                    this.httpPostRequest = new HttpPost(this.url);
                    this.httpPostRequest.addHeader("content-type", "application/json");
                    this.httpPostRequest.setHeader("Content-Language", Locale.US.toLanguageTag());
                }
            }
        }
        return this.httpPostRequest;
    }

    private String httpPostRequest(String body) throws IOException {
        HttpPost request = getHttpPostRequest();
        HttpEntity param = new StringEntity(body, "UTF-8");
        request.setEntity(param);

        HttpResponse response = getHttpClient().execute(request);
        return EntityUtils.toString(response.getEntity(), "UTF-8");
    }
}
