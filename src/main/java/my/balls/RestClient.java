package my.balls;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.Asserts;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class RestClient {
    private final Executor executor;
    private final URI baseUri;

    public RestClient(String baseUri, String userName, String password) {
        Asserts.notBlank(baseUri, "baseUri");
        Asserts.notBlank(userName, "userName");
        Asserts.notBlank(password, "password");

        if (!baseUri.endsWith("/")) {
            baseUri = baseUri + "/";
        }

        this.baseUri = URI.create(baseUri);

        executor = Executor.newInstance()
                .auth(this.baseUri.getHost(), new UsernamePasswordCredentials(userName, password));
    }

    public HttpResponse executePostRequest(String relativeUri, String requestBody, String accept, String contentType) throws IOException, URISyntaxException {
        return executePostRequest(relativeUri, requestBody, accept, contentType, null);
    }

    public HttpResponse executePostRequest(String relativeUri, String requestBody, String accept, String contentType, Map<String, String> parameters) throws IOException, URISyntaxException {
        Asserts.notBlank(requestBody, "requestBody");
        Asserts.notBlank(accept, "accept");
        Asserts.notBlank(contentType, "contentType");

        URI uri = prepareUri(relativeUri);

        Request request = Request.Post(addParameters(uri, parameters))
                .addHeader(HttpHeaders.ACCEPT, accept)
                .addHeader(HttpHeaders.CONTENT_TYPE, contentType)
                .addHeader("X-Remote-Domain", "1")
                .bodyByteArray(requestBody.getBytes());

        Response response = executor.execute(request);
        HttpResponse httpResponse = response.returnResponse();
        response.discardContent();

        return httpResponse;
    }

    public HttpResponse executeDeleteRequest(String relativeUri) throws IOException {
        URI uri = prepareUri(relativeUri);

        Request request = Request.Delete(uri)
                .addHeader("X-Remote-Domain", "1");

        Response response = executor.execute(request);
        HttpResponse httpResponse = response.returnResponse();
        response.discardContent();

        return httpResponse;
    }

    private URI prepareUri(String relativeUri) {
        URI uri = this.baseUri;
        if (relativeUri != null) {
            relativeUri = relativeUri.replaceAll("^/|/$", "");
            if (!relativeUri.isEmpty()) {
                uri = uri.resolve(relativeUri);
            }
        }
        return uri;
    }

    static URI addParameters(URI baseUri, Map<String, String> parameters) throws URISyntaxException {
        if (parameters != null && ! parameters.isEmpty()) {
            URIBuilder uriBuilder = new URIBuilder(baseUri);
            parameters.forEach(uriBuilder::addParameter);
            return uriBuilder.build();
        }
        return baseUri;
    }
}
