/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server;

import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.server.smile.JsonResponseWrapper;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.FutureCallback;

import java.net.SocketException;
import java.net.URI;

import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.presto.server.smile.JsonResponseWrapper.unwrapJsonResponse;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SimpleHttpFaultInjectionResponseHandler<T>
        implements FutureCallback<BaseResponse<T>>
{
    private final SimpleHttpResponseCallback<T> callback;

    private final URI uri;
    private final SimpleHttpResponseHandlerStats stats;
    private final ErrorCodeSupplier errorCode;
    private boolean isLeaf;

    public SimpleHttpFaultInjectionResponseHandler(SimpleHttpResponseCallback<T> callback, URI uri, SimpleHttpResponseHandlerStats stats, ErrorCodeSupplier errorCode, boolean isLeaf)
    {
        this.callback = callback;
        this.uri = uri;
        this.stats = requireNonNull(stats, "stats is null");
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.isLeaf = isLeaf;
    }

    @Override
    public void onSuccess(BaseResponse<T> response)
    {
        FaultInjectionHelper helper = FaultInjectionHelper.getInstance();
        stats.updateSuccess();
        stats.responseSize(response.getResponseSize());
        try {
            if (response.getStatusCode() == OK.code() && response.hasValue()) {
                if (isLeaf) {
                    if (helper.isRouteToSuccess(uri)) {
                        callback.success(response.getValue());
                    }
                    else {
                        callback.failed(new SocketException("simulate socket exception"));
                    }
                }
                else {
                    callback.success(response.getValue());
                }
            }
            else if (response.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE.code()) {
                callback.failed(new ServiceUnavailableException(uri));
            }
            else {
                // Something is broken in the server or the client, so fail immediately (includes 500 errors)
                Exception cause = response.getException();
                if (cause == null) {
                    if (response.getStatusCode() == OK.code()) {
                        cause = new PrestoException(errorCode, format("Expected response from %s is empty", uri));
                    }
                    else {
                        cause = new PrestoException(errorCode, createErrorMessage(response));
                    }
                }
                else {
                    cause = new PrestoException(errorCode, format("Unexpected response from %s", uri), cause);
                }
                callback.fatal(cause);
            }
        }
        catch (Throwable t) {
            // this should never happen
            callback.fatal(t);
        }
    }

    private String createErrorMessage(BaseResponse<T> response)
    {
        if (response instanceof JsonResponseWrapper) {
            return format("Expected response code from %s to be %s, but was %s: %s%n%s",
                    uri,
                    OK.code(),
                    response.getStatusCode(),
                    response.getStatusMessage(),
                    unwrapJsonResponse(response).getResponseBody());
        }
        return format("Expected response code from %s to be %s, but was %s: %s",
                uri,
                OK.code(),
                response.getStatusCode(),
                response.getStatusMessage(),
                new String(response.getResponseBytes()));
    }

    @Override
    public void onFailure(Throwable t)
    {
        stats.updateFailure();
        callback.failed(t);
    }
}
