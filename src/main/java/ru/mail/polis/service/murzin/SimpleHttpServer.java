package ru.mail.polis.service.murzin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

public final class SimpleHttpServer extends HttpServer implements Service {
    private static final Logger logger = LoggerFactory.getLogger(SimpleHttpServer.class);
    private final DAO dao;

    /**
     * Http server for DAO implementation.
     * @param port number of port on which server work
     * @param dao DAO object
     * @throws IOException if can`t getConfig(port)
     */
    public SimpleHttpServer(final int port, @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
        logger.info("Server is running on port " + port);
    }

    @Path("/v0/status")
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    /**
     * API for requests on /entity path
     * @param id key
     * @param request Http request
     * @return Http response
     */
    @Path("/v0/entity")
    public Response entity(@Param("id") final String id, final Request request) {

        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, "Key not found".getBytes(Charsets.UTF_8));
        }

        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));

        logger.info("Request " + request.getMethod() + " with param: " + id);

        try {
            switch (request.getMethod()) {
            case Request.METHOD_GET:
                final ByteBuffer value = dao.get(key);
                final ByteBuffer duplicate = value.duplicate();
                final byte[] body = new byte[duplicate.remaining()];
                duplicate.get(body);
                return new Response(Response.OK, body);
            case Request.METHOD_PUT:
                dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                return new Response(Response.CREATED, Response.EMPTY);
            case Request.METHOD_DELETE:
                dao.remove(key);
                return new Response(Response.ACCEPTED, Response.EMPTY);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
        }
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 65536) {
            throw new IllegalArgumentException("Invalid port");
        }
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        config.selectors = 4;
        return config;
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }
}