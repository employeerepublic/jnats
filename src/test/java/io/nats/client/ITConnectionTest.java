/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Constants.ERR_CONNECTION_CLOSED;
import static io.nats.client.Constants.ERR_MAX_PAYLOAD;
import static io.nats.client.UnitTestUtilities.newDefaultConnection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.nats.client.Constants.ConnState;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Category(UnitTest.class)
public class ITConnectionTest {
    final Logger logger = LoggerFactory.getLogger(ITConnectionTest.class);

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    ExecutorService executor = Executors.newFixedThreadPool(5);
    // UnitTestUtilities utils = new UnitTestUtilities();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        UnitTestUtilities.startDefaultServer();
        Thread.sleep(500);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        UnitTestUtilities.stopDefaultServer();
        Thread.sleep(500);
    }

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    @Test
    public void testDefaultConnection() throws IOException, TimeoutException {
        try (Connection nc = newDefaultConnection()) {
            assertEquals("Should have status set to CONNECTED", nc.getState(), ConnState.CONNECTED);
            nc.close();
            assertEquals("Should have status set to CLOSED", nc.getState(), ConnState.CLOSED);
        }
    }

    @Test
    public void testConnectionStatus() throws IOException, TimeoutException {
        try (Connection c = new ConnectionFactory().createConnection()) {
            assertEquals(ConnState.CONNECTED, c.getState());
            c.close();
            assertEquals(ConnState.CLOSED, c.getState());
        }
    }

    @Test
    public void testConnClosedCB() {
        final AtomicBoolean closed = new AtomicBoolean(false);

        ConnectionFactory cf = new ConnectionFactory();
        cf.setClosedCallback(new ClosedCallback() {
            public void onClose(ConnectionEvent event) {
                closed.set(true);
            }
        });
        try (Connection c = cf.createConnection()) {
            c.close();
        } catch (IOException | TimeoutException e) {
            fail(e.getMessage());
        }
        assertTrue("Closed callback not triggered", closed.get());
    }

    @Test
    public void testCloseDisconnectedCB() throws IOException, TimeoutException {
        final AtomicBoolean disconnected = new AtomicBoolean(false);
        final Object disconnectedLock = new Object();

        ConnectionFactory cf = new ConnectionFactory();
        cf.setReconnectAllowed(false);
        cf.setDisconnectedCallback(new DisconnectedCallback() {
            @Override
            public void onDisconnect(ConnectionEvent event) {
                logger.trace("in disconnectedCB");
                synchronized (disconnectedLock) {
                    disconnected.set(true);
                    disconnectedLock.notify();
                }
            }
        });

        Connection c = cf.createConnection();
        assertFalse(c.isClosed());
        assertTrue(c.getState() == ConnState.CONNECTED);
        c.close();
        assertTrue(c.isClosed());
        synchronized (disconnectedLock) {
            try {
                disconnectedLock.wait(500);
                assertTrue("disconnectedCB not triggered.", disconnected.get());
            } catch (InterruptedException e) {
            }
        }

    }

    @Test
    public void testServerStopDisconnectedCB() throws IOException, TimeoutException {
        final Lock disconnectLock = new ReentrantLock();
        final Condition hasBeenDisconnected = disconnectLock.newCondition();

        ConnectionFactory cf = new ConnectionFactory();
        cf.setReconnectAllowed(false);
        cf.setDisconnectedCallback(new DisconnectedCallback() {
            @Override
            public void onDisconnect(ConnectionEvent event) {
                disconnectLock.lock();
                try {
                    hasBeenDisconnected.signal();
                } finally {
                    disconnectLock.unlock();
                }
            }
        });

        try (Connection c = cf.createConnection()) {
            assertFalse(c.isClosed());
            disconnectLock.lock();
            try {
                UnitTestUtilities.bounceDefaultServer(1000);
                assertTrue(hasBeenDisconnected.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
            } finally {
                disconnectLock.unlock();
            }
        }
    }

    // TODO TestServerSecureConnections
    // TODO TestClientCertificate
    // TODO TestServerTLSHintConnections

    @Test
    public void testClosedConnections() throws Exception {
        Connection c = new ConnectionFactory().createConnection();
        SyncSubscription s = c.subscribeSync("foo");

        c.close();
        assertTrue(c.isClosed());

        // While we can annotate all the exceptions in the test framework,
        // just do it manually.

        boolean exThrown = false;

        try {
            c.publish("foo", null);
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        assertTrue(c.isClosed());
        try {
            c.publish(new Message("foo", null, null));
        } catch (Exception e) {
            exThrown = true;
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            c.subscribeAsync("foo");
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            c.subscribeSync("foo");
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            c.subscribeAsync("foo", "bar");
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            c.subscribeSync("foo", "bar");
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            c.request("foo", null);
            assertTrue(c.isClosed());
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            s.nextMessage();
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            s.nextMessage(100);
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            s.unsubscribe();
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            s.autoUnsubscribe(1);
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }
    }

    // TODO TestErrOnConnectAndDeadlock MOCK
    // TODO TestMoreErrOnConnect

    @Test
    public void testErrOnMaxPayloadLimit() {
        long expectedMaxPayload = 10;
        String serverInfo =
                "INFO {\"server_id\":\"foobar\",\"version\":\"0.6.6\",\"go\":\"go1.5.1\",\"host\":\"%s\",\"port\":%d,\"auth_required\":false,\"ssl_required\":false,\"max_payload\":%d}\r\n";

        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            String infoString = (String.format(serverInfo, "mockserver", 2222, expectedMaxPayload));
            // System.err.println(infoString);
            mock.setServerInfoString(infoString);
            ConnectionFactory cf = new ConnectionFactory();
            try (Connection c = cf.createConnection(mock)) {
                // Make sure we parsed max payload correctly
                assertEquals(c.getMaxPayload(), expectedMaxPayload);

                // Check for correct exception
                boolean exThrown = false;
                try {
                    c.publish("hello", "hello world".getBytes());
                } catch (IllegalArgumentException e) {
                    assertEquals(ERR_MAX_PAYLOAD, e.getMessage());
                    exThrown = true;
                } finally {
                    assertTrue("Should have generated a IllegalArgumentException.", exThrown);
                }

                // Check for success on less than maxPayload

            } catch (IOException | TimeoutException e) {
                fail("Connection to mock server failed: " + e.getMessage());
            }
        } catch (Exception e1) {
            fail(e1.getMessage());
        }
    }

    // TODO TestConnectVerbose
    // TODO TestCallbacksOrder
    // TODO TestFlushReleaseOnClose - Mock
    // TODO TestMaxPendingOut - Mock
    // TODO TestErrInReadLoop - Mock
    // TODO TestErrStaleConnection - Mock
    // TODO TestServerErrorClosesConnection - Mock
}
