package io.nats.client;

import static io.nats.client.Constants.ERR_CONNECTION_CLOSED;
import static io.nats.client.Constants.ERR_CONNECTION_READ;
import static io.nats.client.Constants.ERR_NO_SERVERS;
import static io.nats.client.Constants.ERR_STALE_CONNECTION;
import static io.nats.client.Constants.ERR_TIMEOUT;
import static io.nats.client.TestConstants.TEST_INFO_STRING;
import static io.nats.client.UnitTestUtilities.newMockedConnection;
import static io.nats.client.UnitTestUtilities.newMockedConnectionImpl;
import static io.nats.client.UnitTestUtilities.newMockedTcpConnection;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.client.ConnectionImpl.Srv;
import io.nats.client.Constants.ConnState;

import ch.qos.logback.classic.Level;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Category(UnitTest.class)
public class ConnectionImplTest {

    static final Logger logger = LoggerFactory.getLogger(ConnectionImplTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Mock
    private Channel<Boolean> mockChannelBoolean;

    @Mock
    private Channel<Message> mockChannelMessage;

    @Mock
    private ArrayList<Channel<Boolean>> mockPongs;

    @Mock
    private OutputStream mockOutputStream;

    @Mock
    Map<Long, SubscriptionImpl> mockSubs;

    @Mock
    SubscriptionImpl mockSub;

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        verifier.setup();
    }

    @After
    public void tearDown() throws Exception {
        verifier.teardown();
    }

    @Test
    public void testConnectionImpl() {
        new ConnectionImpl();
    }

    @Test
    public void testConnectionImplOptions() {
        Options opts = mock(Options.class);
        ConnectionImpl conn = new ConnectionImpl(opts);
        assertEquals(opts, conn.getOptions());
    }

    @Test
    public void testConnectionImplOptionsTCPConnection() throws IOException {
        new ConnectionImpl(new Options(), newMockedTcpConnection());
    }

    @Test
    public void testGetPropertiesInputStream() throws IOException {
        TCPConnection tcpConn = newMockedTcpConnection();
        ConnectionImpl conn = new ConnectionImpl(new Options(), tcpConn);
        InputStream is = getClass().getClassLoader().getResourceAsStream("jnats.properties");
        Properties props = conn.getProperties(is);
        assertNotNull(props);
    }

    @Test
    public void testGetPropertiesInputStreamFailure() throws TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            Properties props = c.getProperties("foobar.properties");
            assertNull(props);

            InputStream is = mock(InputStream.class);
            doThrow(new IOException("Foo")).when(is).read(any(byte[].class));
            doThrow(new IOException("Foo")).when(is).read(any(byte[].class), any(Integer.class),
                    any(Integer.class));
            doThrow(new IOException("Foo")).when(is).read();

            props = c.getProperties(is);
            assertNull("getProperties() should have returned null", props);
        } catch (IOException e) {
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void testGetPropertiesString() {
        try (ConnectionImpl c = newMockedConnection()) {
            Properties props = c.getProperties("jnats.properties");
            assertNotNull(props);
            String version = props.getProperty("client.version");
            assertNotNull(version);
            // System.out.println("version: " + version);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSetupServerPool() throws IOException {
        try (TCPConnection tcpConn = newMockedTcpConnection()) {
            Options opts = new Options();
            String url = "nats://localhost:4242";
            String[] servers = new String[] { "nats://host-a:4242", "nats://host-b:4242" };

            // Test with empty strings
            opts.setUrl((String) null);
            opts.setServers((String[]) null);
            ConnectionImpl conn = spy(new ConnectionImpl(opts, tcpConn));
            conn.setupServerPool();
            List<Srv> pool = conn.getServerPool();
            assertEquals(1, pool.size());
            URI testUri = URI.create(ConnectionFactory.DEFAULT_URL);
            assertEquals(pool.get(0).toString(), conn.new Srv(testUri).toString());
            assertEquals(testUri, conn.url);

            // Test with no randomization
            opts.setUrl("nats://localhost:4242");
            opts.setServers(servers);
            opts.setNoRandomize(true);
            conn.setupServerPool();
            testUri = URI.create(url);
            assertEquals(testUri, conn.url);

        }
    }

    @Test
    public void testCurrentServer() {
        fail("Not yet implemented"); // TODO
    }

    @Test // TODO recheck criteria
    public void testSelectNextServer() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_NO_SERVERS);

        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl conn = (ConnectionImpl) cf.createConnection()) {
            List<ConnectionImpl.Srv> pool = conn.getServerPool();
            pool.clear();
            conn.selectNextServer();
        }
    }

    @Test
    public void testConnect() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testCreateConn() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testCreateConnExhaustedSrvPool() throws IOException, TimeoutException {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_NO_SERVERS);

        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            List<ConnectionImpl.Srv> pool = new ArrayList<ConnectionImpl.Srv>();
            c.setServerPool(pool);
            assertNull(c.currentServer());
            c.createConn();
        }
    }

    @Test
    public void testCreateConnFailure() throws IOException, TimeoutException {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_NO_SERVERS);
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            mock.setOpenFailure(true);
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                fail("Should not have connected");
            }
        }
    }

    @Test
    public void testClose() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testProcessConnectInit() throws IOException, TimeoutException {
        try (TCPConnection tcpConn = newMockedTcpConnection()) {
            Options opts = new Options();
            opts.setUrl("nats://localhost:4222");
            try (ConnectionImpl conn = spy(new ConnectionImpl(opts, tcpConn))) {
                conn.createConn();
                conn.setup();
                conn.processConnectInit();
                // Check correct status
                verify(conn).processExpectedInfo();
                verify(conn).sendConnect();
                verify(conn).spinUpSocketWatchers();
                // Check that server INFO was processed correctly
                assertEquals(TEST_INFO_STRING, conn.getConnectedServerInfo().toString());
            }
        }
    }

    @Test
    public void testProcessExpectedInfo() throws IOException, TimeoutException {
        BufferedReader mockReader = mock(BufferedReader.class);
        TCPConnection mockTcpConn = mock(TCPConnection.class);
        when(mockTcpConn.getBufferedReader()).thenReturn(mockReader);

        try (TCPConnection tcpConn = newMockedTcpConnection()) {
            Options opts = new Options();
            opts.setUrl("nats://localhost:4222");
            try (ConnectionImpl conn = spy(new ConnectionImpl(opts, tcpConn))) {
                conn.createConn();
                conn.setup();
                conn.status = ConnState.CONNECTING;
                conn.processExpectedInfo();
                // Check correct status
                verify(conn).readOp();
                String infoArg = TEST_INFO_STRING.substring("INFO".length() + 1).trim();
                verify(conn).processInfo(eq(infoArg));
                verify(conn).checkForSecure();
                // Check that server INFO was processed correctly
                assertEquals(TEST_INFO_STRING, conn.getConnectedServerInfo().toString());
            }
        }
    }

    @Test
    public void testProcessExpectedInfoReadOpFailure() throws IOException, TimeoutException {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_CONNECTION_READ);

        BufferedReader mockReader = mock(BufferedReader.class);
        TCPConnection mockTcpConn = mock(TCPConnection.class);
        when(mockTcpConn.getBufferedReader()).thenReturn(mockReader);

        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl conn = (ConnectionImpl) cf.createConnection()) {
            conn.setTcpConnection(mockTcpConn);
            doThrow(new IOException(ERR_CONNECTION_READ)).when(mockReader).readLine();
            conn.processExpectedInfo();
        }

        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            mock.setBadReader(true);
            boolean exThrown = true;
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {

            } catch (IOException | TimeoutException e) {
                assertTrue(e instanceof IOException);
                assertEquals(ERR_CONNECTION_READ, e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown IOException", exThrown);
        }
    }

    @Test
    public void testProcessPing() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                BufferedOutputStream bw = mock(BufferedOutputStream.class);
                doThrow(new IOException("Mock OutputStream write exception")).when(bw)
                        .write(any(byte[].class), any(int.class), any(int.class));
                doThrow(new IOException("Mock OutputStream write exception")).when(bw)
                        .write(any(byte[].class));
                doThrow(new IOException("Mock OutputStream write exception")).when(bw)
                        .write(any(int.class));
                c.setOutputStream(bw);
                c.processPing();
                assertTrue(c.getLastException() instanceof IOException);
                assertEquals("Mock OutputStream write exception",
                        c.getLastException().getMessage());
            } catch (IOException | TimeoutException e) {
                fail("Connection attempt failed.");
            } catch (NullPointerException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testProcessPong() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testProcessOK() throws IOException {
        ConnectionImpl conn = newMockedConnectionImpl();
        conn.processOK();
    }

    @Test
    public void testProcessInfo() throws IOException {
        // Test valid
        ConnectionImpl conn = newMockedConnectionImpl();
        ServerInfo expected = new ServerInfo(TestConstants.TEST_INFO_STRING);
        conn.processInfo(TestConstants.TEST_INFO_STRING);
        assertEquals(expected.toString(), conn.getConnectedServerInfo().toString());

        // Test null
        conn = newMockedConnectionImpl();
        conn.processInfo(null);
        assertEquals(null, conn.getConnectedServerInfo());

        // Test empty
        conn = newMockedConnectionImpl();
        conn.processInfo("");
        assertEquals(null, conn.getConnectedServerInfo());


    }

    @Test
    public void testProcessDisconnect() throws IOException {
        ConnectionImpl conn = newMockedConnectionImpl();
        conn.status = ConnState.CONNECTED;
        conn.processDisconnect();
        assertEquals(ConnState.DISCONNECTED, conn.getState());

    }

    @Test
    public void testIsReconnecting() throws IOException, TimeoutException {
        ConnectionImpl conn = newMockedConnectionImpl();
        assertFalse(conn.isReconnecting());
    }

    @Test
    public void testIsClosed() throws IOException, TimeoutException {
        ConnectionImpl conn = newMockedConnectionImpl();
        assertFalse(conn.isClosed());
    }

    @Test
    public void testFlushReconnectPendingItems() {
        final AtomicBoolean exThrown = new AtomicBoolean(false);
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                BufferedOutputStream bw = mock(BufferedOutputStream.class);
                doThrow(new IOException("IOException from testFlushReconnectPendingItems")).when(bw)
                        .flush();

                doAnswer(new Answer<Void>() {
                    @Override
                    public Void answer(InvocationOnMock invocation) throws Throwable {
                        exThrown.set(true);
                        throw new IOException("Shouldn't have written empty pending");
                    }
                }).when(bw).write(any(byte[].class), any(int.class), any(int.class));

                assertNull(c.getPending());

                // Test path when pending is empty
                c.flushReconnectPendingItems();
                assertFalse("Should not have thrown exception", exThrown.get());

                exThrown.set(false);
                doAnswer(new Answer<Void>() {
                    @Override
                    public Void answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        byte[] buf = (byte[]) args[0];
                        assertArrayEquals(c.pingProtoBytes, buf);
                        String s = new String(buf);
                        exThrown.set(true);
                        throw new IOException("testFlushReconnectPendingItems IOException");
                    }
                }).when(bw).write(any(byte[].class), any(int.class), any(int.class));

                // Test with PING pending
                ByteArrayOutputStream baos =
                        new ByteArrayOutputStream(ConnectionImpl.DEFAULT_PENDING_SIZE);
                baos.write(c.pingProtoBytes, 0, c.pingProtoBytesLen);
                c.setPending(baos);
                c.setOutputStream(bw);
                c.flushReconnectPendingItems();
                assertTrue("Should have thrown exception", exThrown.get());
            } catch (IOException | TimeoutException e) {
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testNormalizeErrString() {
        final String errString = "-ERR 'Authorization Violation'";
        String str = ConnectionImpl.normalizeErr(errString);
        assertEquals("authorization violation", str);
    }

    @Test
    public void testNormalizeErrByteBuffer() {
        final String errString = "-ERR 'Authorization Violation'";
        ByteBuffer error = ByteBuffer.allocate(1024);
        error.put(errString.getBytes());
        error.flip();

        String str = ConnectionImpl.normalizeErr(error);
        assertEquals("authorization violation", str);
    }

    @Test
    public void testProcessErr() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testSendConnect() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testReadLine() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testSendProto() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testReadOp() throws IOException {
        ConnectionImpl conn = spy(newMockedConnectionImpl());
        doReturn(TestConstants.TEST_INFO_STRING).when(conn).readLine();
        ConnectionImpl.Control expected = conn.new Control(TestConstants.TEST_INFO_STRING);
        ConnectionImpl.Control result = conn.readOp();
        assertEquals(expected.op, result.op);
        assertEquals(expected.args, result.args);
    }

    @Test
    public void testRunTasks() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testSpinUpSocketWatchers() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testGo() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testReadLoop() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testReadLoopTerminatesWhenConnClosed() {
        // Tests to ensure that readLoop() breaks out if the connection is closed
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                c.close();
                BufferedInputStream br = mock(BufferedInputStream.class);
                c.setInputStream(br);
                assertEquals(br, c.getInputStream());
                doThrow(new IOException("readLoop() should already have terminated")).when(br)
                        .read(any(byte[].class), any(int.class), any(int.class));
                assertTrue(c.isClosed());
                c.readLoop();
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testDeliverMsgs() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testDeliverMsgsConnClosed() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                Channel<Message> ch = new Channel<Message>();
                Message m = new Message();
                ch.add(m);
                assertEquals(1, ch.getCount());
                c.close();
                c.deliverMsgs(ch);
                assertEquals(1, ch.getCount());

            } catch (IOException | TimeoutException e1) {
                e1.printStackTrace();
                fail(e1.getMessage());
            }
        }
    }

    // @Test
    // public void testDeliverMsgsChannelTimeout() {
    // try (TCPConnectionMock mock = new TCPConnectionMock()) {
    // try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
    // @SuppressWarnings("unchecked")
    // Channel<Message> ch = (Channel<Message>)mock(Channel.class);
    // when(ch.get()).
    // thenThrow(new TimeoutException("Timed out getting message from channel"));
    //
    // boolean timedOut=false;
    // try {
    // c.deliverMsgs(ch);
    // } catch (Error e) {
    // Throwable cause = e.getCause();
    // assertTrue(cause instanceof TimeoutException);
    // timedOut=true;
    // }
    // assertTrue("Should have thrown Error (TimeoutException)", timedOut);
    //
    // } catch (Exception e) {
    // e.printStackTrace();
    // fail(e.getMessage());
    // }
    // } catch (Exception e) {
    // fail(e.getMessage());
    // }
    // }

    @Test
    public void testDeliverMsgsSubProcessFail() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            final String subj = "foo";
            final String plString = "Hello there!";
            final byte[] payload = plString.getBytes();
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {

                final SyncSubscriptionImpl sub = mock(SyncSubscriptionImpl.class);
                when(sub.getSid()).thenReturn(14L);
                when(sub.processMsg(any(Message.class))).thenReturn(false);
                when(sub.getLock()).thenReturn(mock(ReentrantLock.class));

                @SuppressWarnings("unchecked")
                Channel<Message> ch = (Channel<Message>) mock(Channel.class);
                when(ch.get()).thenReturn(new Message(payload, payload.length, subj, null, sub))
                        .thenReturn(null);

                try {
                    c.deliverMsgs(ch);
                } catch (Error e) {
                    fail(e.getMessage());
                }

            } catch (IOException | TimeoutException e1) {
                e1.printStackTrace();
                fail(e1.getMessage());
            }
        }
    }

    @Test
    public void testProcessMsg() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testProcessMsgReturnsEarlyOnBadSid() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                assertFalse(c.isClosed());
                try {
                    mock.deliverMessage("foo", 27, null, "Hello".getBytes());
                } catch (Exception e) {
                    fail("Mock server shouldn't have thrown an exception: " + e.getMessage());
                }

            } catch (IOException | TimeoutException e) {
                fail(e.getMessage());
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testProcessMsgUnsubscribesWhenMaxReached() {
        final String subject = "foo";
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                Map<Long, SubscriptionImpl> subs = c.getSubs();
                assertNotNull(subs);
                c.setSubs(subs);
                SyncSubscriptionImpl s = (SyncSubscriptionImpl) c.subscribeSync(subject);
                Parser.MsgArg args = c.parser.new MsgArg();
                args.sid = s.getSid();
                args.subject.clear();
                args.subject.put(subject.getBytes());
                s.setMax(1);
                c.ps.ma = args;
                assertNotNull("Sub should have been present", c.getSubs().get(args.sid));
                c.processMsg(null, 0, 0);
                c.processMsg(null, 0, 0);
                c.processMsg(null, 0, 0);
                assertNull("Sub should have been removed", c.getSubs().get(args.sid));
            } catch (IOException | TimeoutException e) {
                fail("Connection failed");
            }
        }
    }

    @Test
    public void testRemoveSub() throws IOException, TimeoutException {
        final Map<Long, SubscriptionImpl> subs = new HashMap<Long, SubscriptionImpl>();
        final ConnectionImpl conn = spy(newMockedConnectionImpl());

        SubscriptionImpl mockSubscription = mock(SubscriptionImpl.class);
        Lock mockLock = mock(ReentrantLock.class);
        when(mockSubscription.getSid()).thenReturn(22L);
        when(mockSubscription.getLock()).thenReturn(mockLock);
        when(mockSubscription.getChannel()).thenReturn(mockChannelMessage);

        subs.put(mockSubscription.getSid(), mockSubscription);
        conn.setSubs(subs);

        // Now test
        conn.removeSub(mockSubscription);
        assertNull(subs.get(mockSubscription.getSid()));
        verify(mockSubscription).setChannel(eq(null));
        verify(mockLock).lock();
        verify(mockLock).unlock();

        // Test with channel null
        when(mockSubscription.getChannel()).thenReturn(null);
        subs.put(mockSubscription.getSid(), mockSubscription);
        conn.removeSub(mockSubscription);
        assertNull(subs.get(mockSubscription.getSid()));
        verify(mockSubscription).setChannel(eq(null));
        verify(mockLock, times(2)).lock();
        verify(mockLock, times(2)).unlock();

    }

    @Test
    public void testProcessSlowConsumer() throws IOException, TimeoutException {
        Options opts = new Options();
        final ConnectionImpl conn = spy(newMockedConnectionImpl());
        ExceptionHandler handler = spy(new ExceptionHandler() {
            public void onException(NATSException nex) {
                assertEquals(mockSub, nex.getSubscription());
                assertEquals(conn, nex.getConnection());
                Throwable cause = nex.getCause();
                assertTrue(cause instanceof IOException);
                assertEquals(Constants.ERR_SLOW_CONSUMER, cause.getMessage());
            }
        });
        conn.setExceptionHandler(handler);
        conn.processSlowConsumer(mockSub);
        // Verify correct exception set
        assertTrue(conn.getLastException() instanceof IOException);
        assertEquals(Constants.ERR_SLOW_CONSUMER, conn.getLastException().getMessage());
        // Verify async handler called
        verify(handler).onException(any(NATSException.class));
        // Verify slow consumer was set
        verify(mockSub).setSlowConsumer(eq(true));
    }

    @Test
    public void testRemoveFlushEntry() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                ArrayList<Channel<Boolean>> pongs = new ArrayList<Channel<Boolean>>();
                c.setPongs(pongs);
                assertEquals(pongs, c.getPongs());
                // Basic case
                Channel<Boolean> testChan = new Channel<Boolean>();
                testChan.add(true);
                pongs.add(testChan);
                assertTrue("Failed to find chan", c.removeFlushEntry(testChan));

                Channel<Boolean> testChan2 = new Channel<Boolean>();
                testChan2.add(false);
                pongs.add(testChan);
                assertFalse("Should not have found chan", c.removeFlushEntry(testChan2));

                pongs.clear();
                assertFalse("Should have returned false", c.removeFlushEntry(testChan));

            } catch (IOException | TimeoutException e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }
    }


    @Test
    public void testSendPing() throws IOException, TimeoutException {
        ConnectionImpl conn = spy(newMockedConnectionImpl());
        conn.setPongs(mockPongs);
        conn.setOutputStream(mockOutputStream);
        conn.sendPing(mockChannelBoolean);
        verify(mockPongs).add(mockChannelBoolean);
        verify(mockOutputStream).write(eq(conn.pingProtoBytes), eq(0), eq(conn.pingProtoBytesLen));
        verify(mockOutputStream).flush();
    }

    @Test
    public void testSendPingFailure() throws IOException, TimeoutException {
        ConnectionImpl conn = spy(newMockedConnectionImpl());
        String errMsg = "Mock OutputStream write exception";
        doThrow(new IOException(errMsg)).when(mockOutputStream).write(eq(conn.pingProtoBytes),
                eq(0), eq(conn.pingProtoBytesLen));
        conn.setOutputStream(mockOutputStream);
        conn.setPongs(mockPongs);
        conn.sendPing(new Channel<Boolean>());
        verify(conn).setLastError(any(IOException.class));
        verifier.verifyLogMsgEquals(Level.ERROR, "Could not send PING");
        assertEquals("Mock OutputStream write exception", conn.getLastException().getMessage());
    }

    @Test
    public void testResetPingTimer() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testProcessPingTimer() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testPingTimer() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                c.close();
                assertTrue(c.isClosed());
                c.processPingTimer();
            } catch (IOException | TimeoutException e) {
                fail("Connection failed");
            }

        }
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setMaxPingsOut(0);
            cf.setReconnectAllowed(false);
            try (ConnectionImpl c = cf.createConnection(mock)) {
                mock.setNoPongs(true);
                BufferedOutputStream bw = mock(BufferedOutputStream.class);

                c.setOutputStream(bw);
                c.processPingTimer();
                assertTrue(c.isClosed());
                assertTrue(c.getLastException() instanceof IOException);
                assertEquals(ERR_STALE_CONNECTION, c.getLastException().getMessage());
            } catch (IOException | TimeoutException e) {
                fail("Connection failed");
            }
        }
    }

    @Test
    public void testUnsubscribe() {
        boolean exThrown = false;
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                SyncSubscriptionImpl s = (SyncSubscriptionImpl) c.subscribeSync("foo");
                long sid = s.getSid();
                assertNotNull("Sub should have been present", c.getSubs().get(sid));
                s.unsubscribe();
                assertNull("Sub should have been removed", c.getSubs().get(sid));
                c.close();
                assertTrue(c.isClosed());
                c.unsubscribe(s, 0);
            } catch (IllegalStateException e) {
                assertEquals("Unexpected exception: " + e.getMessage(), ERR_CONNECTION_CLOSED,
                        e.getMessage());
                exThrown = true;
            } catch (IOException | TimeoutException e) {
                fail("Unexpected exception");
            }
            assertTrue("Should have thrown IllegalStateException.", exThrown);
        }
    }

    @Test
    public void testUnsubscribeAlreadyRemoved() {
        // FIXME success criteria?
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                SyncSubscriptionImpl s = (SyncSubscriptionImpl) c.subscribeSync("foo");
                c.subs.remove(s.getSid());
                c.unsubscribe(s, 415);
                assertNotEquals(415, s.getMax());
            } catch (IOException | TimeoutException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testUnsubscribeSubscriptionImplInt() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testUnsubscribeSubscriptionImplLong() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testKickFlusher() throws IOException {
        Channel<Boolean> fch = new Channel<Boolean>();
        ConnectionImpl conn = newMockedConnectionImpl();
        conn.setFlushChannel(fch);

        // Test with non-null bw
        conn.setOutputStream(mockOutputStream);
        conn.kickFlusher();
        assertEquals(1, fch.getCount());
        assertTrue(fch.get());

        // Test with null bw
        conn.setOutputStream(null);
        conn.kickFlusher();
        assertEquals(0, fch.getCount());
    }

    @Test
    public void testFlusher() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testFlusherFlushError() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                c.close();

            } catch (IOException | TimeoutException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testFlusherFalse() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                BufferedOutputStream bw = mock(BufferedOutputStream.class);
                doThrow(new IOException("Should not have flushed")).when(bw).flush();
                c.close();
                c.setOutputStream(bw);
                Channel<Boolean> fch = c.getFlushChannel();
                fch.add(false);
                c.setFlushChannel(fch);
                c.flusher();
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testFlushTimeout() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testFlushTimeoutFailure() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                mock.setNoPongs(true);
                boolean exThrown = false;
                try {
                    c.flush(500);
                } catch (Exception e) {
                    assertTrue(e instanceof TimeoutException);
                    assertEquals(ERR_TIMEOUT, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Should have thrown a timeout exception", exThrown);

            } catch (IOException | TimeoutException e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }
    }

    @Test
    public void testFlushFailureNoPong() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            boolean exThrown = false;
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                assertFalse(c.isClosed());
                exThrown = false;
                try {
                    mock.setNoPongs(true);
                    c.flush(1000);
                } catch (TimeoutException e) {
                    // System.err.println("timeout connection closed");
                    exThrown = true;
                } catch (Exception e) {
                    fail("Wrong exception: " + e.getClass().getName());
                }
                assertTrue(exThrown);

            } catch (IOException | TimeoutException e) {
                fail("Exception thrown");
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testFlush() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testFlushFailure() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            // mock.setBadWriter(true);
            boolean exThrown = false;
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                assertFalse(c.isClosed());
                c.close();
                try {
                    c.flush(-1);
                } catch (IllegalArgumentException e) {
                    exThrown = true;
                }
                assertTrue(exThrown);

                exThrown = false;
                try {
                    c.flush();
                } catch (Exception e) {
                    assertTrue(
                            "Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                            e instanceof IllegalStateException);
                    assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
                    exThrown = true;
                }
                assertTrue(exThrown);

                exThrown = false;
                try {
                    mock.setNoPongs(true);
                    c.flush(5000);
                } catch (Exception e) {
                    assertTrue(
                            "Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                            e instanceof IllegalStateException);
                    assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
                    exThrown = true;
                }
                assertTrue(exThrown);

            } catch (IOException | TimeoutException e) {
                fail("Exception thrown");
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testResendSubscriptions() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                AsyncSubscriptionImpl sub =
                        (AsyncSubscriptionImpl) c.subscribe("foo", new MessageHandler() {
                            public void onMessage(Message msg) {
                                System.err.println("got msg: " + msg);
                            }
                        });
                sub.setMax(122);
                assertEquals(122, sub.max);
                sub.delivered.set(122);
                assertEquals(122, sub.delivered.get());
                logger.trace("TEST Sub = {}", sub);
                c.resendSubscriptions();
                c.getOutputStream().flush();
                sleep(100);
                String s = String.format("UNSUB %d", sub.getSid());
                assertEquals(s, mock.getBuffer());

                SyncSubscriptionImpl syncSub = (SyncSubscriptionImpl) c.subscribeSync("foo");
                syncSub.setMax(10);
                syncSub.delivered.set(8);
                long adjustedMax = (syncSub.getMax() - syncSub.delivered.get());
                assertEquals(2, adjustedMax);
                c.resendSubscriptions();
                c.getOutputStream().flush();
                sleep(100);
                s = String.format("UNSUB %d %d", syncSub.getSid(), adjustedMax);
                assertEquals(s, mock.getBuffer());

            } catch (IOException | TimeoutException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testSubscribeStringMessageHandler() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testSubscribeStringStringMessageHandler() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testSubscribeAsyncStringStringMessageHandler() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testSubscribeAsyncSubjQueue() {
        String subject = "foo";
        String queue = "bar";
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                c.subscribeAsync(subject, queue);
                c.subscribeSync(subject, queue);
            } catch (IOException | TimeoutException e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }
    }

    @Test
    public void testSubscribeAsyncStringMessageHandler() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testSubscribeAsyncString() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testSubscribeSyncStringString() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testSubscribeSyncString() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void test_publish() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testPublishStringStringByteArray() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testPublishStringByteArray() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testPublishMessage() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testPublishIoError() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                Message m = new Message();
                m.setSubject("foo");
                m.setData(null);
                BufferedOutputStream bw = mock(BufferedOutputStream.class);
                doThrow(new IOException("Mock OutputStream write exception")).when(bw)
                        .write(any(byte[].class), any(int.class), any(int.class));
                c.setOutputStream(bw);
                c.publish(m);
            } catch (IOException | TimeoutException e) {
                fail("Unexpected exception: " + e.getMessage());

            }
        }
    }

    @Test
    public void testRequestStringByteArrayLong() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testRequestStringByteArrayLongTimeUnit() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testRequestStringByteArray() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testNewInbox() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            String inbox = c.newInbox();
            assertTrue(inbox.startsWith(ConnectionImpl.inboxPrefix));
            int expectedLength = NUID.totalLen + ConnectionImpl.inboxPrefix.length();
            assertEquals(expectedLength, inbox.length());
        }
    }

    @Test
    public void testSetAndGetStats() throws IOException {
        ConnectionImpl conn = newMockedConnectionImpl();
        Statistics stats = new Statistics();
        stats.incrementInMsgs();
        stats.incrementOutMsgs();
        stats.incrementInBytes(100);
        stats.incrementOutBytes(200);
        stats.incrementFlushes();
        conn.setStats(stats);
        Statistics result = conn.getStats();

        assertEquals(stats.getInMsgs(), result.getInMsgs());
        assertEquals(stats.getOutMsgs(), result.getOutMsgs());
        assertEquals(stats.getInBytes(), result.getInBytes());
        assertEquals(stats.getOutBytes(), result.getOutBytes());
        assertEquals(stats.getFlushes(), result.getFlushes());
    }

    @Test
    public void testResetStats() throws IOException, TimeoutException {
        ConnectionImpl conn = newMockedConnectionImpl();
        Statistics mockStats = mock(Statistics.class);
        conn.setStats(mockStats);
        conn.resetStats();
        verify(mockStats).clear();
    }

    @Test
    public void testGetMaxPayload() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            ServerInfo info = mock(ServerInfo.class);
            long max = 12345;
            when(info.getMaxPayload()).thenReturn(max);
            c.setConnectedServerInfo(info);
            assertEquals(max, c.getMaxPayload());
        }
    }

    @Test
    public void testSendSubscriptionMessage() {
        String infoString =
                "INFO {\"server_id\":\"a1c9cf0c66c3ea102c600200d441ad8e\",\"version\":\"0.7.2\",\"go\":\"go1.4.2\",\"host\":\"0.0.0.0\",\"port\":4222,\"auth_required\":false,\"ssl_required\":false,\"tls_required\":false,\"tls_verify\":false,\"max_payload\":1048576}\r\n";
        final BufferedOutputStream bw = mock(BufferedOutputStream.class);
        final BufferedReader br = mock(BufferedReader.class);
        final BufferedInputStream bis = mock(BufferedInputStream.class);

        byte[] pingBytes = "PING\r\n".getBytes();
        try {
            // When mock gets a PING, it should return a PONG
            doAnswer(new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    when(br.readLine()).thenReturn("PONG");
                    return null;
                }
            }).when(bw).write(pingBytes, 0, pingBytes.length);

            when(br.readLine()).thenReturn(infoString);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        try {
            when(bis.read(any(byte[].class), any(int.class), any(int.class)))
                    .thenAnswer(new Answer<Integer>() {

                        @Override
                        public Integer answer(InvocationOnMock invocation) {
                            UnitTestUtilities.sleep(5000);
                            return -1;
                        }
                    });
        } catch (

        IOException e2) {
            // TODO Auto-generated catch block
            e2.printStackTrace();
        }

        TCPConnection mockConn = mock(TCPConnection.class);

        when(mockConn.isConnected()).thenReturn(true);
        when(mockConn.getBufferedReader()).thenReturn(br);
        when(mockConn.getBufferedInputStream(ConnectionImpl.DEFAULT_STREAM_BUF_SIZE))
                .thenReturn(bis);
        when(mockConn.getBufferedOutputStream(any(int.class))).thenReturn(bw);

        SyncSubscriptionImpl sub = mock(SyncSubscriptionImpl.class);
        when(sub.getSubject()).thenReturn("foo");
        when(sub.getQueue()).thenReturn(null);
        when(sub.getSid()).thenReturn(1L);
        when(sub.getMaxPendingMsgs()).thenReturn(100);

        final AtomicBoolean exThrown = new AtomicBoolean(false);

        String s = String.format(ConnectionImpl.SUB_PROTO, sub.getSubject(),
                sub.getQueue() != null ? " " + sub.getQueue() : "", sub.getSid());

        byte[] bufToExpect = Utilities.stringToBytesASCII(s);
        try {
            doAnswer(new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    exThrown.set(true);
                    throw new IOException("Mock OutputStream sendSubscriptionMessage exception");
                }
            }).when(bw).write(bufToExpect);
        } catch (IOException e1) {
            fail(e1.getMessage());
        }

        try (ConnectionImpl c = new ConnectionFactory().createConnection(mockConn)) {
            c.sendSubscriptionMessage(sub);
            assertTrue("Should have thrown IOException", exThrown.get());
            exThrown.set(false);
            c.status = ConnState.RECONNECTING;
            c.sendSubscriptionMessage(sub);
            assertFalse("Should not have thrown IOException", exThrown.get());
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testSetAndGetClosedCallback() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            ClosedCallback cb = mock(ClosedCallback.class);
            c.setClosedCallback(cb);
            assertEquals(cb, c.getClosedCallback());
        }
    }

    @Test
    public void testSetAndGetDisconnectedCallback() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            DisconnectedCallback cb = mock(DisconnectedCallback.class);
            c.setDisconnectedCallback(cb);
            assertEquals(cb, c.getDisconnectedCallback());
        }
    }

    @Test
    public void testSetAndGetReconnectedCallback() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            ReconnectedCallback cb = mock(ReconnectedCallback.class);
            c.setReconnectedCallback(cb);
            assertEquals(cb, c.getReconnectedCallback());
        }
    }

    @Test
    public void testSetAndGetExceptionHandler() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            ExceptionHandler cb = mock(ExceptionHandler.class);
            c.setExceptionHandler(cb);
            assertEquals(cb, c.getExceptionHandler());
        }
    }

    @Test
    public void testGetConnectedUrl() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            assertEquals("nats://localhost:4222", c.getConnectedUrl());
        }
    }

    @Test
    public void testGetConnectedServerId() throws IOException, TimeoutException {
        String id = "a1c9cf0c66c3ea102c600200d441ad8e";
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            assertEquals(id, c.getConnectedServerId());
        }
    }

    @Test
    public void testGetState() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl c = (ConnectionImpl) cf.createConnection()) {
            assertEquals(ConnState.CONNECTED, c.getState());
        }
    }

    @Test
    public void testGetConnectedServerInfo() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                assertTrue(!c.isClosed());
                ServerInfo info = c.getConnectedServerInfo();
                assertEquals("0.0.0.0", info.getHost());
                assertEquals("0.7.2", info.getVersion());
                assertEquals(4222, info.getPort());
                assertFalse(info.isAuthRequired());
                assertFalse(info.isTlsRequired());
                assertEquals(1048576, info.getMaxPayload());
            } catch (IOException | TimeoutException e) {
                fail("Should not have thrown exception: " + e.getMessage());
            }
        }
    }

    @Test
    public void testSetConnectedServerInfoServerInfo() throws IOException {
        ConnectionImpl conn = newMockedConnectionImpl();
        ServerInfo info = new ServerInfo(TEST_INFO_STRING);
        conn.setConnectedServerInfo(info);
        assertEquals(info, conn.getConnectedServerInfo());
    }

    @Test
    public void testSetConnectedServerInfoString() throws IOException {
        ConnectionImpl conn = newMockedConnectionImpl();
        conn.setConnectedServerInfo(TEST_INFO_STRING);
        assertEquals(TEST_INFO_STRING, conn.getConnectedServerInfo().toString());
    }

    @Test
    public void testGetLastException() throws IOException, TimeoutException {
        ConnectionImpl conn = newMockedConnectionImpl();
        Exception mockEx = mock(IOException.class);
        conn.setLastError(mockEx);
        assertEquals(mockEx, conn.getLastException());
    }

    @Test
    public void testGetOptions() throws IOException {
        Options mockOpts = mock(Options.class);
        ConnectionImpl conn = newMockedConnectionImpl(mockOpts);
        assertEquals(mockOpts, conn.getOptions());
    }

    @Test
    public void testSetAndGetPending() throws IOException {
        ByteArrayOutputStream mockStream = mock(ByteArrayOutputStream.class);
        ConnectionImpl conn = newMockedConnectionImpl();
        conn.setPending(mockStream);
        assertEquals(mockStream, conn.getPending());
    }

    @Test
    public void testSleepMsec() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testSetAndGetOutputStream() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl conn = (ConnectionImpl) cf.createConnection()) {
            OutputStream out = mock(OutputStream.class);
            conn.setOutputStream(out);
            assertEquals(out, conn.getOutputStream());
        }
    }

    @Test
    public void testSetAndGetInputStream() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl conn = (ConnectionImpl) cf.createConnection()) {
            BufferedInputStream is = mock(BufferedInputStream.class);
            conn.setInputStream(is);
            assertEquals(is, conn.getInputStream());
        }
    }

    @Test
    public void testSetAndGetPongs() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl conn = (ConnectionImpl) cf.createConnection()) {
            ArrayList<Channel<Boolean>> pongs = new ArrayList<Channel<Boolean>>();
            conn.setPongs(pongs);
            assertEquals(pongs, conn.getPongs());
        }
    }

    @Test
    public void testSetAndGetSubs() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl conn = (ConnectionImpl) cf.createConnection()) {
            Map<Long, SubscriptionImpl> subs = new HashMap<Long, SubscriptionImpl>();
            conn.setSubs(subs);
            assertEquals(subs, conn.getSubs());
        }
    }

    @Test
    public void testSetAndGetServerPool() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl conn = (ConnectionImpl) cf.createConnection()) {
            List<Srv> pool = new ArrayList<Srv>();
            conn.setServerPool(pool);
            assertEquals(pool, conn.getServerPool());
        }
    }

    @Test
    public void testGetPendingByteCount() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testSetAndGetFlushChannel() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl conn = (ConnectionImpl) cf.createConnection()) {
            Channel<Boolean> fch = new Channel<Boolean>();
            conn.setFlushChannel(fch);
            assertEquals(fch, conn.getFlushChannel());
        }
    }

    @Test
    public void testSetAndGetTcpConnection() throws IOException, TimeoutException {
        ConnectionFactory cf = new MockConnectionFactory();
        try (ConnectionImpl conn = (ConnectionImpl) cf.createConnection()) {
            TCPConnection tconn = mock(TCPConnection.class);
            conn.setTcpConnection(tconn);
            assertEquals(tconn, conn.getTcpConnection());
        }
    }

    @Test
    public void testGetHandlers() {
        try (TCPConnectionMock mock = new TCPConnectionMock()) {
            try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
                assertNull(c.getClosedCallback());
                assertNull(c.getReconnectedCallback());
                assertNull(c.getDisconnectedCallback());
                assertNull(c.getExceptionHandler());
            } catch (IOException | TimeoutException e) {
                fail("Unexpected exception: " + e.getMessage());
            }
        }
    }

    // TODO finish this test
    @Test
    public void testDoReconnectIoErrors() {
        String infoString =
                "INFO {\"server_id\":\"a1c9cf0c66c3ea102c600200d441ad8e\",\"version\":\"0.7.2\",\"go\":\"go1.4.2\",\"host\":\"0.0.0.0\",\"port\":4222,\"auth_required\":false,\"ssl_required\":false,\"tls_required\":false,\"tls_verify\":false,\"max_payload\":1048576}\r\n";
        final BufferedOutputStream bw = mock(BufferedOutputStream.class);
        final BufferedReader br = mock(BufferedReader.class);
        byte[] pingBytes = "PING\r\n".getBytes();
        try {
            // When mock gets a PING, it should return a PONG
            doAnswer(new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    when(br.readLine()).thenReturn("PONG");
                    return null;
                }
            }).when(bw).write(pingBytes, 0, pingBytes.length);

            when(br.readLine()).thenReturn(infoString);
        } catch (IOException e) {
            fail(e.getMessage());
        }
        TCPConnection mockConn = mock(TCPConnection.class);
        when(mockConn.isConnected()).thenReturn(true);
        when(mockConn.getBufferedReader()).thenReturn(br);
        BufferedInputStream bis = mock(BufferedInputStream.class);
        when(mockConn.getBufferedInputStream(ConnectionImpl.DEFAULT_STREAM_BUF_SIZE))
                .thenReturn(bis);
        when(mockConn.getBufferedOutputStream(any(int.class))).thenReturn(bw);

        try (ConnectionImpl c = new ConnectionFactory().createConnection(mockConn)) {
            assertFalse(c.isClosed());
            c.sendPing(null);
            c.sendPing(null);

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            fail("Unexpected exception: " + e.getMessage());
        }
    }



}
