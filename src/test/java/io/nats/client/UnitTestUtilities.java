/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class UnitTestUtilities {
    static final Logger logger = LoggerFactory.getLogger(UnitTestUtilities.class);

    // final Object mu = new Object();
    static NATSServer defaultServer = null;
    Process authServerProcess = null;

    public static synchronized void startDefaultServer() {
        startDefaultServer(false);
    }

    public static synchronized void startDefaultServer(boolean debug) {
        if (defaultServer == null) {
            defaultServer = new NATSServer(debug);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }
    }

    public static synchronized NATSServer runDefaultServer() {
        return runDefaultServer(false);
    }

    public static synchronized NATSServer runDefaultServer(boolean debug) {
        if (defaultServer == null) {
            defaultServer = new NATSServer(debug);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }
        return defaultServer;
    }

    public static synchronized void stopDefaultServer() {
        if (defaultServer != null) {
            defaultServer.shutdown();
            defaultServer = null;
        }
    }

    public static synchronized void bounceDefaultServer(int delayMillis) {
        stopDefaultServer();
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            // NOOP
        }
        startDefaultServer();
    }

    public void startAuthServer() throws IOException {
        authServerProcess = Runtime.getRuntime().exec("gnatsd -config auth.conf");
    }

    NATSServer createServerOnPort(int p) {
        return createServerOnPort(p, false);
    }

    NATSServer createServerOnPort(int p, boolean debug) {
        NATSServer n = new NATSServer(p, debug);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
        return n;
    }

    NATSServer createServerWithConfig(String configFile) {
        return createServerWithConfig(configFile, false);
    }

    NATSServer createServerWithConfig(String configFile, boolean debug) {
        NATSServer n = new NATSServer(configFile, debug);
        sleep(500);
        return n;
    }

    public static String getCommandOutput(String command) {
        String output = null; // the string to return

        Process process = null;
        BufferedReader reader = null;
        InputStreamReader streamReader = null;
        InputStream stream = null;

        try {
            process = Runtime.getRuntime().exec(command);

            // Get stream of the console running the command
            stream = process.getInputStream();
            streamReader = new InputStreamReader(stream);
            reader = new BufferedReader(streamReader);

            String currentLine = null; // store current line of output from the
                                       // cmd
            StringBuilder commandOutput = new StringBuilder(); // build up the
                                                               // output from
                                                               // cmd
            while ((currentLine = reader.readLine()) != null) {
                commandOutput.append(currentLine + "\n");
            }

            int returnCode = process.waitFor();
            if (returnCode == 0) {
                output = commandOutput.toString();
            }

        } catch (IOException e) {
            System.err.println("Cannot retrieve output of command");
            System.err.println(e);
            output = null;
        } catch (InterruptedException e) {
            System.err.println("Cannot retrieve output of command");
            System.err.println(e);
        } finally {
            // Close all inputs / readers

            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    System.err.println("Cannot close stream input! " + e);
                }
            }
            if (streamReader != null) {
                try {
                    streamReader.close();
                } catch (IOException e) {
                    System.err.println("Cannot close stream input reader! " + e);
                }
            }
            if (reader != null) {
                try {
                    streamReader.close();
                } catch (IOException e) {
                    System.err.println("Cannot close stream input reader! " + e);
                }
            }
        }
        // Return the output from the command - may be null if an error occured
        return output;
    }

    void getConnz() {
        URL url = null;
        try {
            url = new URL("http://localhost:8222/connz");
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"))) {
            for (String line; (line = reader.readLine()) != null;) {
                System.out.println(line);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    static void sleep(int timeout) {
        sleep(timeout, TimeUnit.MILLISECONDS);
    }

    static void sleep(int duration, TimeUnit unit) {
        try {
            unit.sleep(duration);
        } catch (InterruptedException e) {
        }
    }

    static boolean wait(Channel<Boolean> ch) {
        return waitTime(ch, 5, TimeUnit.SECONDS);
    }

    static boolean waitTime(Channel<Boolean> ch, long timeout, TimeUnit unit) {
        boolean val = false;
        try {
            val = ch.get(timeout, unit);
        } catch (TimeoutException e) {
        }
        return val;
    }

    public static ConnectionImpl newDefaultConnection() throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory();
        return (ConnectionImpl) cf.createConnection();
    }

    public static TCPConnection newMockedTcpConnection() throws IOException {
        return newMockedTcpConnection(false);
    }

    public static TCPConnection newMockedTcpConnection(final boolean verbose) throws IOException {
        final String info =
                "INFO {\"server_id\":\"a1c9cf0c66c3ea102c600200d441ad8e\",\"version\":\"0.7.2\",\"go\":\"go1.4.2\",\"host\":\"0.0.0.0\",\"port\":4222,\"auth_required\":false,\"ssl_required\":false,\"tls_required\":false,\"tls_verify\":false,\"max_payload\":1048576}\r\n";
        byte[] pingBytes = ConnectionImpl.PING_PROTO.getBytes();
        byte[] pongBytes = ConnectionImpl.PONG_PROTO.getBytes();
        String pongOp = ConnectionImpl._PONG_OP_;


        TCPConnection conn = mock(TCPConnection.class);
        final BufferedReader reader = mock(BufferedReader.class);
        final BufferedInputStream bis = mock(BufferedInputStream.class);
        final BufferedOutputStream bos = mock(BufferedOutputStream.class);

        when(conn.getBufferedReader()).thenReturn(reader);
        when(conn.getBufferedInputStream(any(int.class))).thenReturn(bis);
        when(conn.getBufferedOutputStream(any(int.class))).thenReturn(bos);

        // Handle initial connection exchange
        AtomicBoolean connected = new AtomicBoolean(false);
        when(reader.readLine()).then(new Answer<String>() {
            private int count = 0;

            public String answer(InvocationOnMock invocation) {
                String result = null;
                // Object[] args = invocation.getArguments();
                // Mock mock = (Mock) invocation.getMock();
                if (++count == 1) {
                    if (verbose) {
                        System.err.printf("HANDSHAKE: Sending %s\n", info.trim());
                    }
                    result = info;
                } else {
                    if (verbose) {
                        System.err.println("HANDSHAKE: Sending PONG to client");
                    }
                    result = pongOp;
                    connected.set(true);
                }
                return result;
            }
        });

        // Handle CONNECT, SUB, UNSUB
        doAnswer(new Answer<Void>() {
            // SUB, UNSUB, and CONNECT are the only ops that call write with one arg.
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                byte[] line = (byte[]) args[0];
                String arg = new String(line).trim();
                // int offset = (int) args[1];
                // int len = (int) args[2];
                if (verbose) {
                    if (arg.startsWith("CONNECT")) {
                        System.err.printf("HANDSHAKE: Received %s\n", arg);
                    } else {
                        System.err.printf("MockServer: <== %s\n", new String(line).trim());
                    }
                }
                return null;
            }
        }).when(bos).write(any(byte[].class));

        // Action taken on any bw.write(byte[], int int)
        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                byte[] line = (byte[]) args[0];
                int offset = (int) args[1];
                int len = (int) args[2];
                String arg = new String(line, offset, len);
                if (verbose) {
                    if (!connected.get() && arg.startsWith(ConnectionImpl.PING_PROTO)) {
                        System.err.println("HANDSHAKE: Received PING");
                    } else {
                        System.err.printf("MockServer (ANY): <== %s\n",
                                new String(line, offset, len).trim());
                    }
                }
                return null;
            }
        }).when(bos).write(any(byte[].class), any(int.class), any(int.class));
        return conn;
    }

    public static ConnectionImpl newMockedConnection() throws IOException, TimeoutException {
        return newMockedConnection(false);
    }

    public static ConnectionImpl newMockedConnection(final boolean verbose)
            throws IOException, TimeoutException {
        ConnectionImpl nc = newMockedConnectionImpl(null, verbose);
        nc.connect();
        return nc;
    }

    public static ConnectionImpl newMockedConnectionImpl() throws IOException {
        return newMockedConnectionImpl((Options) null);
    }

    public static ConnectionImpl newMockedConnectionImpl(Options opts) throws IOException {
        return newMockedConnectionImpl(opts, false);
    }

    public static ConnectionImpl newMockedConnectionImpl(Options copts, final boolean verbose)
            throws IOException {
        Options opts = null;
        if (copts == null) {
            opts = new Options();
        } else {
            opts = copts;
        }
        opts.setUrl("nats://localhost:4222");
        TCPConnection conn = newMockedTcpConnection(verbose);
        ConnectionImpl nc = new ConnectionImpl(opts, conn);
        return nc;
    }
}
