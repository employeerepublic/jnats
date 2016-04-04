package io.nats.client;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class MockConnectionFactory extends ConnectionFactory {
    private TCPConnection tcpConn = new TCPConnectionMock();

    public MockConnectionFactory(Properties props) {
        super(props);
        // TODO Auto-generated constructor stub
    }

    public MockConnectionFactory() {
        // TODO Auto-generated constructor stub
    }

    public MockConnectionFactory(String url) {
        super(url);
        // TODO Auto-generated constructor stub
    }

    public MockConnectionFactory(String[] servers) {
        super(servers);
        // TODO Auto-generated constructor stub
    }

    public MockConnectionFactory(String url, String[] servers) {
        super(url, servers);
        // TODO Auto-generated constructor stub
    }

    public MockConnectionFactory(ConnectionFactory cf) {
        super(cf);
        // TODO Auto-generated constructor stub
    }

    TCPConnection initTcpConnection() {
        return tcpConn;
    }

    @Override
    public Connection createConnection() throws IOException, TimeoutException {
        return createConnection(tcpConn);
    }

    @Override
    public ConnectionImpl createConnection(TCPConnection conn)
            throws IOException, TimeoutException {
        return super.createConnection(conn);
    }

    public TCPConnection getTcpConnectionMock() {
        return tcpConn;
    }

}
