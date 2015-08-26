package edu.cmu.pdl.shardfs.lock;

import edu.cmu.pdl.lockserver.LockServer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;

/**
 * SimpleLock implements the LockService interface by contacting the lock server without handling
 * failures and recovery.
 */
public class SimpleLock implements LockService {

  private LockServer.Iface client = null;
  private String server = null;
  private int id = 0;
  private static int server_port = 55555;

  public SimpleLock(int id)
  {
   this.id = id;
  }

  public SimpleLock(String server_address) throws TException {
    setServer(server_address);


  }

  public void setServer(String server_address) throws TException {
    if (server_address.indexOf(":") > 0) {
      server_address =server_address.substring(0, server_address.indexOf(":"));
      this.server = server_address;
    }
    TTransport socket = new TSocket(server_address, server_port);
    client = new LockServer.Client(new TBinaryProtocol(socket));
    socket.open();

  }

  public void connectServer() throws TException{
    TTransport socket = new TSocket(server, server_port);
    client = new LockServer.Client(new TBinaryProtocol(socket));
    socket.open();
  }

  @Override
  public void AcquireLock(String name, String log) throws IOException, TException {
    if (client == null) {
      connectServer();
    }
    client.AcquireLock(name, log);
  }

  @Override
  public void ReleaseLock(String name) throws TException {
    client.ReleaseLock(name);
  }
}
