package edu.cmu.pdl.shardfs.lock;

import org.apache.thrift.TException;

import java.io.IOException;

/**
 * Interface for lock service.
 */
public interface LockService {

  public void AcquireLock(String name, String log) throws IOException, TException;

  public void ReleaseLock(String name) throws TException;
}
