package edu.cmu.pdl.shardfs;

import edu.cmu.pdl.indexfs.client.FileStat;
import edu.cmu.pdl.indexfs.client.IndexFS;
import edu.cmu.pdl.indexfs.client.IndexFSFactory;
import edu.cmu.pdl.indexfs.rpc.*;
import edu.cmu.pdl.shardfs.lock.SimpleLock;
import edu.cmu.pdl.shardfs.util.Murmur3_32Hash;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.thrift.TException;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

/**
 * ShardFS on top of stand alone IndexFS servers
 */
public class ShardIndexFS extends AbstractFileSystem {

  public static final String SHARDRECFS_URI_SCHEME = "shardindexFS";

  private static final int MAXRETRY = 10;
  private static final String HASHLASL_PROP = "hash.fileonly";
  private static final short DEFAULT_PERM = 0;
  private static final FileStatus FILESTAT_DIR = new FileStatus(0, true, 3, 1024, 0, 0, null, "", "", null, null);
  private static final FileStatus FILESTAT_FILE = new FileStatus(0, false, 3, 1024, 0, 0, null, "", "", null, null);
  public static String CONF_STRING_SHARD_SERVERS = "fs.index.servers";
  private static final String IDXZK = "shardindexfs.zk.address";
  public static final String READONLY_PROP = "shardreadonly"; // whether to convert all operaitons into stat
  // dir mutations will send stat to all MDSs

  private ArrayList<IndexFS> bucket; // A list of metadata servers
  private String[] servers = null;
  private int bsize;
  final static AtomicInteger lockid = new AtomicInteger(0);  //differentiate locks acquired by different threads
  private boolean readonly = false;

  private ExecutorService executor;


  private ThreadLocal<SimpleLock> lock = new ThreadLocal<SimpleLock>() {
    @Override
    protected SimpleLock initialValue() {
      return new SimpleLock(lockid.getAndIncrement());
    }
  };

  private FsServerDefaults serverDefaults;

  public ShardIndexFS(final URI theUri, final Configuration conf)
          throws IOException, URISyntaxException {
    super(theUri, ShardIndexFS.SHARDRECFS_URI_SCHEME, false, -1);

    if (!theUri.getScheme().equalsIgnoreCase(SHARDRECFS_URI_SCHEME)) {
      throw new IllegalArgumentException(
              "Passed URI's scheme is not for shardIndexFS");
    }


    servers = conf.getStrings(CONF_STRING_SHARD_SERVERS,
            new String[0]);
    readonly = conf.getBoolean(READONLY_PROP, false);
    if (servers.length < 1)
      throw new IOException("An invalid server list: "
              + conf.get(CONF_STRING_SHARD_SERVERS));
    bucket = new ArrayList<IndexFS>();

    IndexFSFactory fac = new IndexFSFactory();
    for (String server : servers) {
      System.out.println("server: " + server);
      bucket.add(fac.createIndexFS(server, 45678));
    }
    for (int i = 0; i < servers.length; i++) {
      servers[i] = "/" + servers[i];
    }
    bsize = servers.length;


    executor = Executors.newFixedThreadPool(bsize);
    conf.set(Constants.ZKAddress, conf.get(IDXZK));
    try {
      lock.get().setServer(conf.get(IDXZK));
    } catch (TException e) {
      e.printStackTrace();
      throw new IOException(e);
    }


    // Get the checksum type from config
    String checksumTypeStr = conf.get(DFS_CHECKSUM_TYPE_KEY, DFS_CHECKSUM_TYPE_DEFAULT);
    DataChecksum.Type checksumType;
    try {
      checksumType = DataChecksum.Type.valueOf(checksumTypeStr);
    } catch (IllegalArgumentException iae) {
      throw new IOException("Invalid checksum type in "
              + DFS_CHECKSUM_TYPE_KEY + ": " + checksumTypeStr);
    }
    this.serverDefaults = new FsServerDefaults(
            conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT),
            conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY, DFS_BYTES_PER_CHECKSUM_DEFAULT),
            conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY, DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT),
            (short) conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT),
            conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
            conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY, DFS_ENCRYPT_DATA_TRANSFER_DEFAULT),
            conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT),
            checksumType);

  }


  @Override
  public int getUriDefaultPort() {
    return -1;
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return serverDefaults;
  }

  @Override
  public FSDataOutputStream createInternal(Path f, EnumSet<CreateFlag> flag, FsPermission absolutePermission,
                                           int bufferSize, short replication, long blockSize, Progressable progress,
                                           Options.ChecksumOpt checksumOpt,
                                           boolean createParent) throws AccessControlException,
          FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException,
          UnsupportedFileSystemException, UnresolvedLinkException, IOException {
    int i = getTargetFileSystemIndex(f);
    IndexFS fs = bucket.get(i);
    f = getLocalPath(f);
    try {
      fs.createFile(f.toString(), DEFAULT_PERM);
    } catch (IllegalPath illegalPath) {
      illegalPath.printStackTrace();
    } catch (NotADirectory notADirectory) {
      throw new ParentNotDirectoryException(notADirectory.getPath());
    } catch (FileAlreadyExists fileAlreadyExists) {
      throw new FileAlreadyExistsException(f.toString());
    } catch (ParentPathNotFound parentPathNotFound) {
      parentPathNotFound.printStackTrace();
      throw new FileNotFoundException(f.toString());
    } catch (TException e) {
      throw new IOException(e);
    }
    return new FSDataOutputStream(new ByteArrayOutputStream(), statistics);

  }


  /**
   * Create directory for the given path without creating its parent
   *
   * @param dir
   * @param permission
   * @throws org.apache.hadoop.security.AccessControlException
   * @throws org.apache.hadoop.fs.FileAlreadyExistsException
   * @throws java.io.FileNotFoundException
   * @throws org.apache.hadoop.fs.UnresolvedLinkException
   * @throws java.io.IOException
   */
  private void mkdir(Path dir, final FsPermission permission)
          throws ParentPathNotFound, IOException {
    // special case
    if (bsize == 1) {
      try {
        if (readonly)
          bucket.get(0).getFileStat(serverPath(0, dir));
        else
          bucket.get(0).createDirectory(serverPath(0, dir), permission.toShort());
      } catch (ParentPathNotFound parentPathNotFound) {
        throw parentPathNotFound;
      } catch (FileAlreadyExists fileAlreadyExists) {
        //fileAlreadyExists.printStackTrace();
      } catch (IOError ioError) {
        ioError.printStackTrace();
        throw new IOException(ioError);
      } catch (ServerInternalError serverInternalError) {
        serverInternalError.printStackTrace();
        throw new IOException(serverInternalError);
      } catch (IllegalPath illegalPath) { // the path either doesn't start with '/' or ends with '/'(except for root)
        illegalPath.printStackTrace();
        throw new IOException(illegalPath);
      } catch (NotADirectory notADirectory) { // one of the ancestors is a file, contains the path which is a file
        notADirectory.printStackTrace();
        throw new IOException(notADirectory);
      } catch (NoSuchFileOrDirectory noSuchForD) {
        throw new IOException(noSuchForD);
      }

      return;
    }

    // Only createParent = false is supported
    // 1. Test if the path already exists
    int fs_index = this.getTargetFileSystemIndex(dir);
    IndexFS fs = this.getTargetFileSystem(fs_index);
    dir = getLocalPath(dir);
    String dirpath = dir.toString();


    try {
      FileStat fi = fs.getFileStat(dirpath);
      // No exception from getfileStatus means the path exists, so mkdir fails
      if (fi.isDir())
        return;
      else
        throw new FileAlreadyExistsException();

    } catch (NoSuchFileOrDirectory noSuchFileOrDirectory) {//parent exist, but not the last component

    } catch (ParentPathNotFound parentPathNotFound) {// one of the ancestors does not exist
      throw parentPathNotFound;
    } catch (IOError ioError) {
      throw new IOException(ioError);
    } catch (ServerInternalError serverInternalError) {
      throw new IOException(serverInternalError);
    } catch (IllegalPath illegalPath) { // the path either doesn't start with '/' or ends with '/'(except for root)
      throw new IOException(illegalPath);
    } catch (NotADirectory notADirectory) { // one of the ancestors is a file, contains the path which is a file
      throw new IOException(notADirectory);
    }

    TException e;
    e = null;
    IOException ioe = null;


    // 2. Grab the lock for the path
    boolean unlock = false;
    try {
      String log = "mkdir";
      lock.get().AcquireLock(dirpath, log);
      unlock = true;


      // 3. Try to create the directory on primary server
      try {
        if (readonly) {
          try {
            fs.getFileStat(serverPath(fs_index, dir));
          } catch (TException e1) {
          }
        } else
          fs.createDirectory(serverPath(fs_index, dir), permission.toShort());
      } catch (FileAlreadyExists eexist) {
        // someone always grabbed the lock first and created the directory
        return;
      } catch (TException e1) {
        // We'll throw the exception after lock is released
        e = e1;
      }
      // Successfully mkdir on the primary node, we'll roll forward
      if (e == null) {
        if (readonly) {
          statAll(dir, fs_index, true);
        } else
          // Try to create directories on all other servers
          if (!mkdirAll(dir, permission.toShort(), fs_index)) {
            // Fail to mkdir on some namenodes
            ioe = new IOException("Mkdir failed on some namenodes!");
//            unlock = false;
          }
      }


    } catch (TException e1) {
      e1.printStackTrace();
    } finally {
      if (unlock) {
        try {
          lock.get().ReleaseLock(dirpath);
        } catch (TException e1) {
          e1.printStackTrace();
        }
      }
      if (e != null)
        throw new IOException(e);
      else if (ioe != null)
        throw ioe;
    }

  }

  public class MkdirCallable implements Callable<Object> {
    int i;
    short perm;
    Path path;

    public MkdirCallable(int i, Path path, short permission) {
      this.i = i;
      this.perm = permission;
      this.path = path;
    }

    @Override
    public Object call() throws IOException, TException, UnresolvedLinkException {
      bucket.get(i).createDirectory(serverPath(i, path), perm);
      return null;
    }
  }

  public class StatCallable implements Callable<Object> {
    int i;
    Path path;

    public StatCallable(int i, Path path) {
      this.i = i;

      this.path = path;
    }

    @Override
    public Object call() throws IOException, TException, UnresolvedLinkException {
      bucket.get(i).getFileStat(serverPath(i, path));
      return null;
    }
  }

  public class SetPermCallable implements Callable<Object> {
    int i;
    short perm;
    Path path;

    public SetPermCallable(int i, Path path, short permission) {
      this.i = i;
      this.perm = permission;
      this.path = path;
    }

    @Override
    public Object call() throws IOException, TException, UnresolvedLinkException {
      bucket.get(i).setPermission(serverPath(i, path), perm);
      return null;
    }
  }

  /**
   * Create directory on all Namenodes except primary node
   *
   * @param dir        path to create
   * @param permission permission
   * @param fs_index   index for the primary node
   */
  private boolean mkdirAll(final Path dir, final short permission, int fs_index) {
    List<Future<?>> pendingOps;
    pendingOps = new ArrayList<Future<?>>(bsize - 1);
    int i, j;
    i = fs_index;
    boolean succ = true;
    for (j = 0; j < bsize - 1; j++) {
      i = (i + 1) % bsize;
      pendingOps.add(executor.submit(new MkdirCallable(i, dir, permission)));
    }
    for (Future<?> future : pendingOps) {
      try {
        future.get();
      } catch (InterruptedException e) {
        succ = false;
        e.printStackTrace();
      } catch (ExecutionException e) {
        succ = false;
        e.printStackTrace();
        e.getCause().printStackTrace();
      }
    }
    return succ;
  }


  /**
   * Stat directory on all Namenodes except primary node
   *
   * @param dir      path to create
   * @param fs_index index for the primary node
   * @param exclude  whether to exclude the primary node
   */
  private boolean statAll(final Path dir, int fs_index, boolean exclude) {
    List<Future<?>> pendingOps;
    pendingOps = new ArrayList<Future<?>>(bsize - 1);
    int i, j;
    i = fs_index;
    boolean succ = true;
    for (j = 0; j < bsize - 1; j++) {
      i = (i + 1) % bsize;
      pendingOps.add(executor.submit(new StatCallable(i, dir)));
    }
    if (!exclude)
      pendingOps.add(executor.submit(new StatCallable((i + 1) % bsize, dir)));
    for (Future<?> future : pendingOps) {
      try {
        future.get();
      } catch (InterruptedException e) {
        succ = false;
      } catch (ExecutionException e) {
        succ = false;
      }
    }
    return succ;
  }


  /**
   * Create directory on all Namenodes except primary node
   *
   * @param dir        path to create
   * @param permission permission
   */
  private boolean setPermAll(final Path dir, final short permission) {
    List<Future<?>> pendingOps;

    pendingOps = new ArrayList<Future<?>>(bsize - 1);
    int i, j;
    boolean succ = true;
    for (j = 0; j < bsize; j++) {
      pendingOps.add(executor.submit(new SetPermCallable(j, dir, permission)));
    }
    for (Future<?> future : pendingOps) {
      try {
        future.get();
      } catch (InterruptedException e) {
        succ = false;
      } catch (ExecutionException e) {
        succ = false;
      }
    }

    return succ;
  }


  @Override
  public void mkdir(Path dir, FsPermission permission,
                    boolean createParent) throws AccessControlException, FileAlreadyExistsException,
          FileNotFoundException, UnresolvedLinkException, IOException {

    try {
      mkdir(dir, permission);
    } catch (ParentPathNotFound e) {
      if (!createParent)
        throw new IOException(e);
      // at this point, we know the parent dir doesn't exist
      String dir_path = dir.toString();
      String path = e.getPath();
      try {
        for (int len = path.length(); len < dir_path.length(); len++) {
          if (dir_path.charAt(len - 1) == Path.SEPARATOR_CHAR) {
            mkdir(new Path(dir_path.substring(0, len)), permission);
          }
        }
        mkdir(dir, permission);
      } catch (ParentPathNotFound parentPathNotFound) {
        parentPathNotFound.printStackTrace();
        throw new IOException(parentPathNotFound);
      }

    }

  }

  @Override
  public boolean delete(Path f,
                        boolean recursive) throws AccessControlException, FileNotFoundException,
          UnresolvedLinkException, IOException {
    return false;
  }

  @Override
  public FSDataInputStream open(Path f,
                                int bufferSize) throws AccessControlException, FileNotFoundException,
          UnresolvedLinkException, IOException {

    throw new IOException("not implemented");
  }

  @Override
  public boolean setReplication(Path f,
                                short replication) throws AccessControlException, FileNotFoundException,
          UnresolvedLinkException, IOException {
    throw new IOException("not implemented");
  }

  @Override
  public void renameInternal(Path src,
                             Path dst) throws AccessControlException, FileAlreadyExistsException,
          FileNotFoundException, ParentNotDirectoryException, UnresolvedLinkException, IOException {
    throw new IOException("not implemented");
  }

  @Override
  public void setPermission(Path f,
                            FsPermission permission) throws AccessControlException, FileNotFoundException,
          UnresolvedLinkException, IOException {
    setPermissionB(f, permission);
  }


  // Return true when the path is a directory
  public boolean setPermissionB(Path f,
                                FsPermission permission) throws AccessControlException, FileNotFoundException,
          UnresolvedLinkException, IOException {

    int fs_index = getTargetFileSystemIndex(f);

    FileStat stat = null;
    // setFilePermission : specifically for files

    try {
      if (readonly) {
        stat = bucket.get(fs_index).getFileStat(serverPath(fs_index, f));
        if (!stat.isDir())
          return false;
      } else {
        bucket.get(fs_index).setFilePermission(serverPath(fs_index, f), permission.toShort());
        return false;
      }

    } catch (NotAFile e) {
      // directory
    } catch (NoSuchFileOrDirectory e) {
      throw new FileNotFoundException(f.toString());
    } catch (TException e) {
      throw new IOException(e);
    }


    // path f is a directory
    // 2. Grab the lock for the path
    boolean unlock = false;
    IOException e3 = null;
    String localpath = getLocalPath(f).toString();
    try {
      lock.get().AcquireLock(localpath, "setPerm");
      unlock = true;


      if (readonly) {
        statAll(f, fs_index, false);
      } else
        // change permissions on all servers
        if (!setPermAll(f, permission.toShort())) {
          // Fail to mkdir on some namenodes
          e3 = new IOException("Set permission failed on some namenodes!");
          //unlock = false;
        }


    } catch (TException e) {
      e.printStackTrace();
    } finally {
      if (unlock) {
        try {
          lock.get().ReleaseLock(localpath);
        } catch (TException e) {
          e.printStackTrace();
        }
      }
      if (e3 != null)
        throw e3;
    }
    return true;

  }

  @Override
  public void setOwner(Path f, String username,
                       String groupname) throws AccessControlException, FileNotFoundException,
          UnresolvedLinkException, IOException {
    throw new IOException("not implemented");
  }

  @Override
  public void setTimes(Path f, long mtime,
                       long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException,
          IOException {
    throw new IOException("not implemented");
  }

  @Override
  public FileChecksum getFileChecksum(
          Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    throw new IOException("not implemented");
  }

  @Override
  public FileStatus getFileStatus(
          Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    int idx = getTargetFileSystemIndex(f);
    IndexFS fs = getTargetFileSystem(idx);
    String path = serverPath(idx, f);
    try {
      FileStat stat = fs.getFileStat(path);
      if (stat.isDir())
        return FILESTAT_DIR;
      else
        return FILESTAT_FILE;
    } catch (NoSuchFileOrDirectory noSuchFileOrDirectory) {
      throw new FileNotFoundException(path);
    } catch (TException e) {
      throw new IOException(e);
    }

  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path f, long start,
                                               long len) throws AccessControlException, FileNotFoundException,
          UnresolvedLinkException, IOException {
    throw new IOException("not implemented");
  }

  @Override
  public FsStatus getFsStatus() throws AccessControlException, FileNotFoundException, IOException {
    throw new IOException("not implemented");
  }

  @Override
  public FileStatus[] listStatus(
          Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    throw new IOException("not implemented");
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) throws AccessControlException, IOException {

  }


  private int getTargetFileSystemIndex(Path path) {
    return getHash(path) % bsize;
  }


  // path passed in is always fully qualified & absolute.
  private int getHash(Path path) {
    return Murmur3_32Hash.hash(path.toUri().getPath()) & 0x7fffffff;
  }

  private Path getLocalPath(Path path) {
    return new Path(path.toUri().getPath());
  }


  private String serverPath(int i, String f) {
    return f;
    //return new Path(servers[i]+f);
  }

  private String serverPath(int i, Path f) {
    return f.toString();
    //Path p = new Path(servers[i] + f.toString());
    //return p;
  }

  private IndexFS getTargetFileSystem(int index) {
    return bucket.get(index);
  }


  private abstract class FSOperation {
    public abstract void run(IndexFS fs) throws IOException, TException;
  }


}
