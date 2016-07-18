package jzipline.utils;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.CopyOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemLoopException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/*
Caching utilities for zipline
*/

//from collections import namedtuple, MutableMapping
//import errno
//import os
//import pickle
//from shutil import rmtree, copyfile, copytree
//from tempfile import mkdtemp, NamedTemporaryFile
//
//import pandas as pd

public class Cache {
    
//        """Marks that a :class:`CachedObject` has expired.
    public static class ExpiredException extends RuntimeException {
        private static final long serialVersionUID = -2139848392578799699L;

        public ExpiredException( final LocalDateTime expires ) {
            super( String.format( "Expired: %s", expires.toString() ) );
        }
    }
    
    
    /*
        A simple struct for maintaining a cached object with an expiration date.
    
        Parameters
        ----------
        value : object
            The object to cache.
        expires : datetime-like
            Expiration date of `value`. The cache is considered invalid for dates
         **strictly greater** than `expires`.
    
        Usage
        -----
        >>> from pandas import Timestamp, Timedelta
        >>> expires = Timestamp('2014', tz='UTC')
        >>> obj = CachedObject(1, expires)
        >>> obj.unwrap(expires - Timedelta('1 minute'))
        1
        >>> obj.unwrap(expires)
        1
        >>> obj.unwrap(expires + Timedelta('1 minute'))
        Traceback (most recent call last):
            ...
        Expired: 2014-01-01 00:00:00+00:00
     */
    public static class CachedObject {
    
        private final Object value;
        private final LocalDateTime expires;

        public CachedObject( Object value, LocalDateTime expiration ) {
            this.value = value;
            this.expires = expiration;
        }

        /**
            Get the cached value.
    
            Returns
            -------
            value : object
                The cached value.
    
            Raises
            ------
            Expired
                Raised when `dt` is greater than self.expires.
         */
        public Object unwrap( final LocalDateTime dt ) {
            if( dt.isAfter( expires ) )
                throw new ExpiredException( expires );
            
            return value;
        }
    }
    
    
    /**
        A cache of multiple CachedObjects, which returns the wrapped the value
        or raises and deletes the CachedObject if the value has expired.
    
        Parameters
        ----------
        cache : dict-like, optional
            An instance of a dict-like object which needs to support at least:
            `__del__`, `__getitem__`, `__setitem__`
            If `None`, than a dict is used as a default.
    
        Usage
        -----
        >>> from pandas import Timestamp, Timedelta
        >>> expires = Timestamp('2014', tz='UTC')
        >>> value = 1
        >>> cache = ExpiringCache()
        >>> cache.set('foo', value, expires)
        >>> cache.get('foo', expires - Timedelta('1 minute'))
        1
        >>> cache.get('foo', expires + Timedelta('1 minute'))
        Traceback (most recent call last):
            ...
        KeyError: 'foo'
     */
    public static class ExpiringCache {
        private Map<Object,CachedObject> cache;

    
        public ExpiringCache( Map<Object,CachedObject> cache ) {
            this.cache = cache != null ? cache : new ConcurrentHashMap<>();
        }
    
        /**
            Get the value of a cached object.
    
            Parameters
            ----------
            key : any
                The key to lookup.
            dt : datetime
                The time of the lookup.
    
            Returns
            -------
            result : any
                The value for ``key``.
    
            Raises
            ------
            KeyError
                Raised if the key is not in the cache or the value for the key
                has expired.
         */
        public Optional<Object> get( Object key, LocalDateTime dt ) {
            try {
                return Optional.ofNullable( cache.get( key ) ).map( co -> co.unwrap( dt ) );
            }
            catch( ExpiredException e ) {
                cache.remove( key );
                return Optional.empty();
            }
        }
    
        /**
            Adds a new key value pair to the cache.
    
            Parameters
            ----------
            key : any
                The key to use for the pair.
            value : any
                The value to store under the name ``key``.
            expiration : datetime
                When should this mapping expire? The cache is considered invalid
                for dates **strictly greater** than ``expiration_dt``.
         */
        public void set( Object key, Object value, LocalDateTime expiration ) {
            cache.put( key, new CachedObject( value, expiration ) );
        }
    }
    
    
    /*A disk-backed cache for dataframes.
    
        ``dataframe_cache`` is a mutable mapping from string names to pandas
        DataFrame objects.
        This object may be used as a context manager to delete the cache directory
        on exit.
    
        Parameters
        ----------
        path : str, optional
            The directory path to the cache. Files will be written as
            ``path/<keyname>``.
        lock : Lock, optional
            Thread lock for multithreaded/multiprocessed access to the cache.
            If not provided no locking will be used.
        clean_on_failure : bool, optional
            Should the directory be cleaned up if an exception is raised in the
            context manager.
        serialize : {'msgpack', 'pickle:<n>'}, optional
            How should the data be serialized. If ``'pickle'`` is passed, an
            optional pickle protocol can be passed like: ``'pickle:3'`` which says
            to use pickle protocol 3.
    
        Notes
        -----
        The syntax ``cache[:]`` will load all key:value pairs into memory as a
        dictionary.
        The cache uses a temporary file format that is subject to change between
        versions of zipline.
     */
    public static class DataframeCache extends ConcurrentHashMap<Object,Object> implements Closeable {
        private static final long serialVersionUID = 4025453177437943050L;
        
        private final Path path;
        private final Object lock;
        private final boolean cleanOnFailure;
        
        public DataframeCache( Path path, Object lock ) throws IOException {
            this( path, lock, true );
        }
        
        public DataframeCache( Path path, Object lock, boolean cleanOnFailure ) throws IOException {
            this.path = path != null ? path : Files.createTempFile( null, null );
            this.lock = lock != null ? lock : ContextTricks.NO_OP_CONTEXT;
            this.cleanOnFailure = cleanOnFailure;
    
            Paths.ensureDirectory( this.path );
        }
    
        public void serialize( Serializable df, Path path ) throws IOException {
            try( final ObjectOutputStream out = new ObjectOutputStream( Files.newOutputStream( path, StandardOpenOption.CREATE, StandardOpenOption.WRITE ) ) ) {
                out.writeObject( df );
            }
        }
    
        public Path keyPath( String key ) {
            return this.path.resolve( key );
        }
    
    
//        def __getitem__(self, key):
//            if key == slice(None):
//                return dict(self.items())
//    
//            with self.lock:
//                try:
//                    with open(self._keypath(key), 'rb') as f:
//                        return self.deserialize(f)
//                except IOError as e:
//                    if e.errno != errno.ENOENT:
//                        raise
//                    raise KeyError(key)
//    
//        def __setitem__(self, key, value):
//            with self.lock:
//                self.serialize(value, self._keypath(key))
//    
//        def __delitem__(self, key):
//            with self.lock:
//                try:
//                    os.remove(self._keypath(key))
//                except OSError as e:
//                    if e.errno == errno.ENOENT:
//                        # raise a keyerror if this directory did not exist
//                        raise KeyError(key)
//                    # reraise the actual oserror otherwise
//                    raise
    
        public Iterator<Path> iterator() throws IOException {
            return Files.list( path ).iterator();
        }
    
        public long count() throws IOException {
            return Files.list( path ).count();
        }
    
        public String toString() {
            return String.format( "<%s: keys={%s}>", getClass().getSimpleName(), keySet().stream().map( Object::toString ).collect( Collectors.joining( ", " ) ) );
        }

        @Override
        public void close() throws IOException {
//          def __exit__(self, type_, value, tb):
//          if not (self.clean_on_failure or value is None):
//              # we are not cleaning up after a failure and there was an exception
//              return
//  
//          with self.lock:
//              rmtree(self.path)

            deleteDirectory( path );
        }
    }
    
    private static void deleteDirectory( Path path ) throws IOException {
        Files.walkFileTree( path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile( Path file, BasicFileAttributes attrs ) throws IOException {
                Files.delete( file );
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory( Path dir, IOException e ) throws IOException {
                if( e == null ) {
                    Files.delete( dir );
                    return FileVisitResult.CONTINUE;
                }
                else
                    throw e;
            }
        } );
    }
    
    
    /**
        A context manager for managing a temporary file that will be moved
        to a non-temporary location if no exceptions are raised in the context.
    
        Parameters
        ----------
        final_path : str
            The location to move the file when committing.
     *args, **kwargs
            Forwarded to NamedTemporaryFile.
    
        Notes
        -----
        The file is moved on __exit__ if there are no exceptions.
        ``working_file`` uses :func:`shutil.copyfile` to move the actual files,
        meaning it has as strong of guarantees as :func:`shutil.copyfile`.
     */
    public static class WorkingFile {
        
        private final Path tmpfile;
        private final Path finalPath;

        public WorkingFile( Path finalPath, String prefix, String suffix ) throws IOException {
            this.tmpfile = Files.createTempFile( prefix, suffix );
            this.finalPath = finalPath;
        }
    
        public Path path() {
            /*Alias for ``name`` to be consistent with
            :class:`~zipline.utils.cache.working_dir`.
            */
            return tmpfile;
        }
    
//            Sync the temporary file to the final path.
        public void commit() throws IOException {
            Files.copy( tmpfile, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES );
        }
    
//        def __getattr__(self, attr):
//            return getattr(self._tmpfile, attr)
    
//        def __enter__(self):
//            self._tmpfile.__enter__()
//            return self
    
//        def __exit__(self, *exc_info):
//            if exc_info[0] is None:
//                self._commit()
//            self._tmpfile.__exit__(*exc_info)
    }
    
    
    /*
        A context manager for managing a temporary directory that will be moved
        to a non-temporary location if no exceptions are raised in the context.
    
        Parameters
        ----------
        final_path : str
            The location to move the file when committing.
     *args, **kwargs
            Forwarded to tmp_dir.
    
        Notes
        -----
        The file is moved on __exit__ if there are no exceptions.
        ``working_dir`` uses :func:`shutil.copytree` to move the actual files,
        meaning it has as strong of guarantees as :func:`shutil.copytree`.
     */
    public static class WorkingDir implements Closeable {
        public final Path path;
        private final Path finalPath;
        
        public WorkingDir( Path path ) throws IOException {
            this( path, null );
        }
        
        public WorkingDir( Path finalPath, String prefix ) throws IOException {
            this.path = Files.createTempDirectory( prefix );
            this.finalPath = finalPath;
        }

        /*
            Create a subdirectory of the working directory.
    
            Parameters
            ----------
            path_parts : iterable[str]
                The parts of the path after the working directory.
         */
        public Path mkdir( Path... pathParts ) throws IOException {
            final Path path = getPath( pathParts );
            Files.createDirectories( path );
            return path;
        }
    
        /*
            Get a path relative to the working directory.
    
            Parameters
            ----------
            path_parts : iterable[str]
                The parts of the path after the working directory.
         */
        private Path getPath( Path... pathParts ) {
            return Arrays.stream( pathParts ).reduce( path, (d,f) -> d.resolve( f ) );
        }
        
        public void commit() throws IOException {
//            Sync the temporary directory to the final path.
            final EnumSet<FileVisitOption> opts = EnumSet.of( FileVisitOption.FOLLOW_LINKS );
            final TreeCopier tc = new TreeCopier( path, finalPath );
            Files.walkFileTree( path, opts, Integer.MAX_VALUE, tc );
        }

        @Override
        public void close() throws IOException {
            deleteDirectory( path );
        }
    }
    
    /**
     * A {@code FileVisitor} that copies a file-tree ("cp -r")
     */
    private static class TreeCopier implements FileVisitor<Path> {
        private static final CopyOption[] STANDARD_COPY_OPTIONS = new CopyOption[] { COPY_ATTRIBUTES, REPLACE_EXISTING };
        
        private final Path source;
        private final Path target;

        TreeCopier( Path source, Path target ) {
            this.source = source;
            this.target = target;
        }

        @Override
        public FileVisitResult preVisitDirectory( Path dir, BasicFileAttributes attrs ) {
            // before visiting entries in a directory we copy the directory
            // (okay if directory already exists).

            Path newdir = target.resolve( source.relativize( dir ) );
            try {
                Files.copy( dir, newdir, COPY_ATTRIBUTES );
            }
            catch( FileAlreadyExistsException x ) {
                // ignore
            }
            catch( IOException x ) {
                System.err.format( "Unable to create: %s: %s%n", newdir, x );
                return SKIP_SUBTREE;
            }
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFile( Path file, BasicFileAttributes attrs ) {
            copyFile( file, target.resolve( source.relativize( file ) ) );
            return CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory( Path dir, IOException exc ) {
            // fix up modification time of directory when done
            if( exc == null ) {
                Path newdir = target.resolve( source.relativize( dir ) );
                try {
                    FileTime time = Files.getLastModifiedTime( dir );
                    Files.setLastModifiedTime( newdir, time );
                }
                catch( IOException x ) {
                    System.err.format( "Unable to copy all attributes to: %s: %s%n", newdir, x );
                }
            }
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed( Path file, IOException exc ) {
            if( exc instanceof FileSystemLoopException )
                System.err.println( "cycle detected: " + file );
            else
                System.err.format( "Unable to copy: %s: %s%n", file, exc );
            return CONTINUE;
        }
        
        private void copyFile( Path source, Path target ) {
            if( Files.notExists( target ) ) {
                try {
                    Files.copy( source, target, STANDARD_COPY_OPTIONS );
                }
                catch( IOException x ) {
                    System.err.format( "Unable to copy: %s: %s%n", source, x );
                }
            }
        }
    }
}

/* --------------------------------------------------------------*/

/*
"""
Caching utilities for zipline
"""
from collections import namedtuple, MutableMapping
import errno
import os
import pickle
from shutil import rmtree, copyfile, copytree
from tempfile import mkdtemp, NamedTemporaryFile

import pandas as pd

from .context_tricks import nop_context
from .paths import ensure_directory


class Expired(Exception):
    """Marks that a :class:`CachedObject` has expired.
    """


class CachedObject(namedtuple("_CachedObject", "value expires")):
    """
    A simple struct for maintaining a cached object with an expiration date.

    Parameters
    ----------
    value : object
        The object to cache.
    expires : datetime-like
        Expiration date of `value`. The cache is considered invalid for dates
        **strictly greater** than `expires`.

    Usage
    -----
    >>> from pandas import Timestamp, Timedelta
    >>> expires = Timestamp('2014', tz='UTC')
    >>> obj = CachedObject(1, expires)
    >>> obj.unwrap(expires - Timedelta('1 minute'))
    1
    >>> obj.unwrap(expires)
    1
    >>> obj.unwrap(expires + Timedelta('1 minute'))
    Traceback (most recent call last):
        ...
    Expired: 2014-01-01 00:00:00+00:00
    """

    def unwrap(self, dt):
        """
        Get the cached value.

        Returns
        -------
        value : object
            The cached value.

        Raises
        ------
        Expired
            Raised when `dt` is greater than self.expires.
        """
        if dt > self.expires:
            raise Expired(self.expires)
        return self.value


class ExpiringCache(object):
    """
    A cache of multiple CachedObjects, which returns the wrapped the value
    or raises and deletes the CachedObject if the value has expired.

    Parameters
    ----------
    cache : dict-like, optional
        An instance of a dict-like object which needs to support at least:
        `__del__`, `__getitem__`, `__setitem__`
        If `None`, than a dict is used as a default.

    Usage
    -----
    >>> from pandas import Timestamp, Timedelta
    >>> expires = Timestamp('2014', tz='UTC')
    >>> value = 1
    >>> cache = ExpiringCache()
    >>> cache.set('foo', value, expires)
    >>> cache.get('foo', expires - Timedelta('1 minute'))
    1
    >>> cache.get('foo', expires + Timedelta('1 minute'))
    Traceback (most recent call last):
        ...
    KeyError: 'foo'
    """

    def __init__(self, cache=None):
        if cache is not None:
            self._cache = cache
        else:
            self._cache = {}

    def get(self, key, dt):
        """Get the value of a cached object.

        Parameters
        ----------
        key : any
            The key to lookup.
        dt : datetime
            The time of the lookup.

        Returns
        -------
        result : any
            The value for ``key``.

        Raises
        ------
        KeyError
            Raised if the key is not in the cache or the value for the key
            has expired.
        """
        try:
            return self._cache[key].unwrap(dt)
        except Expired:
            del self._cache[key]
            raise KeyError(key)

    def set(self, key, value, expiration_dt):
        """Adds a new key value pair to the cache.

        Parameters
        ----------
        key : any
            The key to use for the pair.
        value : any
            The value to store under the name ``key``.
        expiration_dt : datetime
            When should this mapping expire? The cache is considered invalid
            for dates **strictly greater** than ``expiration_dt``.
        """
        self._cache[key] = CachedObject(value, expiration_dt)


class dataframe_cache(MutableMapping):
    """A disk-backed cache for dataframes.

    ``dataframe_cache`` is a mutable mapping from string names to pandas
    DataFrame objects.
    This object may be used as a context manager to delete the cache directory
    on exit.

    Parameters
    ----------
    path : str, optional
        The directory path to the cache. Files will be written as
        ``path/<keyname>``.
    lock : Lock, optional
        Thread lock for multithreaded/multiprocessed access to the cache.
        If not provided no locking will be used.
    clean_on_failure : bool, optional
        Should the directory be cleaned up if an exception is raised in the
        context manager.
    serialize : {'msgpack', 'pickle:<n>'}, optional
        How should the data be serialized. If ``'pickle'`` is passed, an
        optional pickle protocol can be passed like: ``'pickle:3'`` which says
        to use pickle protocol 3.

    Notes
    -----
    The syntax ``cache[:]`` will load all key:value pairs into memory as a
    dictionary.
    The cache uses a temporary file format that is subject to change between
    versions of zipline.
    """
    def __init__(self,
                 path=None,
                 lock=None,
                 clean_on_failure=True,
                 serialization='msgpack'):
        self.path = path if path is not None else mkdtemp()
        self.lock = lock if lock is not None else nop_context
        self.clean_on_failure = clean_on_failure

        if serialization == 'msgpack':
            self.serialize = pd.DataFrame.to_msgpack
            self.deserialize = pd.read_msgpack
            self._protocol = None
        else:
            s = serialization.split(':', 1)
            if s[0] != 'pickle':
                raise ValueError(
                    "'serialization' must be either 'msgpack' or 'pickle[:n]'",
                )
            self._protocol = int(s[1]) if len(s) == 2 else None

            self.serialize = self._serialize_pickle
            self.deserialize = pickle.load

        ensure_directory(self.path)

    def _serialize_pickle(self, df, path):
        with open(path, 'wb') as f:
            pickle.dump(df, f, protocol=self._protocol)

    def _keypath(self, key):
        return os.path.join(self.path, key)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, tb):
        if not (self.clean_on_failure or value is None):
            # we are not cleaning up after a failure and there was an exception
            return

        with self.lock:
            rmtree(self.path)

    def __getitem__(self, key):
        if key == slice(None):
            return dict(self.items())

        with self.lock:
            try:
                with open(self._keypath(key), 'rb') as f:
                    return self.deserialize(f)
            except IOError as e:
                if e.errno != errno.ENOENT:
                    raise
                raise KeyError(key)

    def __setitem__(self, key, value):
        with self.lock:
            self.serialize(value, self._keypath(key))

    def __delitem__(self, key):
        with self.lock:
            try:
                os.remove(self._keypath(key))
            except OSError as e:
                if e.errno == errno.ENOENT:
                    # raise a keyerror if this directory did not exist
                    raise KeyError(key)
                # reraise the actual oserror otherwise
                raise

    def __iter__(self):
        return iter(os.listdir(self.path))

    def __len__(self):
        return len(os.listdir(self.path))

    def __repr__(self):
        return '<%s: keys={%s}>' % (
            type(self).__name__,
            ', '.join(map(repr, sorted(self))),
        )


class working_file(object):
    """A context manager for managing a temporary file that will be moved
    to a non-temporary location if no exceptions are raised in the context.

    Parameters
    ----------
    final_path : str
        The location to move the file when committing.
    *args, **kwargs
        Forwarded to NamedTemporaryFile.

    Notes
    -----
    The file is moved on __exit__ if there are no exceptions.
    ``working_file`` uses :func:`shutil.copyfile` to move the actual files,
    meaning it has as strong of guarantees as :func:`shutil.copyfile`.
    """
    def __init__(self, final_path, *args, **kwargs):
        self._tmpfile = NamedTemporaryFile(*args, **kwargs)
        self._final_path = final_path

    @property
    def path(self):
        """Alias for ``name`` to be consistent with
        :class:`~zipline.utils.cache.working_dir`.
        """
        return self._tmpfile.name

    def _commit(self):
        """Sync the temporary file to the final path.
        """
        copyfile(self.name, self._final_path)

    def __getattr__(self, attr):
        return getattr(self._tmpfile, attr)

    def __enter__(self):
        self._tmpfile.__enter__()
        return self

    def __exit__(self, *exc_info):
        if exc_info[0] is None:
            self._commit()
        self._tmpfile.__exit__(*exc_info)


class working_dir(object):
    """A context manager for managing a temporary directory that will be moved
    to a non-temporary location if no exceptions are raised in the context.

    Parameters
    ----------
    final_path : str
        The location to move the file when committing.
    *args, **kwargs
        Forwarded to tmp_dir.

    Notes
    -----
    The file is moved on __exit__ if there are no exceptions.
    ``working_dir`` uses :func:`shutil.copytree` to move the actual files,
    meaning it has as strong of guarantees as :func:`shutil.copytree`.
    """
    def __init__(self, final_path, *args, **kwargs):
        self.path = mkdtemp()
        self._final_path = final_path

    def mkdir(self, *path_parts):
        """Create a subdirectory of the working directory.

        Parameters
        ----------
        path_parts : iterable[str]
            The parts of the path after the working directory.
        """
        path = self.getpath(*path_parts)
        os.mkdir(path)
        return path

    def getpath(self, *path_parts):
        """Get a path relative to the working directory.

        Parameters
        ----------
        path_parts : iterable[str]
            The parts of the path after the working directory.
        """
        return os.path.join(self.path, *path_parts)

    def _commit(self):
        """Sync the temporary directory to the final path.
        """
        copytree(self.path, self._final_path)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if exc_info[0] is None:
            self._commit()
        rmtree(self.path)
        """
        Caching utilities for zipline
        """
        from collections import namedtuple, MutableMapping
        import errno
        import os
        import pickle
        from shutil import rmtree, copyfile, copytree
        from tempfile import mkdtemp, NamedTemporaryFile

        import pandas as pd

        from .context_tricks import nop_context
        from .paths import ensure_directory
*/