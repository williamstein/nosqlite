"""
NoSQLite is a lightweight zeroconf noSQL document-oriented forking
Python SQLite networked authenticated XMLRPC database server.

AUTHOR: (c) William Stein, 2011

LICENSE: Modified BSD

TEST SUITE:

   To run this module's doctest suite, type::
   
             python nosqlite.py
             
   All doctest examples assume that the following line was executed::
   
          >>> from nosqlite import client, server
"""

import os
import re
import shutil
import tempfile

# Database
import sqlite3

# Object serialization
import cPickle
import base64
import zlib

# Simple forking XMLRPC server
import xmlrpclib
import SocketServer
import socket
from SimpleXMLRPCServer import (SimpleXMLRPCServer, SimpleXMLRPCRequestHandler)

# I also develop the Sage (http://sagemath.org) library, so personally
# find having automatic support for Sage Integers and RealNumbers to
# be very handy.  This will get ignored if you don't have Sage
# installed.
try:
    from sage.rings.all import is_Integer, is_RealNumber
except:
    is_Integer = lambda x: False
    is_RealNumber = lambda x: False


###########################################################################
# Server:
#
#   VerifyingServer -- a simple authenticated forking XMLRPC server.
#       * authenticated -- so login/password is supported
#       * forking -- so we can handle many simultaneous connections 
###########################################################################

# http://code.activestate.com/recipes/81549-a-simple-xml-rpc-server/
# See http://www.acooke.org/cute/BasicHTTPA0.html for this recipe.
class VerifyingServer(SocketServer.ForkingMixIn,
                      SimpleXMLRPCServer):
    def __init__(self, username, password, *args, **kargs):
        self.username = username
        self.password = password
        # we use an inner class so that we can call out to the
        # authenticate method
        class VerifyingRequestHandler(SimpleXMLRPCRequestHandler):
            # this is the method we must override
            def parse_request(myself):
                # first, call the original implementation which returns
                # True if all OK so far
                if SimpleXMLRPCRequestHandler.parse_request(myself):
                    # next we authenticate
                    if self.authenticate(myself.headers):
                        return True
                    else:
                        # if authentication fails, tell the client
                        myself.send_error(401, 'Authentication failed')
                return False
        # and intialise the superclass with the above
        SimpleXMLRPCServer.__init__(self,
                                    requestHandler=VerifyingRequestHandler,
                                    logRequests=False,
                                    *args, **kargs)

    def authenticate(self, headers):
        (basic, _, encoded) = \
                headers.get('Authorization').partition(' ')
        assert basic == 'Basic', 'Only basic authentication supported'
        (username, _, password) = base64.b64decode(encoded).partition(':')
        return username == self.username and password == self.password

class Server(object):
    """
    The noSQLite server object.  Create an instance of this object to
    start a server.

    If s is a server() instance, it is also useful to type s.help() to
    see directions about how to setup an ssh tunnel in order to
    securely connect to the server over the network.

        >>> s = server(); s
        nosqlite server on port ...
        >>> s.quit()
        >>> s
        nosqlite server object (not running)
    """
    _test_mode = False
    def __init__(self,
                 username='username', password='password',
                 directory='nosqlite_db',
                 address="localhost", port=8100,
                 auto_run = True):
        """
        INPUTS:
        - username -- string (default: 'username')
        - password -- string (default: 'password'); change this!
        - directory -- string (default: 'nosqlite_db')
        - address -- string (default: 'localhost'); the address that
          the server listens on.
        - auto_run -- bool (default: True); if True, start the server
          upon creation of the Server object.
        """
        # check for a common mistake
        if 'http://' in username or 'http://' in password or 'http://' in address \
           or 'http://' in directory:
            raise ValueError, 'input contains "http://": please read the documentation'
        self.pid = 0
        self.test = self.__class__._test_mode
        if self.test:
            directory = tempfile.mkdtemp()
        self.directory = str(directory)
        self.username = username
        self.password = password
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.address = str(address)
        self.port = int(port)
        self._dbs = {}
        if auto_run:
            self._run()

    def __del__(self):
        try:
            self.quit()
        finally:
            if hasattr(self, 'test') and self.test:
                shutil.rmtree(self.directory, ignore_errors=True)

    def db(self, file):
        """
        Return sqlite connection to database with given filename in self.directory.
        
        EXAMPLES::

            >>> s = server()
            >>> import os
            >>> con = s.db(os.path.join(s.directory, 'bar')); con
            <sqlite3.Connection object at 0x...>
            >>> list(con.cursor().execute('PRAGMA database_list'))
            [(0, u'main', u'/.../bar')]
        """
        try:
            return self._dbs[file]
        except KeyError:
            db = sqlite3.connect(file)
            self._dbs[file] = db
            return db

    def quit(self):
        """
        Terminate the server, which is by default running in the background.

        EXAMPLES::
        
            >>> s = server(); s
            nosqlite server on port ...
            >>> s.pid != 0
            True
            >>> s.quit()
            >>> s.pid
            0
            >>> s
            nosqlite server object (not running)
            >>> port = s._run(); port != 0
            True
            >>> s
            nosqlite server on port ...
        """
        if hasattr(self, 'pid') and self.pid:
            os.kill(self.pid, 9)
            self.pid = 0

    def _run(self, max_tries=1000):
        """
        Run the server.

        By default, this function gets called when you first create
        the server object (use auto_run=False to stop that).  It
        attempts to run the server listening at self.port, and if that
        fails tries the next port, etc. up to max_tries times.  The
        server itself is run in a separate background process.  To
        kill the server, use self.quit().

        INPUT:
        - max_tries -- int (default: 1000); maximum number of ports to try
        
        OUTPUT:
        - the port on which the server is running; also self.port is set

        EXAMPLES::

            >>> s = server(auto_run=False); s
            nosqlite server object (not running)
            >>> port = s._run()
            >>> port != 0
            True
        """
        port = self.port
        success = False
        for i in range(max_tries):
            try:
                server = VerifyingServer(
                    self.username, self.password,
                    (self.address, port), allow_none=True)
                success = True
                break
            except socket.error:
                port += 1
                
        if not success:
            raise RuntimeError("Unable to find an open port.")

        self.port = port

        pid = os.fork()
        if pid != 0:
            self.pid = pid
            self.port = port
            return port
        
        def execute(cmds, t, file='default', many=False):
            db = self.db(os.path.join(self.directory, file) if file != ':memory:' else file)
            cursor = db.cursor()
            if isinstance(cmds, str):
                if t is not None:
                    cmds = [(cmds, t)]
                else:
                    cmds = [cmds]
            v = []
            for c in cmds:
                try:
                    if isinstance(c, tuple):
                        o = cursor.executemany(*c) if many else cursor.execute(*c)
                    else:
                        o = cursor.execute(c)
                except sqlite3.OperationalError, e:
                    raise RuntimeError("%s" % e)
                v.extend(list(o))
            db.commit()
            return v

        server.register_function(execute, 'execute')
        server.serve_forever()

    def help(self):
        """
        Display a help message about this server, including
        instructions on how to connect to it.  This also explains how
        to setup an ssh tunnel in order to securely over a network.

        EXAMPLES::

            >>> s = server()
            >>> s.help()
            ----------------------------------------------------------------------
            nosqlite server on port ...
            Connect with
            ...
            ----------------------------------------------------------------------
        """
        fqdn = socket.getfqdn()
        print("-"*70)
        print(self)
        s = "Connect with\n\n\tclient(%s, '%s', 'xxx'"%(self.port, self.username)
        if self.address != 'localhost':
            s += ", '%s')"%self.address
        else:
            s += ")"
        print s
        print("")
        if self.address == 'localhost':
            print("To securely connect from a remote client, setup an ssh tunnel by")
            print("typing on the client:\n")
            print("\tssh -L %s:localhost:%s %s"%(self.port, self.port, fqdn))
            print("\nthen\n")
            print("\tclient(%s, '%s', 'xxx')"%(self.port, self.username))
        print("\nTo stop server, delete the server object, call the quit()")
        print("method, or kill pid %s."%os.getpid())
        print("-"*70)
        

    def __repr__(self):
        """
        Return string representation of the server.

        EXAMPLES::
        
            >>> server().__repr__()
            'nosqlite server on port ...'
        """
        if self.pid == 0:
            return "nosqlite server object (not running)"
        s = "nosqlite server on port %s"%self.port
        if self.address != 'localhost':
            s += ' of %s'%self.address

        return s


###########################################################################
# Client -- manages connections to the noSQLite server.
#
#  This is where all of the complicated logic is.
#
###########################################################################

class LocalServer(object):
    def __init__(self, directory):
        self.directory = directory
        self._dbs = {}
        if not os.path.exists(directory):
            os.makedirs(directory)
    
    # TODO: These two functions are in the Server above too -- *refactor* somehow...
    def db(self, file):
        try:
            return self._dbs[file]
        except KeyError:
            db = sqlite3.connect(file)
            self._dbs[file] = db
            return db

    def execute(self, cmds, t, file='default', many=False):
        db = self.db(os.path.join(self.directory, file) if file != ':memory:' else file)
        cursor = db.cursor()
        if isinstance(cmds, str):
            if t is not None:
                cmds = [(cmds, t)]
            else:
                cmds = [cmds]
        v = []
        for c in cmds:
            try:
                if isinstance(c, tuple):
                    o = cursor.executemany(*c) if many else cursor.execute(*c)
                else:
                    o = cursor.execute(c)
            except sqlite3.OperationalError, e:
                raise RuntimeError("%s" % e)
            v.extend(list(o))
        db.commit()
        return v

# see http://www.devpicayune.com/entry/200609191448
socket.setdefaulttimeout(10)  

class Client(object):
    """
    The noSQLite client object.  Create an instance of this object to
    connect to a server.

    If C is a client instance, use C.db_name to create the database
    named db_name.  To create the special in-memory (non-persistent)
    SQLite database, use C.memory.
    
    EXAMPLES::
    
        >>> s = server()
        >>> c = client(s.port)
        >>> c
        nosqlite client connected to port ...

    We illustrate all options::

        >>> c = client(8100, username='foo', password='bar', address='localhost')
        >>> c = client(8100, 'foo', 'bar', 'localhost')
    """
    def __init__(self, port_or_dir=8100, username='username', password='password',
                 address="localhost"):
        """
        INPUTS:
        - port -- int or string (default: 8100); port to connect to or a string that
          instead uses a new local server served out of that directory
        - username -- string (default: 'username')
        - password -- string (default: 'password'); you likely have to
          change this
        - address -- string (default: 'localhost'); name of computer
          to connect to
        """
        # check for a common mistake
        if 'http://' in str(port_or_dir) or 'http://' in username or 'http://' in password or 'http://' in address:
            raise ValueError, 'input contains "http://": please read the documentation'
        
        if isinstance(port_or_dir, str):
            # instead open local databases directory (no client/server).
            self.server = LocalServer(port_or_dir)
        else:
            self.address = str(address)
            self.port = int(port_or_dir)
            self.server = xmlrpclib.Server('http://%s:%s@%s:%s'%
                       (username, password, address, self.port),
                                           allow_none=True)

    def __repr__(self):
        """
        EXAMPLES::

            >>> client(8110, 'mod.math.washington.edu').__repr__()
            'nosqlite client connected to port 8110'
        """
        s = "nosqlite client connected to port %s"%self.port
        if self.address != 'localhost':
            s += ' of %s'%self.address
        return s
        
    def __call__(self, cmd, t=None, file='default', many=False, coerce=True):
        """
        Send a SQL query to the server.

        INPUT:
        - cmd -- string; a single SQL command
        - t -- tuple (default: None) optional arguments that replace
          the ?'s in the cmd (but see 'many' option below).
        - file -- string (default: 'default') the database file on
          which to execute the query
        - many -- bool (default: False); if True, then execute cmd
          with each tuple in t replacing ?.  This is used, e.g., for
          very fast batch inserts.
        - coerce -- bool (default: True); if True, then entries in t
          are coerced to int, bool, float, str, or pickles.

        OUTPUT:
        - list of results of the query

        EXAMPLES::

            >>> s = server(); c = client(s.port)
            >>> c.db.data.insert([{'a':5, 'bc':10}, {'a':3}, {'a':4, 'bc':15}])
            >>> c('SELECT * FROM data WHERE a<?', t=(5,), file='db')
            [[3, None], [4, 15]]
            >>> c('INSERT INTO data VALUES(?,?)', t=[(1,2),(3,8)], file='db', many=True)
            []
            >>> c('SELECT * FROM data', file='db')
            [[3, None], [5, 10], [4, 15], [1, 2], [3, 8]]

        Coercion automatically pickles when the datatype is not int,
        bool, float, or str::

            >>> c('INSERT INTO data VALUES(?,?)', t=[[1,2],[3,4]], file='db', coerce=True)
            []
            >>> c('SELECT * FROM data WHERE a>="__pickle"', file='db')
            [['__pickleeJxr...', '__pickleeJxrY...']]

        If we do not coerce, we just get an error::

            >>> c('INSERT INTO data VALUES(?,?)', t=[[1,2],[3,4]], file='db', coerce=False)
            Traceback (most recent call last):
            ...
            RuntimeError: ...
        """
        if not isinstance(cmd, str):
            raise TypeError("cmd (=%s) must be a string"%cmd)
        if coerce:
            if many:
                t = [tuple([self._coerce_(x) for x in y]) for y in t]
            else:
                if t is not None:
                    t = tuple([self._coerce_(x) for x in t])
        try:
            return self.server.execute(cmd, t, file, many)
        except xmlrpclib.Fault, e:
            raise RuntimeError, str(e) + ', cmd="%s"'%cmd
            
    def __getattr__(self, name):
        """
        Return the database with given name.  If name is 'memory',
        returns the in-memory database.

        INPUT:
        - name -- string

        OUTPUT:
        - Database object

        EXAMPLES::

            >>> s = server(); c = client(s.port)
            >>> c.mydb
            Database 'mydb'

        WARNING: there is a special in-memory only database that you get
        by accessing "memory".    This does not get saved to disk::

            >>> db = c.memory
            >>> db.name
            ':memory:'
            >>> db
            Database ':memory:'
        """
        if name == 'memory':
            name = ':memory:'
        return Database(self, name)

    def _coerce_(self, x):
        """
        EXAMPLES::

            >>> s = server(); c = client(s.port)
            >>> c._coerce_(False)
            0
            >>> c._coerce_(True)
            1
            >>> c._coerce_('lkjdf')
            'lkjdf'
            >>> c._coerce_(2.5)
            2.5
            >>> c._coerce_([1,2])
            '__pickleeJxrYIot...'
        """
        if isinstance(x, bool):
            x = int(x)
        elif isinstance(x, (str, int, long, float)):
            pass
        elif x is None:
            pass
        elif is_Integer(x) and x.nbits()<32:
            x = int(x)
        elif is_RealNumber(x) and x.prec()==53:
            return float(x)
        elif isinstance(x, unicode):
            return str(x)
        else:
            x = '__pickle' + base64.b64encode(zlib.compress(cPickle.dumps(x, 2)))
        return x

    def _coerce_back_(self, x):
        """
        EXAMPLES::

            >>> s = server(); c = client(s.port)
            >>> z = c._coerce_([1,2])
            >>> c._coerce_back_(z)
            [1, 2]
        """
        if isinstance(x, (str, unicode)) and x.startswith('__pickle'):
            return cPickle.loads(zlib.decompress(base64.b64decode(x[8:])))
        return x

class Database(object):
    """
    A nosqlite Database object.  This represents a group of
    collections.
    """
    def __init__(self, client, name):
        """
        EXAMPLES::

            >>> s = server(); c = client(s.port)
            >>> db = c.database; db
            Database 'database'
            >>> type(db)
            <class '__main__.Database'>
            >>> db.client
            nosqlite client connected to port ...
            >>> db.name
            'database'
        """
        self.client = client
        self.name = str(name)

    def vacuum(self):
        """
        Free unused disk space used by this database.  If you delete
        collections and want the corresponding disk spaces to be
        freed, call this function.
        
        EXAMPLES::

            >>> s = server(); db = client(s.port).database
            >>> db.vacuum()
        """
        self('vacuum')

    def __call__(self, cmds, t=None, many=False, coerce=True):
        """
        Send an SQL query to the database server.  The input
        parameters are exactly the same as for the Client object's
        __call__ method, except that the file defaults to self.name.
        
        EXAMPLES::

            >>> s = server(); db = client(s.port).database
            >>> db.coll.insert([{'a':i} for i in range(6)])
            >>> db('select count(*) from coll')
            [[6]]
        """
        return self.client(cmds, t, file=self.name, many=many, coerce=coerce)

    def __getattr__(self, name):
        """
        Returns the collection in this database with the given name.
        
        EXAMPLES::

            >>> s = server(); db = client(s.port).database
            >>> c = db.coll; c
            Collection 'database.coll'
            >>> type(c)
            <class '__main__.Collection'>
            >>> db.__getattr__('coll')
            Collection 'database.coll'
        """
        return Collection(self, name)

    def trait_names(self):
        """
        Used so that we can tab complete in IPython/Sage when
        selecting a collection in this database as an attribute.
        
        EXAMPLES::

            >>> s = server(); db = client(s.port).database
            >>> db.col1.insert({'a':0}); db.my_col2.insert({'a':10})
            >>> db.trait_names()
            ['col1', 'my_col2']
        """
        return [C.name for C in self.collections()]

    def __repr__(self):
        """
        EXAMPLES::

            >>> s = server(); db = client(s.port).database; db.__repr__()
            "Database 'database'"
        """
        return "Database '%s'"%self.name

    def collections(self):
        """
        A list of all of the collections in this database.

        NOTE: This is not a list of the names of collections but of
        the actual collections themselves.
        
        EXAMPLES::

            >>> s = server(); db = client(s.port).database
            >>> db.col1.insert({'a':0}); db.my_col2.insert({'a':10})
            >>> v = db.collections(); v
            [Collection 'database.col1', Collection 'database.my_col2']
            >>> v[0].find_one()
            {'a': 0}
        """
        cmd = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        return [Collection(self, x[0]) for x in self(cmd)]

class Collection(object):
    def __init__(self, database, name):
        """
        INPUTS:
        - database -- a Database object
        - name -- string, name of this collection
        
        EXAMPLES::

            >>> s = server(); db = client(s.port).database
            >>> C = db.mycoll; C
            Collection 'database.mycoll'
            >>> type(C)
            <class '__main__.Collection'>
            >>> C.database
            Database 'database'
            >>> C.name
            'mycoll'
        """
        self.database = database
        self.name = str(name)

    def __call__(self, *args, **kwds):
        return self.database(*args, **kwds)

    def __repr__(self):
        """
        EXAMPLES::

            >>> s = server(); db = client(s.port).database
            >>> C = db.mycoll; C.__repr__()
            "Collection 'database.mycoll'"
        """
        return "Collection '%s.%s'"%(self.database.name, self.name)

    def __len__(self):
        """
        Return the number of documents in this collection.
        
        EXAMPLES::

            >>> s = server(); C = client(s.port).database.mycol
            >>> len(C)
            0
            >>> C.insert([{'a':i} for i in range(100)])
            >>> len(C)
            100
        """
        try:
            cmd = 'SELECT COUNT(*) FROM "%s"'%self.name
            return int(self.database(cmd)[0][0])
        except RuntimeError:
            if len(self._columns()) == 0:
                return 0
            raise

    def _validate_column_names(self, columns):
        """
        Raise a ValueError exception if a given column name is invalid.  A column
        name is invalid only if it contains a double quote.
        
        EXAMPLES::

        We illustrate that crazy column names are just fine::

            >>> s = server(); C = client(s.port).database.mycol
            >>> C.insert({"'":393})
            >>> C.find_one()
            {"'": 393}
            >>> C._validate_column_names("'")
            >>> C._validate_column_names("h5 2")
            >>> C.insert({"h5 2":3931})
            >>> list(C.find())
            [{"'": 393}, {'h5 2': 3931}]

        But a column name with a double quote is a problem.
            >>> C._validate_column_names('"')
            Traceback (most recent call last):
            ...
            ValueError: column name '"' must not contain a quote
        """
        for c in columns:
            if '"' in c:
                raise ValueError, "column name '%s' must not contain a quote"%c

    def _create(self, columns):
        """
        Create this table for the first time with the given columns.

        INPUT:
        - columns -- a nonempty list of strings
        
        EXAMPLES::

            >>> s = server(); C = client(s.port).database.mycol
            >>> C._create(['a', 'b', 'c'])
            >>> C.columns()
            ['a', 'b', 'c']
        """
        self._validate_column_names(columns)
        self.database('CREATE TABLE IF NOT EXISTS "%s" (%s)'%(self.name, ', '.join('"%s"'%s for s in columns)))
        
    ###############################################################
    # Inserting documents: one at a time or in a batch
    ###############################################################
    def insert(self, d=None, coerce=True, on_conflict=None, **kwds):
        """
        Insert a document or list of documents into this collection.
        
        INPUT:
        - d -- dict (single document) or list of dict's
        - coerce -- bool (default: True); if True, coerce values
        - on_conflict -- string (default: None); if given should be one of
          'rollback', 'abort', 'fail', 'ignore', 'replace'
          (see http://www.sqlite.org/lang_conflict.html).
        - kwds -- gets merged into d, providing a convenient shorthand
          for inserting a document.

        EXAMPLES::

            >>> s = server(); C = client(s.port).database.C

        Insert a document defined by a dictionary::
        
            >>> C.insert({'a':5, 'xyz':10})
            >>> list(C.find())
            [{'a': 5, 'xyz': 10}]

        You can also insert a document by using var=value inputs::
        
            >>> C.insert(a=10, xyz='hi', m=[1,2])
            >>> list(C.find())
            [{'a': 5, 'xyz': 10}, {'a': 10, 'xyz': 'hi', 'm': [1, 2]}]

        By giving a list of dictionaries, you can batch insert
        serveral documents::
        
            >>> C.insert([{'a':2}, {'a':7}, dict(a=5,b=10)])
            >>> list(C.find())
            [{'a': 5, 'xyz': 10}, {'a': 10, 'xyz': 'hi', 'm': [1, 2]}, {'a': 5, 'b': 10}, {'a': 2}, {'a': 7}]

        Inserting a list of documents is dramatically faster than
        calling insert repeatedly.  For example, the following insert
        of 10,000 distinct documents should take less than a second::

            >>> C.insert([{'a':i} for i in range(10000)])
            >>> len(C)
            10005

        Inserting a list of documents works even if the documents have
        different keys::

            >>> C.insert([{'a':5, 'b':10, 'x':15}, {'x':20, 'y':30}])
            >>> C.find_one(x=15)
            {'a': 5, 'x': 15, 'b': 10}
            >>> C.find_one(y=30)
            {'y': 30, 'x': 20}
        """
        if d is None:
            d = kwds
        elif isinstance(d, dict):
            d.update(kwds)
        else:
            if len(kwds) > 0:
                raise ValueError, "if kwds given, then d must be None or a dict"

        # Determine the keys of all documents we will be inserting.
        if isinstance(d, list):
            keys = set().union(*d)
        else:
            keys = set(d.keys())
            
        # Make sure that the keys of d are a subset of the columns of
        # the corresponding table.  If not, expand that table by
        # adding a new column, which is one thing we can easily do
        # to change a table in sqlite.
        current_cols = self._columns()
        new_columns = keys.difference(current_cols)
        if len(current_cols) == 0:
            # table doesn't exist yet
            self._create(new_columns)
        else:
            # table exists -- add any new columns to it (usually new_columns is empty)
            self._add_columns(new_columns)

        # Now do the insert -- either batch or individual.
        if isinstance(d, list):
            # batch insert.  Since the keys in the dictionaries in d can vary, we
            # group d into a list of sublists with constant keys.   Then each of
            # these get inserted using SQL's executemany.
            for v in _constant_key_grouping(d):
                cmd = _insert_statement(self.name, v[0].keys(), on_conflict)
                self.database(cmd, [x.values() for x in v], many=True, coerce=coerce)
            
        else:
            # individual insert
            self.database(_insert_statement(self.name, d.keys(), on_conflict), d.values(), coerce=coerce)


    ###############################################################
    # Copy or rename a collection
    ###############################################################
    def rename(self, new_name):
        """
        Rename this collection to the given new name.
        
        INPUT:
        - new_name -- string

        EXAMPLES::

            >>> s = server(); db = client(s.port).database; C = db.C
            >>> C.name
            'C'
            >>> C.insert([{'a':5, 'b':10, 'x':15}, {'x':20, 'y':30}])
            >>> C.rename('collection2')
            >>> C.name
            'collection2'
            >>> C
            Collection 'database.collection2'
            >>> C = db.collection2
            >>> list(C)
            [{'y': 30, 'x': 20}, {'a': 5, 'x': 15, 'b': 10}]
            >>> list(db.C)
            []
        """
        cmd = "ALTER TABLE %s RENAME TO %s"%(self.name, new_name)
        self.database(cmd)
        self.name = new_name
    
    def copy(self, collection, query='', fields=None, **kwds):
        """
        Copy documents from self into the given collection.  The query
        and fields are specified exactly as for the find command.

        INPUT:
        - collection -- a Collection or string (that names a collection).

        EXAMPLES::

            >>> s = server(); db = client(s.port).database; C = db.C
            >>> C.insert([{'a':5, 'b':10, 'x':15}, {'x':20, 'y':30}])
            >>> C.copy('foo')
            >>> list(db.foo)
            [{'y': 30, 'x': 20}, {'a': 5, 'x': 15, 'b': 10}]
        """
        if isinstance(collection, str):
            collection = self.database.__getattr__(collection)
        # which columns we want to copy
        fields = self._columns() if fields is None else fields
        # which are already in other collection
        other = collection._columns()
        # which are missing
        cols = set(fields).difference(other)
        if len(other) == 0:
            # other collection hasn't been created yet
            collection._create(cols)
        elif cols:
            # need to add some columns to other collection
            collection._add_columns(cols)
        # now recipient table has all needed columns, so do the insert in one go.
        c = ','.join(['"%s"'%x for x in fields])
        cmd = 'INSERT INTO "%s" (%s) SELECT %s FROM "%s" %s'%(
            collection.name, c, c, self.name, self._where_clause(query, kwds))
        self.database(cmd)

    ###############################################################
    # Updating documents
    ###############################################################
    def update(self, d, query='', **kwds):
        """
        Set the values specified by the dictionary d for every
        document that satisfy the given query string (or equality
        query defined by kwds).
        
        EXAMPLES::

            >>> s = server(); db = client(s.port).database; C = db.C
            >>> C.insert([{'a!b':5, 'b.c':10, 'x':15}, {'x':15, 'y':30}])
            >>> C.update({'z z':'hello', 'y':20}, x=15)
            >>> list(C)
            [{'y': 20, 'x': 15, 'z z': 'hello'}, {'b.c': 10, 'x': 15, 'z z': 'hello', 'y': 20, 'a!b': 5}]
        """
        new_cols = set(d.keys()).difference(self._columns())
        if new_cols:
            self._add_columns(new_cols)

        t = tuple([self.database.client._coerce_(x) for x in d.values()])
        s = ','.join(['"%s"=? '%x for x in d.keys()])
        cmd = 'UPDATE "%s" SET %s %s'%(
            self.name, s, self._where_clause(query, kwds))
        self.database(cmd, t)
        
    ###############################################################
    # Importing and exporting data in various formats
    ###############################################################
    def export_csv(self, csvfile, delimiter=' ', quotechar='|', order_by=None, write_columns=True):
        """
        Export all documents in self to the given csvfile.  The first row
        of the cvsfile will be headers that specify the keys.

        INPUT:
        - csvfile -- string or readable file
        - delimiter -- string (default: ' ')
        - quotechar -- string (default: '|')
        - order_by -- string (default: None)

        EXAMPLES::

            >>> 
        """
        if isinstance(csvfile, str):
            csvfile = open(csvfile, 'wb')
        import csv
        W = csv.writer(csvfile, delimiter=delimiter, quotechar=quotechar, quoting=csv.QUOTE_MINIMAL)
        cmd = 'SELECT * FROM "%s" '%self.name
        if order_by is not None:
            cmd += ' ORDER BY %s'%order_by
        if write_columns:
            W.writerow(self.columns())
        for x in self.database(cmd):
            W.writerow(['%r'%a for a in x])

    def import_csv(self, csvfile, columns=None, delimiter=' ', quotechar='|'):
        """
        Import data into self from the given csvfile.  If columns is
        None, then the first row of the cvsfile must be headers that
        specify the keys.  If columns is not None, then the first row
        is assumed to be data. 

        INPUT:
        - csvfile -- string or readable file
        - delimiter -- string (default: ' ')
        - quotechar -- string (default: '|')
        - columns -- None or list of strings (column headings)

        EXAMPLES::

            >>>         
        """
        if isinstance(csvfile, str):
            csvfile = open(csvfile, 'rb')
        import csv
        R = csv.reader(csvfile, delimiter=delimiter, quotechar=quotechar)
        if columns is None:
            columns = R.next()
        d = []
        for x in R:
            z = {}
            for i in range(len(x)):
                y = x[i]
                if y != '':
                    if y.isdigit():
                        y = eval(y)
                    else:
                        v = y.split('.')
                        if len(v) == 2 and v[0].isdigit() and v[1].isdigit():
                            y = eval(y)
                    z[columns[i]] = y
            d.append(z)
        self.insert(d)

    ###############################################################
    # Deleting documents
    ###############################################################
    def delete(self, query='', **kwds):
        """
        EXAMPLES::

            >>> 
        """
        if not query and len(kwds) == 0:
            if len(self._columns()) == 0:
                # nothing to do, since table wasn't created yet.
                return
            # just drop the table
            cmd = 'DROP TABLE "%s"'%self.name
        else:
            cmd = 'DELETE FROM "%s" %s'%(self.name, self._where_clause(query, kwds))
        self.database(cmd)

    ###############################################################
    # Indexes: creation, dropping, listing
    ###############################################################
    
    def _index_pattern(self, kwds):
        """
        EXAMPLES::

            >>> 
        """
        cols = ','.join(['%s %s'%(column, 'DESC' if direction < 0 else 'ASC') for
                         column, direction in sorted(kwds.iteritems())])
        index_name = 'idx___%s___%s'%(self.name, cols.replace(',','___').replace(' ',''))
        return cols, index_name

    def ensure_index(self, unique=None, **kwds):
        """
        EXAMPLES::

            >>> 
        """
        if len(kwds) == 0:
            raise ValueError, "must specify some keys"
        cols, index_name = self._index_pattern(kwds)
        current_cols = self.columns()
        new_cols = [c for c in sorted(kwds.keys()) if c not in current_cols]
        if new_cols:
            if not current_cols:
                self._create(new_cols)
            else:
                self._add_columns(new_cols)
                
        cmd = "CREATE %s INDEX IF NOT EXISTS %s ON %s(%s)"%(
            'UNIQUE' if unique else '', index_name, self.name, cols)
        self.database(cmd)

    def drop_index(self, **kwds):
        """
        EXAMPLES::

            >>> 
        """
        cols, index_name = self._index_pattern(kwds)
        cmd = 'DROP INDEX IF EXISTS "%s"'%index_name
        self.database(cmd)

    def drop_indexes(self):
        """
        EXAMPLES::

            >>> 
        """
        cmd = "SELECT * FROM sqlite_master WHERE type='index' and tbl_name='%s'"%self.name
        for x in self.database(cmd):
            if x[1].startswith('idx___'):
                self.database('DROP INDEX IF EXISTS "%s"'%x[1])

    def indexes(self):
        """
        EXAMPLES::

            >>> 
        """
        cmd = "SELECT * FROM sqlite_master WHERE type='index' and tbl_name='%s' ORDER BY name"%self.name
        v = []
        for x in self.database(cmd):
            d = {}
            for a in x[1].split('___')[2:]:
                if a.endswith('ASC'):
                    d[a[:-3]] = 1
                else:
                    d[a[:-4]] = -1
            v.append(d)
        return v


    ###############################################################
    # Finding: queries
    ###############################################################

    def _columns(self):
        """
        EXAMPLES::

            >>> 
        """
        a = self.database('PRAGMA table_info("%s")'%self.name)
        if a is None:
            return []
        return [x[1] for x in a]

    def columns(self):
        """
        EXAMPLES::

            >>> 
        """
        return [x for x in self._columns() if x != 'rowid']

    def _add_columns(self, new_columns):
        """
        EXAMPLES::

            >>> 
        """
        self._validate_column_names(new_columns)        
        for col in new_columns:
            try:
                self.database('ALTER TABLE "%s" ADD COLUMN "%s"'%(self.name, col))
            except xmlrpclib.Fault:
                # TODO: make it into a single transaction...
                # The above could safely fail if another client tried
                # to add at the same time and made the relevant
                # column. Ignore error here and deal with it later.
                pass

    def find_one(self, *args, **kwds):
        """
        Return first document that match the given query.

        EXAMPLES::

            >>> 
        """
        v = list(self.find(*args, limit=1, **kwds))
        if len(v) == 0:
            raise ValueError, "found nothing"
        return v[0]
        
    def _where_clause(self, query, kwds):
        """
        EXAMPLES::

            >>> 
        """
        if len(kwds) > 0:
            for key, val in kwds.iteritems():
                val = self.database.client._coerce_(val)
                
                if query:
                    query += ' AND %s=%r '%(key, val)
                else:
                    query = ' %s=%r '%(key, val)
        return ' WHERE ' + query if query else ''

    def _find_cmd(self, query='', fields=None, limit=None, offset=0,
                  order_by=None, batch_size=50, _rowid=False, _count=False, **kwds):
        """
        EXAMPLES::

            >>> 
        """        
        cmd = 'SELECT rowid,' if _rowid else 'SELECT '
        if fields is None:
            cmd += 'COUNT(*) ' if _count else ' * '
            cmd += ' FROM "%s"'%self.name
        else:
            if isinstance(fields, str):
                fields = [fields]
            cmd += '%s FROM "%s"'%(','.join(fields), self.name)

        cmd += self._where_clause(query, kwds)

        if order_by is not None:
            cmd += ' ORDER BY %s '%order_by

        batch_size = int(batch_size)
        
        if limit is not None:
            cmd += ' LIMIT %s'%int(limit)
        else:
            cmd += ' LIMIT %s'%batch_size
        if offset is not None:
            cmd += ' OFFSET %s'%int(offset)

        return cmd

    def count(self, *args, **kwds):
        """
        Return the number of documents that match a given find query.

        EXAMPLES::

            >>>                 
        """
        kwds['_count'] = True
        cmd = self._find_cmd(*args, **kwds)
        return self.database(cmd)[0]

    def __iter__(self):
        """
        EXAMPLES::

            >>> 
        """        
        return self.find()

    def find(self, query='', fields=None, batch_size=50,
             order_by=None, _rowid=False, limit=None, offset=0, **kwds):
        """
        Return iterator over all documents that match the given query.


        EXAMPLES::

            >>>         
        """
        cmd = self._find_cmd(query=query, fields=fields, batch_size=batch_size,
                             _rowid=_rowid, order_by=order_by,
                             limit=limit, offset=offset, **kwds)
        convert = self.database.client._coerce_back_
        while True:
            cols = self._columns()
            if len(cols) == 0:  # table not yet created
                return
            v = self.database(cmd)
            if fields is None:
                columns = cols
            else:
                columns = fields
            columns = (['rowid'] if _rowid else []) + columns
            for x in v:
                yield dict([a for a in zip(columns, [convert(y) for y in x])
                            if a[1] is not None])
            if limit is not None or len(v) == 0:
                return
            i = cmd.rfind('OFFSET')
            offset += batch_size
            cmd = cmd[:i] + 'OFFSET %s'%offset
        

def _insert_statement(table, cols, on_conflict=None):
    """
    Return SQLite INSERT statement template for inserting the columns
    into the given table.
    
    INPUT:
    - table -- name of a SQLite table
    - cols -- list of strings

    EXAMPLES::

        >>> from nosqlite import _insert_statement
        >>> _insert_statement('table_name', ['col1', 'col2', 'col3'])
        'INSERT  INTO "table_name" ("col1","col2","col3") VALUES(?,?,?)'
    """
    conflict = 'OR %s'%on_conflict if on_conflict else ''
    cols = ['"%s"'%c for c in cols]
    return 'INSERT %s INTO "%s" (%s) VALUES(%s)'%(conflict, table, ','.join(cols), ','.join(['?']*len(cols)))

def _constant_key_grouping(d):
    """
    Group the list d into a list of sublists with constant keys.
    
    INPUT:
    - d -- a list of dictionaries
    OUTPUT:
    - a list of lists of dictionaries with constant keys

    EXAMPLES::

        >>> from nosqlite import _constant_key_grouping
        >>> _constant_key_grouping([{'a':5,'b':7}, {'a':10,'c':4}, {'a':5, 'b':8}])
        [[{'a': 5, 'b': 7}, {'a': 5, 'b': 8}], [{'a': 10, 'c': 4}]]
    """
    x = {}
    for a in d:
        k = tuple(a.keys())
        if x.has_key(k):
            x[k].append(a)
        else:
            x[k] = [a]
    return x.values()

# Easier usage
server = Server
client = Client

# Doctesting
if __name__ == '__main__':
    import doctest
    class TestServer(Server):
        _test_mode = True
    doctest.testmod(optionflags=doctest.ELLIPSIS,
                    extraglobs={'server':TestServer, 'Server':TestServer})
