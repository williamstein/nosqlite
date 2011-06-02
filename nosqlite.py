"""
NoSQLite is a lightweight zeroconf noSQL document-oriented forking
Python SQLite networked authenticated XMLRPC database server.

AUTHOR: (c) William Stein, 2011
LICENSE: Modified BSD
"""

import os

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
except ImportError:
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
    def __init__(self,
                 username='username', password='password',
                 directory='nosqlite.db',
                 address="localhost", port=8100,
                 auto_run=True):
        self.directory = str(directory)
        self.username = username
        self.password = password
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.address = str(address)
        self.port = int(port)
        self._dbs = {}
        if auto_run:
            self.run()

    def db(self, file):
        try:
            return self._dbs[file]
        except KeyError:
            db = sqlite3.connect(file)
            self._dbs[file] = db
            return db

    def run(self, max_tries=100):
        port = self.port
        success = False
        for i in range(max_tries):
            try:
                server = VerifyingServer(
                    self.username, self.password,
                    (self.address, port), allow_none=True)
                success = True
                break
            except socket.error, e:
                print "Port %s is unavailable."%port
                port += 1

        if not success:
            raise RuntimeError, "Unable to find an open port."

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
                if isinstance(c, tuple):
                    o = cursor.executemany(*c) if many else cursor.execute(*c)
                else:
                    o = cursor.execute(c)
                v.extend(list(o))
            db.commit()
            return v

        fqdn = socket.getfqdn()
        print "\n"
        print "-"*70
        print "noSQLite serving '%s/'."%os.path.abspath(self.directory),
        print "Connect with\n\n\tclient(%s, '%s', 'xxx'"%(port, self.username),
        if self.address != 'localhost':
            print ", '%s')"%self.address
        else:
            print ")"
        print ""
        if self.address == 'localhost':
            print "To securely connect from a remote client, setup an ssh tunnel by"
            print "typing on the client:\n"
            print "\tssh -L %s:localhost:%s %s"%(port, port, fqdn)
            print "\nthen\n"
            print "\tclient(%s, '%s', 'xxx')"%(port, self.username)
        print "\nPress control-c to terminate server or kill pid %s."%os.getpid()
        print "-"*70
        server.register_function(execute, 'execute')
        server.serve_forever()
        

    def __repr__(self):
        return "noSQLite server http://%s:%s"%(self.address, self.port)


###########################################################################
# Client -- manages connections to the noSQLite server.
#
#  This is where all of the complicated logic is.
#
###########################################################################

# see http://www.devpicayune.com/entry/200609191448
socket.setdefaulttimeout(10)  

class Client(object):
    def __init__(self, port=8100, username='username', password='password',
                 address="localhost"):
        self.address = str(address)
        self.port = port
        self.server = xmlrpclib.Server('http://%s:%s@%s:%s'%
                   (username, password, address, port),
                                       allow_none=True)

    def __repr__(self):
        return "noSQLite client http://%s:%s"%(self.address, self.port)
        
    def __call__(self, cmd, t=None, file='default', many=False, coerce=True):
        if not isinstance(cmd, str):
            raise TypeError, "cmd (=%s) must be a string"%cmd
        if coerce:
            if many:
                t = [tuple([self._coerce_(x) for x in y]) for y in t]
            else:
                if t is not None:
                    t = tuple([self._coerce_(x) for x in t])
        return self.server.execute(cmd, t, file, many)
    
    def __getattr__(self, name):
        if name == 'memory':
            name = ':memory:'
        return Database(self, name)

    def _coerce_(self, x):
        if isinstance(x, bool):
            x = int(x)
        elif isinstance(x, (str, int, long, float)):
            pass
        elif is_Integer(x) and x.nbits()<32:
            x = int(x)
        elif is_RealNumber(x) and x.prec()==53:
            return float(x)
        else:
            x = '__pickle' + base64.b64encode(zlib.compress(cPickle.dumps(x, 2)))
        return x

    def _coerce_back_(self, x):
        if isinstance(x, str) and x.startswith('__pickle'):
            return cPickle.loads(zlib.decompress(base64.b64decode(x[8:])))
        return x

class Database(object):
    def __init__(self, client, name):
        self.client = client
        self.name = str(name)

    def vacuum(self):
        self('vacuum')

    def __call__(self, cmds, t=None, many=False, coerce=True):
        return self.client(cmds, t, file=self.name, many=many, coerce=coerce)

    def __getattr__(self, name):
        return Collection(self, name)

    def trait_names(self):
        return [C.name for C in self.collections()]

    def __repr__(self):
        return "Database '%s'"%self.name

    def collections(self):
        cmd = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        return [Collection(self, x[0]) for x in self(cmd)]

class Collection(object):
    def __init__(self, database, name):
        self.database = database
        self.name = str(name)

    def __repr__(self):
        return "Collection '%s.%s'"%(self.database.name, self.name)

    def __len__(self):
        try:
            cmd = "SELECT COUNT(*) FROM %s"%self.name
            return int(self.database(cmd)[0][0])
        except xmlrpclib.Fault:
            if len(self._columns()) == 0:
                return 0
            raise

    def _create(self, columns):
        self.database("CREATE TABLE %s (%s)"%(self.name, ', '.join(columns)))
        
    ###############################################################
    # Inserting documents: one at a time or in a batch
    ###############################################################
    def insert(self, d=None, coerce=True, **kwds):
        """
        INPUT:
        - d -- dict (single document) or list of dict's
        - coerce -- whether to coerce values
        - kwds -- gets merged into d, providing a convenient shorthand
          for inserting a document.
        """
        if d is None:
            d = kwds
        elif isinstance(d, dict):
            d.update(kwds)
        else:
            raise ValueError, "if kwds given, then d must be None or a dict"

        # Make sure that the keys of d are a subset of the columns of
        # the corresponding table.  If not, expand that table by
        # adding a new column, which is one thing we can easily do
        # to change a table in sqlite.

        if isinstance(d, list):
            keys = set().union(*d)
        else:
            keys = set(d.keys())

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
                cmd = _insert_statement(self.name, v[0].keys())
                self.database(cmd, [x.values() for x in v], many=True, coerce=coerce)
            
        else:
            # individual insert
            self.database(_insert_statement(self.name, d.keys()), d.values(), coerce=coerce)


    ###############################################################
    # Copy or rename a collection
    ###############################################################
    def rename(self, new_name):
        """
        Rename this collection to the given new name.
        
        INPUT:
        - new_name -- string
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
        if cols:
            # need to add some columns to other collection
            collection._add_columns(cols)
        # now recipient table has all needed columns, so do the insert in one go.
        c = ','.join(fields)
        cmd = "INSERT INTO %s (%s) SELECT %s FROM %s %s"%(
            collection.name, c, c, self.name, self._where_clause(query, kwds))
        self.database(cmd)

    ###############################################################
    # Updating documents
    ###############################################################
    def update(self, d, query='', **kwds):
        t = tuple([self.database.client._coerce_(x) for x in d.values()])
        s = ','.join(['%s=? '%x for x in d.keys()])
        cmd = "UPDATE %s SET %s %s"%(
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
        """
        if isinstance(csvfile, str):
            csvfile = open(csvfile, 'wb')
        import csv
        W = csv.writer(csvfile, delimiter=delimiter, quotechar=quotechar, quoting=csv.QUOTE_MINIMAL)
        cmd = 'SELECT * FROM %s '%self.name
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
        if not query and len(kwds) == 0:
            # just drop the table
            cmd = "DROP TABLE %s"%self.name
        else:
            cmd = "DELETE FROM %s %s"%(self.name, self._where_clause(query, kwds))
        self.database(cmd)

    ###############################################################
    # Indexes: creation, dropping, listing
    ###############################################################
    
    def _index_pattern(self, kwds):
        cols = ','.join(['%s %s'%(column, 'DESC' if direction < 0 else 'ASC') for
                         column, direction in sorted(kwds.iteritems())])
        index_name = 'idx___%s___%s'%(self.name, cols.replace(',','___').replace(' ',''))
        return cols, index_name

    def ensure_index(self, unique=None, **kwds):
        if len(kwds) == 0:
            raise ValueError, "must specify some keys"
        cols, index_name = self._index_pattern(kwds)
        cmd = "CREATE %s INDEX IF NOT EXISTS %s ON %s(%s)"%(
            'UNIQUE' if unique else '', index_name, self.name, cols)
        self.database(cmd)

    def drop_index(self, **kwds):
        cols, index_name = self._index_pattern(kwds)
        cmd = "DROP INDEX IF EXISTS %s"%index_name
        self.database(cmd)

    def drop_indexes(self):
        cmd = "SELECT * FROM sqlite_master WHERE type='index' and tbl_name='%s'"%self.name
        for x in self.database(cmd):
            if x[1].startswith('idx___'):
                self.database("DROP INDEX IF EXISTS %s"%x[1])

    def indexes(self):
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
        a = self.database("PRAGMA table_info(%s)"%self.name)
        if a is None:
            return []
        return [x[1] for x in a]

    def columns(self):
        return [x for x in self._columns() if x != 'rowid']

    def _add_columns(self, new_columns):
        for col in new_columns:
            try:
                self.database("ALTER TABLE %s ADD COLUMN %s"%(self.name, col))
            except xmlrpclib.Fault:
                # TODO: make it into a single transaction...
                # The above could safely fail if another client tried
                # to add at the same time and made the relevant
                # column. Ignore error here and deal with it later.
                pass

    def find_one(self, *args, **kwds):
        """
        Return first document that match the given query.
        """
        v = list(self.find(*args, limit=1, **kwds))
        if len(v) == 0:
            raise ValueError, "found nothing"
        return v[0]
        
    def _where_clause(self, query, kwds):
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
        cmd = 'SELECT rowid,' if _rowid else 'SELECT '
        cmd += 'COUNT(*) ' if _count else ' * '
        if fields is None:
            cmd += ' FROM %s'%self.name
        else:
            if isinstance(fields, str):
                fields = [fields]
            cmd += '%s FROM %s'%(','.join(fields), self.name)

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
        """
        kwds['_count'] = True
        cmd = self._find_cmd(*args, **kwds)
        return self.database(cmd)[0]

    def find(self, query='', fields=None, batch_size=50,
             _rowid=False, limit=None, offset=0, **kwds):
        """
        Return iteratoe over all documents that match the given query.
        """
        cmd = self._find_cmd(query=query, fields=fields, batch_size=batch_size,
                             _rowid=_rowid,
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
        

def _insert_statement(table, cols):
    """
    Return SQLite INSERT statement template for inserting the columns
    into the given table.
    
    INPUT:
    - table -- name of a SQLite table
    - cols -- list of strings

    EXAMPLES::

        sage: from nosqlite import _insert_statement
        sage: _insert_statement('table_name', ['col1', 'col2', 'col3'])
        'INSERT INTO table_name (col1,col2,col3) VALUES(?,?,?)'
    """
    return 'INSERT INTO %s (%s) VALUES(%s)'%(table, ','.join(cols), ','.join(['?']*len(cols)))

def _constant_key_grouping(d):
    """
    Group the list d into a list of sublists with constant keys.
    
    INPUT:
    - d -- a list of dictionaries
    OUTPUT:
    - a list of lists of dictionaries with constant keys

    EXAMPLES::

        sage: from nosqlite import _constant_key_grouping
        sage: _constant_key_grouping([{'a':5,'b':7}, {'a':10,'c':4}, {'a':5, 'b':8}])
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

# convenience for lower-case people    
server = Server
client = Client

