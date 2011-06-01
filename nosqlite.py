import os
import SimpleXMLRPCServer
import sqlite3
import xmlrpclib

class Server(object):
    def __init__(self, directory='/tmp/db', address="localhost", port=8100):
        self.directory = str(directory)
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.address = str(address)
        self.port = int(port)
        self._dbs = {}

    def db(self, file):
        try:
            return self._dbs[file]
        except KeyError:
            db = sqlite3.connect(file)
            self._dbs[file] = db
            return db

    def run(self):
        server = SimpleXMLRPCServer.SimpleXMLRPCServer(
            (self.address, self.port), allow_none=True)

        def execute(cmds, t, file='default'):
            db = self.db(os.path.join(self.directory, file))
            cursor = db.cursor()
            if isinstance(cmds, str):
                if t is not None:
                    cmds = [(cmds, t)]
                else:
                    cmds = [cmds]
            v = []
            for c in cmds:
                if isinstance(c, tuple):
                    o = cursor.execute(*c)
                else:
                    o = cursor.execute(c)
                v.extend(list(o))
            db.commit()
            return v

        server.register_function(execute, 'execute')
        server.serve_forever()

    def __repr__(self):
        return "noSQLite server http://%s:%s"%(self.address, self.port)


################## something for sage
try:
    from sage.rings.all import is_Integer, is_RealNumber
except:
    is_Integer = lambda x: False
    is_RealNumber = lambda x: False

class Client(object):
    def __init__(self, address="localhost", port=8100):
        self.address = str(address)
        self.port = port
        self.server = xmlrpclib.Server('http://%s:%s'%(address, port),
                                       allow_none=True)

    def __repr__(self):
        return "noSQLite client http://%s:%s"%(self.address, self.port)
        
    def __call__(self, cmds, t=None, file='default', coerce=True):
        if t is not None and coerce:
            t = tuple([self._coerce_(x) for x in t])
        return self.server.execute(cmds, t, file)
    
    def __getattr__(self, name):
        return Database(self, name)

    def _coerce_(self, x):
        if is_Integer(x):
            x = int(x)
        elif is_RealNumber(x):
            x = float(x)
        return x
        

class Database(object):
    def __init__(self, client, name):
        self.client = client
        self.name = str(name)

    def __call__(self, cmds, t=None):
        return self.client(cmds, t)

    def __getattr__(self, name):
        return Collection(self, name)

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
        return "Collection '%s'"%self.name

    def __len__(self):
        cmd = "SELECT COUNT(*) FROM %s"%self.name
        return int(self.database(cmd)[0][0])

    ###############################################################
    # Inserting documents
    ###############################################################
    def insert(self, d):
        # Make sure that the keys of d are a subset of the columns of
        # the corresponding table.  If not, expand that table by
        # adding a new column, which is the one thing we can do
        # to change a table in sqlite!  http://www.sqlite.org/lang_altertable.html
        
        if instance(d, list):
            # for this will need to:
            #  1. compute union of keys of all dicts in d
            #  2. ensure all columns present in database
            #  3. do the whole insert very efficiently.
            raise NotImplementedError
        
        cmd = 'INSERT INTO %s (%s) VALUES(%s)'%(
            self.name, ','.join(d.keys()), ','.join(['?']*len(d.keys())))
        t = d.values()
        while True:
            try:
                #print 'trying: ', cmd
                self.database(cmd, t)
                return
            except xmlrpclib.Fault, e:
                s = e.faultString
                if len(d.keys()) == 0:
                    raise ValueError, "d must have at least one key"
                if ':no such table:' in s:
                    c = "CREATE TABLE %s (%s)"%(self.name, ', '.join(d.keys()))
                    #print c
                    self.database(c)
                elif 'has no column named' in s:
                    # be "evil" for a moment
                    i = s.rfind('named ')
                    column = s[i+6:]
                    c = "ALTER TABLE %s ADD COLUMN %s"%(self.name, column)
                    # print c
                    self.database(c)
                else:
                    raise e

    ###############################################################
    # Indexes: creation, dropping, listing
    ###############################################################
    
    def _index_pattern(self, kwds):
        cols = ','.join(['%s %s'%(column, 'DESC' if direction < 0 else 'ASC') for
                         column, direction in sorted(kwds.iteritems())])
        index_name = 'idx___%s___%s'%(self.name, cols.replace(',','___').replace(' ',''))
        return cols, index_name

    def ensure_index(self, **kwds):
        cols, index_name = self._index_pattern(kwds)
        cmd = "CREATE INDEX IF NOT EXISTS %s ON %s(%s)"%(index_name, self.name, cols)
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
        return [x[1] for x in self.database("PRAGMA table_info(%s)"%self.name)]

    def find(self, query=None, fields=None, _limit=100, **kwds):
        if fields is None:
            cmd = 'SELECT rowid,* FROM %s'%self.name
        else:
            if isinstance(fields, str):
                fields = [fields]
            cmd = 'SELECT rowid,%s FROM %s'%(','.join(fields), self.name)
        if len(kwds) > 0:
            for key, val in kwds.iteritems():
                val = self.database.client._coerce_(val)
                if query is not None:
                    query += ' AND %s=%r '%(key, val)
                else:
                    query = ' %s=%r '%(key, val)
        if query is not None:
            cmd += ' WHERE %s'%query
        if _limit is not None:
            cmd += ' LIMIT %s'%int(_limit)
        #print cmd
        v = self.database(cmd)
        if fields is None:
            columns = self._columns()
        else:
            columns = fields
        columns = ['rowid'] + columns
        return [dict([a for a in zip(columns, x) if a[1] is not None]) for x in v]
        


