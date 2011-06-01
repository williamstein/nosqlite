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
        return "coSQLite server http://%s:%s"%(self.address, self.port)


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
        return "coSQLite client http://%s:%s"%(self.address, self.port)
        
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
        return "coSQLite database %s"%self.name

    def collections(self):
        raise NotImplementedError

class Collection(object):
    def __init__(self, database, name):
        self.database = database
        self.name = str(name)

    def columns(self):
        return [x[1] for x in self.database("PRAGMA table_info(%s)"%self.name)]

    def __repr__(self):
        return "coSQLite collection %s"%self.name

    def insert(self, d):
        # Make sure that the keys of d are a subset of the columns of
        # the corresponding table.  If not, expand that table by
        # adding a new column, which is the one thing we can do
        # to change a table in sqlite!  http://www.sqlite.org/lang_altertable.html
        cmd = 'INSERT INTO %s (%s) VALUES(%s)'%(
            self.name, ','.join(d.keys()), ','.join(['?']*len(d.keys())))
        t = d.values()
        while True:
            try:
                print 'trying: ', cmd
                self.database(cmd, t)
                return
            except xmlrpclib.Fault, e:
                s = e.faultString
                if len(d.keys()) == 0:
                    raise ValueError, "d must have at least one key"
                if ':no such table:' in s:
                    c = "CREATE TABLE %s (%s)"%(self.name, ', '.join(d.keys()))
                    print c
                    self.database(c)
                elif 'has no column named' in s:
                    # be "evil" for a moment
                    i = s.rfind('named ')
                    column = s[i+6:]
                    c = "ALTER TABLE %s ADD COLUMN %s"%(self.name, column)
                    print c
                    self.database(c)
                else:
                    raise e

    def ensure_index(self, column):
        raise NotImplementedError

    def find(self, query=None, limit=100, **kwds):
        cmd = 'SELECT * FROM %s'%self.name
        if len(kwds) > 0:
            for key, val in kwds.iteritems():
                if query is not None:
                    query += ' AND %s="%s" '%(key, val)
                else:
                    query = ' %s="%s" '%(key, val)
        if query is not None:
            cmd += ' WHERE %s'%query
        if limit is not None:
            cmd += ' LIMIT %s'%int(limit)
        columns = self.columns()
        print cmd
        v = self.database(cmd)
        return [dict([a for a in zip(columns, x) if a[1] is not None]) for x in v]


