# Purpose : class to implements  methods and variable required for all the database operations
# Author : Kasirajan Pothiraj
# Date : Feb 2018
class Db():

    def __init__(self, **kwargs):

        self._con = None
        self._host = kwargs['host'] if kwargs.has_key('host') else 'localhost'
        self._username = kwargs['username']
        self._password = kwargs['password']
        self._dbname = kwargs['database']
        self._driver = kwargs['driver']

        #encloser characters for system identifiers(key_delim) and strings(str_delim)
        self._key_delim = '"'
        self._str_delim = "'"

        if self._driver == 'mysql':
            self._module = __import__('MySQLdb')
            self._key_delim = '`'
        elif self._driver == 'pgsql':
            self._module = __import__('psycopg2')
        else:
            raise Exception("Unknown database driver")

        self._affected_rows = None
        self._last_query = None
        self._insert_id = None
        self._error = None
        self._autocommit = False

        self.connect()

    #def __del__(self):
        #self.disconnect()

    def connect(self):
        kwargs = {'host': self._host, 'user': self._username}
        if self._driver == 'mysql':
            kwargs['passwd'] = self._password
            kwargs['db'] = self._dbname
        elif self._driver == 'pgsql':
            kwargs['database'] = self._dbname
            kwargs['password'] = self._password

        self._con = self._module.connect(**kwargs)

    def disconnect(self):
        if self._con:
            self._con.commit()
            self._con.close()

    def reconnect(self):
        self.disconnect()
        self.connect()

    #fake connecting to the database. Useful when trying out connection parameters, e.g. during install
    @staticmethod
    def mock(**kwargs):
        try:
            d = Db(kwargs)
            return true
        except Exception:
            return false


    #queries the database, returning the result as a list of dicts or None, if no row found or on commands
    def _query(self, s, params = None):

        if isinstance(params, list):
            params = tuple(params)

        #need this for compatibility with manual queries using MySQL format, where the backtick is used for enclosing column names
        #instead of the standard double quote. Will be removed soon
        if self._driver != 'mysql':
            s = self.__replace_backticks(s)

        try:
            cur = self._con.cursor()
            cur.execute(s, params)
            self._insert_id = cur.lastrowid
            self._affected_rows = cur.rowcount

            try:
                results = cur.fetchall()
                n = len(results)
                if (n > 0):
                    cols = self.table_columns(None, cur)
            except self._module.DatabaseError:
                #INSERT/UPDATE or similar
                return None
            finally:
                cur.close()

            retval = []
            for i in range(0,n):
                aux = results[i]
                row = {}
                for j in range(0,len(cols)):
                    #elem = aux[j].decode('UTF-8') if isinstance(aux[j], basestring) else aux[j]
                    row[cols[j]] = aux[j]

                if len(row):
                    retval.append(row)

            return retval

        except self._module.DatabaseError as e:
            #Error. Reset insert id and affected rows to None
            self._insert_id = None
            self._affected_rows = None
            raise Exception("Database Error: %s" % str(e))

        return retval

    #escape a variable/tuple/list
    def escape(self, s):
        if isinstance(s, basestring):
            return self._con.escape_string(s)
        elif isinstance(s, list):
            return map(lambda x: self.escape(x), s)
        elif isinstance(s, tuple):
            return tuple(self.escape(list(s)))
        else:
            raise TypeException("Unknown parameter given for escaping")

        #never get here
        return None

    #encloses a string with single quotes
    def enclose_str(self, s):
        if isinstance(s, basestring):
            return ''.join([self._str_delim,str(s),self._str_delim])
        elif isinstance(s, list):
            return map(self.enclose_str, s)
        elif isinstance(s, tuple):
            return tuple(map(self.enclose_str, s))
        else:
            raise TypeError("Unknown argument type to enclose_str")

    #encloses an identifier in the appropriate double quotes/backticks
    def enclose_sys(self,s):
        #we do not enclose variable containing spaces because we assume them to be expressions, e.g. COUNT(*) AS ...
        #Column names containing spaces are not supported
        if isinstance(s, basestring):
            if s.count(' ') or s == '*':
                return s
            return ''.join([self._key_delim,str(s),self._key_delim])
        elif isinstance(s, list):
            return map(self.enclose_sys, s)
        elif isinstance(s, tuple):
            return tuple(map(self.enclose_sys, s))
        else:
            raise TypeError("Unknown argument type to enclose_sys")

    #SELECT FROM table
    def select(self, table, columns = None, where = None, op = "AND"):
        if isinstance(columns, tuple):
            columns = ",".join(map(lambda x: self.enclose_sys(x), columns))
        elif isinstance(columns, basestring):
            columns = self.enclose_sys(columns)
        elif not columns:
            columns = "*"
        else:
            raise TypeException("Invalid column definition")

        (where_clause, where_params) = self.__expand_where_clause(where, op)

        if not where_clause:
            return self._query("SELECT %s FROM %s" % (columns, self.enclose_sys(table)))
        else:
            return self._query("SELECT %s FROM %s WHERE %s" % (columns, self.enclose_sys(table), where_clause), where_params)

    #INSERT INTO table
    def insert(self, table, values):
        if isinstance(values, tuple):
            values = [values]
        if not isinstance(values, list):
            raise TypeError("INSERT: Inappropriate argument type for parameter values")
        #cur = self._con.cursor()
        col_arr = self.get_columns(table)
        cols = map(lambda x: self.enclose_sys(x), col_arr)
        vals = tuple(map(lambda x: x, values))
        #cur.close()
        sql = 'INSERT INTO %s(%s) VALUES (%s)' % (self.enclose_sys(table), ','.join(cols), ','.join( ['%s'] * len(vals) ))
        return self._query(sql, vals)

    #UPDATE table
    def update(self, table, values, where = None, op = 'AND'):
        if isinstance(values, tuple):
            values = [values]
        if not isinstance(values, list):
            raise TypeError("UPDATE: Inappropriate argument type for parameter values")

        cols = map(lambda x: self.enclose_sys(x[0])+'=%s', values)
        vals = tuple(map(lambda x: x[1], values))

        (where_clause, where_params) = self.__expand_where_clause(where, op)

        if where_clause:
            return self._query('UPDATE %s SET %s WHERE %s' % (self.enclose_sys(table), ','.join(cols), where_clause), list(vals) + list(where_params))
        else:
            return self._query('UPDATE %s SET %s' % (self.enclose_sys(table), ','.join(cols)), vals)

    #DELETE FROM table
    def delete(self, table, where = None, op = 'AND'):
        (where_clause, where_params) = self.__expand_where_clause(where, op)

        if where_clause:
            return self._query("DELETE FROM %s WHERE %s" % (self.enclose_sys(table), where_clause), where_params)
        else:
            return self._query("DELETE FROM %s" % (self.enclose_sys(table)))

    #upsert and merge perform the same task, having the same end result.
    #The difference is that the former is optimised to work on data where usually little new rows are added
    #while the latter is optimised in the case the majority of the data dealt with will be added, not already existing
    def upsert(self, table, values, where):
        self.update(table, values, where)
        if not self.affected_rows():
            self.insert(table, [values] + [where])
    def merge(self, table, values, where):
        try:
            self.insert(table, [values] + [where])
        except self._module.DatabaseError as e:
            #TODO: Check error in case it's not due to a PK/Unique violation
            self.update(table, values, where)

    #Returns a row, instead of simply a list of 1. Inspired by Wordpress
    def get_row(self, table, columns = None, where = None, op = "AND"):
        r = self.select(table, columns, where, op)
        return r[0] if r else None

    #Returns a variable. Useful for quick counts or returning of an id, for example. Inspired by Wordpress
    def get_var(self, table, columns = None, where = None, op = "AND"):
        r = self.select(table, columns, where, op)
        return r[0].items()[0][1] if r else None

    #Count the rows of a table
    def count(self, table, column, value = None):
        where = (column, value) if value else None
        return self.get_var(table, 'COUNT(*) AS %s' % (self.enclose_sys('cunt')), where)


    def drop(self):
        self._query("DROP DATABASE " + self._dbname)
    def create(self):
        self._query("CREATE DATABASE " + self._dbname)
    def purge(self):
        #only works in MySQL, must find alternative for Postgres
        self.drop()
        self.create()
    def truncate(self, table_name):
        self._query("TRUNCATE TABLE " + self.enclose_sys(table_name))


    #wrappers around transaction management functions
    def commit(self):
        self._con.commit()
    def rollback(self):
        self._con.rollback()
    def autocommit(self, val):
        self._autocommit(bool(val))

    #getters...
    def affected_rows(self):
        return self._affected_rows
    def insert_id(self):
        return self._insert_id

    def __is_escaped(self, s, pos):
        for char in ["'", "\\"]:
            j = pos - 1
            count = 0

            #count back the num. of appearances of certain char
            while (j>=0 and s[j] == char):
                j-=1
                count+=1

            #reduce the count in cases like \'' ,where the last ' to the left is escaped by 1 or more \
            if (char == "'" and count and self.__isEscaped(s, pos-count)):
                count-=1
            if (count):
                break

        return True if (count % 2) else False


    #replaces MySQL style `backticks` with "double quotes", as per SQL standard.
    #Required in order to support MySQL queries containing backticks
    def __replace_backticks(self, str):
        s = list(str)
        delim = None
        inside = False


        for i in range(0, len(s)):
            #only working on important characters
            if (s[i] not in ['"',"'","`"]):
                continue

            if inside:
                if (s[i] == '`' or s[i] != delim): #if we encounter a wrong token, simply continue
                    continue

                if not self.__is_escaped(s, i):
                    inside = False
                    delim = None
            else:
                if s[i] == '`':
                    s[i] = '"'
                    continue

                if not self.__is_escaped(s, i):
                    inside = True
                    delim = s[i]

        return "".join(s)

    #helper function, expands a tuple/list of tuples containing where parameters to string
    def __expand_where_clause(self, where, op):
        params = []
        clauses = []

        if where:
            if isinstance(where, tuple):
                where = [where]
            if not isinstance(where, list):
                raise TypeException("Unknown type for WHERE clause argument")

        if where:
            for clause in where:
                clause_op = clause[2] if len(clause)==3 else '='
                clauses.append(self.enclose_sys(clause[0]) + (" %s " % clause_op) + '%s')
                params.append(clause[1])

        where_clause = (' %s ' % op).join(clauses)
        return (where_clause, tuple(params) if len(params) else None)

    #returns an array containing the names of the columns of a table
    def table_columns(self, table_name = None, cur = None):
        if not cur:
            try:
                cur = self._con.cursor()
                cur.execute("SELECT * FROM " + table_name + " LIMIT 1")
                cur.close()
            except self._module.DatabaseError as e:
                raise Exception("Database Error: %s" % str(e))

        cols = map(lambda x: x[0], cur.description)
        return cols

 #returns the names of the columns of a table
    def get_columns(self, table_name = None):
            try:
                cur = self._con.cursor()
                cur.execute("SELECT * FROM " + table_name + " LIMIT 1")
                result = map(lambda x: x[0], cur.description)
                #result = [dict(zip(fields, row)) for row in cur.fetchall()]
                cur.close()
            except self._module.DatabaseError as e:
                raise Exception("Database Error: %s" % str(e))
            return result
