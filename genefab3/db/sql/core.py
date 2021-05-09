from itertools import count
from sqlite3 import OperationalError
from genefab3.db.sql.pandas import SQLiteIndexName
from genefab3.db.sql.utils import sql_connection
from genefab3.common.utils import validate_no_backtick, validate_no_doublequote
from genefab3.common.logger import GeneFabLogger
from threading import Thread


class SQLiteObject():
    """Universal wrapper for cached objects"""
 
    def __init__(self, sqlite_db, table_schemas=None):
        """Initialize SQLiteObject, ensure tables in `sqlite_db`"""
        self.sqlite_db, self.table_schemas = sqlite_db, table_schemas
        self.changed = None
        with sql_connection(self.sqlite_db, "tables") as (_, execute):
            for table, schema in (table_schemas or {}).items():
                execute(
                    "CREATE TABLE IF NOT EXISTS `{}` ({})".format(
                        validate_no_backtick(table, "table"), ", ".join(
                            "`" + validate_no_backtick(f, "field") + "` " + k
                            for f, k in schema.items()
                        ),
                    ),
                )
 
    @classmethod
    def iterparts(cls, table, connection, *, must_exist=True, partname_mask="{table}://{i}"):
        """During an open connection, iterate all parts of `table` and their index and column names"""
        for i in count():
            partname = table if i == 0 else partname_mask.format(table=table, i=i)
            if must_exist:
                query = f"SELECT * FROM `{partname}` LIMIT 0"
                try:
                    cursor = connection.cursor()
                    cursor.execute(query).fetchall()
                except OperationalError:
                    break
                else:
                    desc = cursor.description
                    index_name = SQLiteIndexName(desc[0][0])
                    columns = [c[0] for c in desc[1:]]
                    yield partname, index_name, columns
            else:
                yield partname, None, None
 
    @classmethod
    def drop_all_parts(cls, table, connection):
        """During an open connection, drop all parts of `table`"""
        for partname, *_ in cls.iterparts(table, connection, must_exist=True):
            connection.execute(f"DROP TABLE IF EXISTS `{partname}`")
 
    @property
    def trigger(self):
        """Evaluates to True if underlying data in need of update, otherwise False"""
        msg = "did not provide self.trigger, will never update"
        GeneFabLogger().warning(f"{type(self).__name__} {msg}")
        return False
 
    def update(self):
        """Update underlying data in SQLite"""
        msg = "did not provide self.update(), will never update"
        GeneFabLogger().warning(f"{type(self).__name__} {msg}")
 
    def retrieve(self):
        """Retrieve underlying data from SQLite"""
        msg = "did not provide self.retrieve(), will always retrieve `None`"
        GeneFabLogger().warning(f"{type(self).__name__} {msg}")
        return None
 
    def _deferrable_cleanup(self):
        """Actions to be performed after request completion"""
        pass
 
    @property
    def data(self):
        """Main interface: returns data associated with this SQLiteObject; will have auto-updated itself in the process if necessary"""
        if self.trigger:
            self.update()
            self.changed = True
        else:
            self.changed = False
        Thread(target=self._deferrable_cleanup).start()
        return self.retrieve()
