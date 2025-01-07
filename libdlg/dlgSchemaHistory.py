import os
import re

from libdlg.dlgStore import *
from libdlg.dlgSchema import SchemaXML, to_releasename
from libjnl.jnlFile import JNLFile

__all__ = ['JNLHistory']

class JNLHistory(object):
    ''' USAGE:

        >>> oHist = JNLHistory()
        >>>


    '''
    def __init__(self, p4schema=None, version='latest'):
        ''' the idea is to use the latest schema available,
            which should contain the bare bones needed make
            the best target release guess. Though, as-is,
            this newly-available schema data does present as
            being weak in substance, and therefore not all
            that useful. Much work ahead of us to guess
            efficiently.

            The class reference exposes a history of the p4
            schema.

            Best of luck!
        '''
        self.history = Lst()
        oSchemaxml = SchemaXML(version=version)
        local_releases = oSchemaxml.listreleases_local()
        p4schema = p4schema.p4schema \
            if (p4schema is not None) \
            else oSchemaxml.p4schema
        schema_history = self.get_schema_history(p4schema)
        EOH = False
        filtered_history = filter(
            lambda hrec: (hrec.version or hrec.release_id),
            schema_history
        )
        while (EOH is False):
            try:
                histrecord = next(filtered_history)
                history_record = self.iter_schemas(
                    histrecord,
                    local_releases
                )
                if (len(history_record) > 0):
                    self.history.append(history_record)
            except StopIteration:
                EOH = True

    def get_jnlreader(self, jnlfile):
        if (isinstance(jnlfile, str) is True):
            jnlfile = os.path.abspath(jnlfile)
            oJFile = JNLFile(jnlfile)
        else:
            oJFile = jnlfile
        return enumerate(oJFile.csvread())

    def __call__(self, jnlfile):
        EOF = False
        reader = self.get_jnlreader(jnlfile)
        while (EOF is False):
            ''' make a fresh reader available at each iteration.
            '''
            try:
                (lineno, jnlline) = next(reader)
                ''' ... '''
                print(lineno, jnlline)
            except (StopIteration, EOFError):
                EOF = True

    def get_schema_history(self, p4schema):
        for item in ('server_versions', 'releases'):
            if (p4schema(item) is not None):
                return p4schema[item].release

    def iter_schemas(self, histrecord, local_releases):
        recversion = histrecord.version or histrecord.release_id
        history_record = Storage()
        if (recversion is not None):
            release = to_releasename(recversion)
            if (release in local_releases):
                history_record.merge(
                    {
                        'version': recversion,
                        'release': release,
                    }
                )
                oSchema = SchemaXML(version=release)
                tables = oSchema.p4schema.tables.table
                tables_count = len(tables)
                history_record.merge(
                    {
                        'tables_count': tables_count,
                        'p4schema': oSchema.p4schema
                    }
                )
                del oSchema
                for key in (
                        'added',
                        'changed',
                        'removed'
                ):
                    actions = histrecord(key) or Lst(())
                    if (isinstance(actions, str) is True):
                        actions = Lst(re.split('\s', actions)).clean()
                    history_record.merge({key: actions})
            return history_record