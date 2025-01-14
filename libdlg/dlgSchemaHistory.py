import os
import re
from pprint import pprint

from libdlg.dlgStore import *
from libdlg.dlgSchema import SchemaXML, to_releasename
from libdlg.dlgSchemaTypes import SchemaType
from libjnl.jnlFile import JNLFile
from libdlg.dlgUtilities import ignore_actions
from libdlg.dlgQuery_and_operators import AND, OR, NOT

__all__ = ['JNLHistory']

class JNLHistory(object):
    ''' USAGE:

        >>> oHist = JNLHistory()
        >>>
    '''
    def __init__(self, p4schema=None, version='latest'):
        (self.history, EOH) = (Lst(), False)
        oSchemaxml = SchemaXML(version=version)
        local_releases = oSchemaxml.listreleases_local()
        p4schema = p4schema.p4schema \
            if (p4schema is not None) \
            else oSchemaxml.p4schema
        self.oSchemaxml = oSchemaxml
        self.local_releases = oSchemaxml.listreleases_local()
        schema_history = self.get_schema_history(p4schema)
        filtered_history = filter(
            lambda hrec: (hrec.version or hrec.release_id),
            schema_history
        )
        while (EOH is False):
            try:
                histrecord = next(filtered_history)
                history_record = self.historical_record(histrecord, local_releases)
                if (len(history_record) > 0):
                    self.history.append(history_record)
            except StopIteration:
                EOH = True

    def gen_hist_filter(self, history, func=None):
        ''' a historical filter iterator
        '''
        qry = lambda rec: (rec.release in self.local_releases) if (func is None) else func
        return filter(qry, history)#reversed(history))

    def foundtables(self, jnlfile):
        re_db = re.compile('^db.')
        reader = self.get_jnlreader(jnlfile)
        EOF = False
        tableset = set()
        while (EOF is False):
            try:
                (lineno, jnlrecord) = next(reader)
                if (not jnlrecord[0] in ignore_actions):
                    if (re_db.match(jnlrecord[2]) is not None):
                        tableset.add(jnlrecord[2])
            except StopIteration:
                EOF = True
        return Lst(tableset)

    ''' Starting with the latest available schema (stored locally), measure journal records against them.    
    '''

    def __call__(self, jnlfile):
        ''' a CSV reader object
        '''
        foundtables = self.foundtables(jnlfile)
        #fhistory = self.gen_hist_filter(self.history)
        self.history#.reverse()
        history = self.history
        all = Lst()
        all_versions = Lst()
        all_tblver_mismatches = Lst()
        all_field_mismatches = Lst()
        for idx in range(0, len(history)):
            reader = self.get_jnlreader(jnlfile)
            seentables = set()
            hist = history(idx)
            tableversions = Storage()
            tblver_mismatches = Storage()
            fieldlen_mismatches = Storage()
            intersect = foundtables.intersect(hist.tablenames)
            if (len(intersect) == len(foundtables)):
                nexthist = (idx + 1) if (idx < (len(history) - 1)) else None
                EOF = False
                while (EOF is False):
                    try:
                        (lineno, jnlrecord) = next(reader)
                        rec_tablename = jnlrecord[2]
                        if (rec_tablename == 'db.change') & (hist.release == 'r16.1'):
                            h = 'here'
                        if (not jnlrecord[0] in ignore_actions):
                            if (not rec_tablename in seentables):
                                if (rec_tablename in foundtables):
                                    seentables.add(rec_tablename)
                                    rec_tableversion = jnlrecord[1]
                                    table = self.get_table_datatype(hist.oSchema, rec_tablename)
                                    if (rec_tableversion == table.version):
                                        tablefields = self.tablefields(hist.oSchema, rec_tablename)
                                        lenrecord = len(jnlrecord)
                                        lenfields = (len(tablefields) + 3)

                                        if (jnlrecord[-1] == ''):
                                            jnlrecord.pop(-1)
                                        if (lenfields == lenrecord):
                                            tableversions.merge({rec_tablename: [rec_tableversion, table.version]})

                                        else:
                                            fieldlen_mismatches.merge({rec_tablename: [lenrecord, lenfields]})
                                            #EOF = True

                                    else:
                                        tblver_mismatches.merge({rec_tablename: [rec_tableversion, table.version]})
                                        #EOF = True

                    except (StopIteration, EOFError) as err:
                        all.merge(
                            {hist.release: {'tableversions': tableversions,
                                            'tableversion_mismatches': tblver_mismatches,
                                            'field_mismatches': fieldlen_mismatches
                                            }
                             }
                        )
                        EOF = True
                #no_intersect = tableversions.getkeys().no_intersect(foundtables)
                #if (len(no_intersect) == 0):

                #all_versions.merge({hist.release: tableversions})
                #all_tblver_mismatches.merge({hist.release: tblver_mismatches})
                #all_field_mismatches.merge({hist.release:fieldlen_mismatches})
        pprint(all)
        #pprint(all_versions)
        #pprint(all_tblver_mismatches)
        #pprint(all_field_mismatches)

                # r10.2:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.have', 'db.desc', 'db.bodtext', 'db.revsx', 'db.locks', 'db.view', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.user', 'db.revsh', 'db.config', 'db.archmap', 'db.domain', 'db.integed']
                # r11.1:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.have', 'db.desc', 'db.bodtext', 'db.revsx', 'db.locks', 'db.view', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.user', 'db.revsh', 'db.config', 'db.archmap', 'db.domain', 'db.integed']
                # r12.1:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.have', 'db.desc', 'db.bodtext', 'db.revsx', 'db.locks', 'db.view', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.user', 'db.revsh', 'db.config', 'db.archmap', 'db.domain', 'db.integed']
                # r12.2:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.have', 'db.desc', 'db.bodtext', 'db.revsx', 'db.locks', 'db.view', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.user', 'db.revsh', 'db.config', 'db.archmap', 'db.domain', 'db.integed']
                # r13.1:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.have', 'db.desc', 'db.bodtext', 'db.revsx', 'db.locks', 'db.view', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.user', 'db.revsh', 'db.config', 'db.archmap', 'db.domain', 'db.integed']
                # r13.3:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.have', 'db.desc', 'db.bodtext', 'db.revsx', 'db.locks', 'db.view', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.user', 'db.revsh', 'db.config', 'db.archmap', 'db.domain', 'db.integed']
                # r14.1:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.have', 'db.desc', 'db.bodtext', 'db.revsx', 'db.locks', 'db.view', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.user', 'db.revsh', 'db.config', 'db.archmap', 'db.domain', 'db.integed']
                # r14.2:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.have', 'db.desc', 'db.bodtext', 'db.locks', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.revsh', 'db.config', 'db.archmap', 'db.integed']
                # r15.1:    ['db.changex', 'db.revhx', 'db.revcx', 'db.job', 'db.ixtext', 'db.bodtext', 'db.change', 'db.revdx', 'db.rev', 'db.workingx', 'db.revsh', 'db.config', 'db.archmap', 'db.integed']
                # r15.2:    ['db.bodtext', 'db.ixtext', 'db.config', 'db.job']
                # r16.1:    ['db.bodtext', 'db.group', 'db.revcx', 'db.revsh', 'db.integed', 'db.archmap', 'db.changex', 'db.rev', 'db.workingx', 'db.locks', 'db.ixtext', 'db.desc', 'db.resolve', 'db.change', 'db.working', 'db.revdx', 'db.config', 'db.job', 'db.revhx']
                # r16.2:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.desc', 'db.bodtext', 'db.locks', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.revsh', 'db.config', 'db.archmap', 'db.integed']
                # r17.1:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.have', 'db.desc', 'db.bodtext', 'db.revsx', 'db.locks', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.revsh', 'db.config', 'db.archmap', 'db.domain', 'db.integed']
                # r17.2:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.have', 'db.desc', 'db.bodtext', 'db.revsx', 'db.locks', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.revsh', 'db.config', 'db.archmap', 'db.domain', 'db.integed']
                # r18.1:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.have', 'db.desc', 'db.bodtext', 'db.revsx', 'db.locks', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.revsh', 'db.config', 'db.archmap', 'db.domain', 'db.integed']
                # r18.2:    ['db.resolve', 'db.changex', 'db.revhx', 'db.group', 'db.revcx', 'db.job', 'db.ixtext', 'db.have', 'db.desc', 'db.bodtext', 'db.revsx', 'db.locks', 'db.change', 'db.revdx', 'db.rev', 'db.working', 'db.workingx', 'db.revsh', 'db.config', 'db.archmap', 'db.domain', 'db.integed']



    def __call__WO(self, jnlfile):
        ''' a CSV reader object
        '''
        reader = self.get_jnlreader(jnlfile)

        #fhistory = self.gen_hist_filter(self.history)
        #self.history#.reverse()

        tablesfound = self.foundtables(jnlfile)
        seentables = set()
        EOF = False
        while (EOF is False):
            ''' make a fresh reader available at each iteration.
            '''
            try:
                (lineno, jnlrecord) = next(reader)
                ''' skip if record's action should be ignored
                '''
                if (not jnlrecord[0] in ignore_actions):
                    rec_tablename = jnlrecord[2]


                    #history = self.gen_hist_filter(
                    #    self.history,
                    #)

                    for idx in range(0, len(self.history)):
                        hist = self.history(idx)
                        previous_idx = (idx - 1) \
                            if (idx > 0) \
                            else None
                        previous_hist = self.history(previous_idx) \
                            if (previous_idx is not None) \
                            else None
                        if (hist.release == 'r16.2'):
                            h = 'here'
                        if (not rec_tablename in seentables):
                            seentables.add(rec_tablename)

                            table = self.get_table_datatype(hist.oSchema, rec_tablename)
                            rec_tableversion = jnlrecord[1]
                            #if (rec_tableversion == table.version):

                            normalized_rec_tablename = re.sub('db.', '', rec_tablename)
                            tablefields = self.tablefields(hist.oSchema, rec_tablename)
                            lenfields = (len(tablefields) + 3)
                            lenrecord = len(jnlrecord)

                            if AND(
                                    (rec_tablename in hist.tablenames),
                                    (rec_tableversion == table.version),
                                    (not rec_tablename in hist.removed),
                                    (lenfields == lenrecord)
                            ):

                                ''' what's different? what changed?
                                '''
                                if (rec_tablename in hist.changed):
                                    relidx = self.local_releases.index(hist.release)
                                    if (relidx > 0):
                                        previousidx = (relidx - 1)
                                        previousrel = self.local_releases(previousidx)
                                        previousschema = SchemaXML(version=previousrel)
                                        previous_hist_schema_lenfields = len(previousschema.p4model[normalized_rec_tablename].fields)
                                        ''' len(fields)
                                        '''
                                        hist_schema_lenfields = len(hist.oSchema.p4model[normalized_rec_tablename].fields)
                                        current_lenrecord = len(jnlrecord)
                                        if (current_lenrecord == hist_schema_lenfields):
                                            ''' WINNER!
                                            '''
                                            return hist.release

                                        #nexttables = nextschema.p4schema.tables.table
                                        #nexttable = next(filter(lambda rec: rec.name == rec_tablename, nexttables))
                                        #tablediff = {key: nexttable[key] for key in (set(nexttable) - set(table))}
                                        #value = { k : second_dict[k] for k in set(second_dict) - set(first_dict) }
                                        #difflen = len(tablediff)
                                if (rec_tablename in hist.added):
                                    #if (previous_hist is not None):
                                    #    if (previous_hist.oSchema.p4schema.)
                                    ''' WINNER!!
                                    '''
                                return hist.release
                            else:
                                seentables = set()
            except (StopIteration, EOFError):
                EOF = True
        return 'NOT SURE'

    def __call__OLD(self, jnlfile):
        ''' a CSV reader object
        '''
        reader = self.get_jnlreader(jnlfile)
        foundtables = self.foundtables(jnlfile)
        seentables = set()
        #fhistory = self.gen_hist_filter(self.history)
        self.history#.reverse()
        history = self.history
        for idx in range(0, len(history)):
            hist = history(idx)
            nexthist = (idx + 1) \
                if (idx < (len(history) - 1)) \
                else None
            ''' as we run through schema releases, parse jnl records
            '''
            EOF = False
            while (EOF is False):
                ''' make a fresh reader available at each iteration.
                '''
                try:
                    (lineno, jnlrecord) = next(reader)
                    ''' skip if record's action should be ignored
                    '''
                    if (not jnlrecord[0] in ignore_actions):
                        rec_tablename = jnlrecord[2]
                        normalized_rec_tablename = re.sub('db.', '', rec_tablename)
                        if (rec_tablename in hist.added):
                            ''' WINNER!!
                            '''
                            return hist.release

                        ''' skip if we've seen this table
                        '''
                        if (not rec_tablename in seentables):
                            seentables.add(rec_tablename)
                            rec_tableversion = jnlrecord[1]
                            tablefields = self.tablefields(hist.oSchema, rec_tablename)
                            lenfields = (len(tablefields) + 4)
                            lenrecord = len(jnlrecord)

                            table = self.get_table_datatype(hist.oSchema, rec_tablename)
                            if NOT(
                                    OR(
                                        (rec_tablename in hist.tablenames),
                                        (rec_tableversion == table.version),
                                        (not rec_tablename in hist.removed),
                                        (lenfields == lenrecord)
                                    )
                            ):
                                (EOH, EOF) = (True, True)
                                raise StopIteration

                            ''' what's different? what changed?
                            '''
                            if (rec_tablename in hist.changed):
                                relidx = self.local_releases.index(hist.release)
                                if (relidx > 0):
                                    nextidx = (relidx -1)
                                    nextrel = self.local_releases(nextidx)
                                    nextschema = SchemaXML(version=nextrel)
                                    nexthist_schema_lenfields = len(nextschema.p4model[normalized_rec_tablename].fields)
                                    ''' len(fields)
                                    '''
                                    hist_schema_lenfields = len(hist.oSchema.p4model[normalized_rec_tablename].fields)
                                    current_lenrecord = len(jnlrecord)
                                    if (current_lenrecord == hist_schema_lenfields):
                                        ''' WINNER!
                                        '''
                                        return hist.release

                                    #nexttables = nextschema.p4schema.tables.table
                                    #nexttable = next(filter(lambda rec: rec.name == rec_tablename, nexttables))
                                    #tablediff = {key: nexttable[key] for key in (set(nexttable) - set(table))}
                                    #value = { k : second_dict[k] for k in set(second_dict) - set(first_dict) }
                                    #difflen = len(tablediff)

                            #tablesfound = self.foundtables(jnlfile)
                            #if (len(tablesfound.intersect(hist.added)) > 0):
                            #    ''' WINNER!
                            #    '''
                            #    return hist.release

                            '''  hist:
                                     {
                                     'version': '2024.1',
                                     'release': 'r24.1',
                                     'lentables': 111,
                                     'oSchema': <libdlg.dlgSchema.SchemaXML object at 0x133963d10>,
                                     'tablenames': ['db.config', 'db.configh', 'db.counters', 'db.nameval', 'db.upgrades.rp', 'db.upgrades', 'db.logger', 'db.ldap', 'db.topology', 'db.server', 'db.svrview', 'db.remote', 'db.rmtview', 'db.stash', 'db.user.rp', 'db.user', 'db.ticket.rp', 'db.ticket', 'db.group', 'db.groupx', 'db.depot', 'db.stream', 'db.streamrelation', 'db.streamview', 'db.streamviewx', 'db.streamq', 'db.integedss', 'db.domain', 'db.template', 'db.templatesx', 'db.templatewx', 'db.view.rp', 'db.view', 'db.haveview', 'db.review', 'db.label', 'db.have.rp', 'db.have.pt', 'db.have', 'db.integed', 'db.integtx', 'db.resolve', 'db.resolvex', 'db.resolveg', 'db.scandir', 'db.scanctl', 'db.storagesh', 'db.storage', 'db.storageg', 'db.storagesx', 'db.revdx', 'db.revhx', 'db.revpx', 'db.revsx', 'db.revsh', 'db.revbx', 'db.revux', 'db.revcx', 'db.rev', 'db.revtx', 'db.revstg', 'db.revfs', 'db.locks', 'db.locksg', 'db.working', 'db.workingx', 'db.workingg', 'db.haveg', 'db.excl', 'db.exclg', 'db.exclgx', 'db.traits', 'db.revtr', 'db.trigger', 'db.change', 'db.changex', 'db.changeidx', 'db.desc', 'db.repo', 'db.refhist', 'db.ref', 'db.refcntadjust', 'db.object', 'db.graphindex', 'db.graphperm', 'db.submodule', 'db.pubkey', 'db.job', 'db.fix', 'db.fixrev', 'db.bodresolve', 'db.bodresolvex', 'db.bodtext', 'db.bodtextcx', 'db.bodtexthx', 'db.bodtextsx', 'db.bodtextwx', 'db.ixtext', 'db.ixtexthx', 'db.uxtext', 'db.protect', 'db.property', 'db.message', 'db.sendq', 'db.sendq.pt', 'db.jnlack', 'db.monitor', 'db.ckphist', 'rdb.lbr', 'pdb.lbr', 'tiny.db'],
                                     'tablenames_no_prefix': ['rdb.lbr', 'pdb.lbr', 'tiny.db'],
                                     'added': [],
                                     'changed': ['db.sendq', 'db.sendq.pt', 'db.view', 'db.view.rp', 'db.haveview', 'db.streamview'],
                                     'removed': []
                                     }
                            '''
                except (StopIteration, EOFError):
                    EOF = True
        return 'NOT SURE'

    def get_jnlreader(self, jnlfile):
        if (isinstance(jnlfile, str) is True):
            jnlfile = os.path.abspath(jnlfile)
            oJFile = JNLFile(jnlfile)
        else:
            oJFile = jnlfile
        return enumerate(oJFile.csvread())

    def get_table_datatype(self, oSchema, tablename):
        tables = oSchema.p4schema.tables.table
        datatype = next(filter(lambda rec: rec.name == tablename, tables))
        return datatype

    def tablefields(self, oSchema, tablename):
        if (re.match('^db.', tablename) is None):
            tablename = f'db.{tablename}'
        tables = oSchema.p4schema.tables.table
        recordtypes = oSchema.p4schema.recordtypes.record
        datatype = next(filter(lambda rec: rec.name == tablename, tables)).type
        recordtype = next(filter(lambda rec: rec.name == datatype, recordtypes))
        return recordtype.column

    def lenfields(self, oSchema, tablename):
        fields = self.tablefields(oSchema, tablename)
        return (len(fields) + 4)

    def tablenames(self, tables):
        eot = False
        tfilter = filter(lambda rec: rec.name, tables)
        tablenames = Lst()
        while (eot is False):
            try:tablenames.append(next(tfilter).name)
            except StopIteration:
                eot = True
        return tablenames

    def get_lockseq(self, oSchema, tablename):
        tables = oSchema.p4schema.tables.table
        tfilter = filter(lambda rec: (re.match('^db.', rec.name) is None), tables)

    def tablenames_no_dbprefix(self, tables):
        tfilter = filter(lambda rec: (re.match('^db.', rec.name) is None), tables)
        (tablenames, eor) = (Lst(), False)
        while (eor is False):
            try:
                tablename = next(tfilter).name
                tablenames.append(tablename)
            except StopIteration:
                eor = True
        return tablenames

    def get_schema_history(self, p4schema):
        for item in ('server_versions', 'releases'):
            if (p4schema(item) is not None):
                return p4schema[item].release

    def historical_record(self, histrecord, local_releases):
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
                oSchemaType = SchemaType(oSchema=oSchema)
                tables = oSchemaType.tables#self.tablenames(tables)
                tablenames = oSchemaType.tablenames()#self.tablenames(tables)
                tablenames_no_prefix = self.tablenames_no_dbprefix(tables)
                history_record.merge(
                    {
                        'lentables': len(tables),
                        'oSchema': oSchema,
                        'oSchemaType': oSchemaType,
                        'tablenames': tablenames,#,
                        'tablenames_no_prefix': tablenames_no_prefix
                    }
                )
                for key in (
                        'added',
                        'changed',
                        'removed'
                ):
                    actions = histrecord(key) or Lst(())
                    if (isinstance(actions, str) is True):
                        actions = Lst(re.split('\s', actions)).clean()
                    history_record.merge({key: actions})
                del oSchema
            return history_record

        def test(self, jnlfile):
            results = Storage()
            '''  a journal reader

                    +-------+------------------+---------------+
                    | Field | function         | value example |
                    +-------+------------------+---------------+
                    | 0     | record ID        | 1             |
                    +-------+------------------+---------------+
                    | 1     | operation handle | @pv@          |
                    +-------+------------------+---------------+
                    | 2     | table version    | 6             |
                    +-------+------------------+---------------+
                    | 3     | table name       | db.domain     |
                    +-------+------------------+---------------+
            '''
            # with fileopen(journal, 'rb') as oFile:
            #    journalrecords = self.oJnlReader.csvread(oFile)
            journalrecords = self.get_jnlreader()
            (seentables, EOR, counter) = (Lst(), False, 0)
            while (EOR is False):
                try:
                    journalrecord = next(journalrecords)
                    counter += 1
                    table = journalrecord[2]
                    if ((not table in seentables) & (table.startswith('db.'))):
                        seentables.append(table)
                        table = re.sub('db\.', '', table)
                        journaltableversion = journalrecord[1]
                        fields = journalrecord[3:]
                        '''  Query each schemaxml files, collect only those that are a good fit
                                with the passed in journal/checkpoint

                                db.change (common record fields)
                                +----+--------+---------+-------------+
                                | id | handle | version | table       |
                                +----+--------+---------+-------------+
                                | 1  | @pv@   | 2       | @db.change@ |
                                +----+--------+---------+-------------+
                                | 0  | 1      | 2       | 3           |
                                +----+--------+---------+-------------+

                                fields unique to db.change
                                +---+---+-------+-------+-----------+---+-------+---------+
                                | 1 | 1 | @bla@ | @bob@ | 894209293 | 1 | @bla@ | @//...@ |
                                +---+---+-------+-------+-----------+---+-------+---------+
                                | 4 | 5 | 6     | 7     | 8         | 9 | 10    | 11      |
                                +---+---+-------+-------+-----------+---+-------+---------+

                                So this journal is compatable with any schema version
                                in the 'versions' set. Why? because for now all we need
                                is a p4model to help us navigate those fields. With a
                                compatable model we would be blind. Let's just grab the
                                latest in the list
                        '''
                        # query1 = (lambda rec: rec.table == table)
                        # query2 = (lambda rec: rec.numFields == (len(fields) + 4))
                        # query3 = (lambda rec: rec.tableVersion == journaltableversion)
                        # drecords = DLGRecordSet(datarecords)(*[query1, query2, query3]).select(
                        #    orderby=['table', 'release'])
                        # results.merge({table: [record.release for record in drecords]})
                except StopIteration:
                    EOR = True

            versions = objectify(self.schemaversions)
            for result in results:
                resl = Lst(results[result])
                if (len(resl) > 0):
                    versions = versions.intersect(resl)
            return versions