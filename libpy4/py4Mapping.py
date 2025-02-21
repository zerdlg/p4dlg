import re
from libdlg.dlgStore import Lst
from libdlg.dlgUtilities import bail


"""
    map = [
        '//depot/dir/file1 //clientname/depot/dir/file1',
        '//depot/dir/file2 //clientname/depot/dir/file2',
        '//depot/dir/file3 //clientname/depot/dir/file3',
        '//depot/dir/subdir/... //clientname/depot/dir/subdir/...',
        ]
    othermap = [
        '//depot/dir/file2 //clientname/depot/dir/subdir/file2',
        '//depot/dir/file4 //otherclientname/depot/dir/file4',
        '//depot/dir/file5 //otherclientname/depot/dir/file5',
        '//depot/dir/file3 //ernie/depot/dir/subdir/file3',
        '//depot/dir/file6 //otherclientname/depot/dir/file6',
        '//depot/dir/projdir/... //bert/depot/dir/projdir/...',
    ]

    >>> newmapping = P4Mapping(map)(othermap)

    ['//depot/dir/file1 //clientname/depot/dir/file1',
     '//depot/dir/file2 //clientname/depot/dir/subdir/file2',
     '//depot/dir/file3 //clientname/depot/dir/subdir/file3',
     '//depot/dir/subdir/... //clientname/depot/dir/subdir/...',
     '//depot/dir/file4 //clientname/depot/dir/file4',
     '//depot/dir/file5 //clientname/depot/dir/file5',
     '//depot/dir/file6 //clientname/depot/dir/file6',
     '//depot/dir/projdir/... //clientname/depot/dir/projdir/...']
"""

'''  [$File: //dev/p4dlg/libpy4/py4Mapping.py $] [$Change: 609 $] [$Revision: #2 $]
     [$DateTime: 2025/02/21 03:36:09 $]
     [$Author: zerdlg $]
'''

__all__ = ['P4Mapping']

class P4Mapping(object):
    def __init__(self, map, *args, **kwargs):
        (
            self.src_mapping,
            self.src_mappings,
        ) = (
            self.getmappings(map)
        )

    def __call__(self, map):
        (
            omapping,
            omappings
        ) = (
            self.getmappings(map)
        )
        leftsides = [self.src_mappings[idxkey](0) for idxkey in self.src_mappings]
        src_clientname = self.clientname_from_clientfile(self.src_mappings[0](1))

        merged_mapping = self.src_mapping
        for oidx in omappings.keys():
            (
                oleft,
                oright
            ) = omappings[oidx]
            nmap = omapping[oidx]
            nright = oright
            oclientname = self.clientname_from_clientfile(oright)
            if (oclientname != src_clientname):
                nright = re.sub(oclientname, src_clientname, oright)
                nmap = f'{oleft} {nright}'
            if (oleft in leftsides):
                for i in self.src_mappings.keys():
                    if (
                            (self.src_mappings[i](0) == oleft) &
                            (self.src_mappings[i](1) != nright)
                    ):
                        merged_mapping.replace(self.src_mapping[i], nmap)
                        break
            else:
                merged_mapping.append(nmap)
        return merged_mapping

    def getmappings(self, mapping):
        mapping = Lst([mapping,]) \
            if (isinstance(mapping, str) is True) \
            else Lst(mapping)
        mappings = Lst(Lst(re.split('\s', mapline)) for
                        mapline in mapping).storageindex(reversed=True)
        return (mapping, mappings)

    def clientname_from_clientfile(self, clientfile):
        try:
            return Lst(re.split('/', clientfile)).clean()(0)
        except Exception as err:
            bail(err)