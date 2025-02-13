import re
from collections import deque
from functools import reduce
from pprint import pprint

from libfs.fsFileIO import ispath
from libdlg.dlgStore import ZDict, Lst
from libdlg.dlgUtilities import bail
from libdlg.dlgControl import DLGControl
from libdlg.contrib.prettytable.prettytable import PrettyTable

'''  [$File: //dev/p4dlg/libdlg/dlgSearch.py $] [$Change: 467 $] [$Revision: #6 $]
     [$DateTime: 2024/08/24 08:15:42 $]
     [$Author: zerdlg $]
'''

__all__ = ['Search']

''' USAGE:

    >>> oSearch = Search()                                          # reference to class Search
    >>> source = '/home/gc/client_workspace/files/filename'         # path to a file
    >>> terms = ['hobbit']                                          # the search terms
    >>> search_results = oSearch(source, *terms)                    # Search is callable and take 2 args: source, terms
    >>> pprint(search_results)                                      # the following or dictionaries of results

    [{'context': 'The hobbit-hole in question belongs to Bilbo Baggins, a very '
                 'respected hobbit.',
     'id': 1,
     'score': 0.5773502691896258,
     'terms': ['hobbit']},
    {'context': 'Bilbo returns to his comfortable hobbit-hole, he is a different '
                'person altogether, well',
     'id': 21,
     'score': 0.2773500981126146,
     'terms': ['hobbit']},
    ...]

    >>> oSearch.printer(search_results)                             # view the result is an ASCII table

    +------------------+-------------------+--------------------+------------------------------------------------------------------------------------------+
    | [__ID/IDX/LINE_] | [__SEARCH_TERM__] | [__SCORE__]        | [__LINE__]                                                                               |
    +------------------+-------------------+--------------------+------------------------------------------------------------------------------------------+
    | 1                | ['hobbit']        | 0.5773502691896258 | The hobbit-hole in question belongs to Bilbo Baggins, a very respected hobbit.           |
    | 21               | ['hobbit']        | 0.2773500981126146 | Bilbo returns to his comfortable hobbit-hole, he is a different person altogether, well  |
    | 9                | ['hobbit']        | 0.2672612419124244 | No such luck however; soon 13 fortune-seeking dwarves have arrived on the hobbit's       |
    | 4                | ['hobbit']        | 0.2672612419124244 | to (which they take 6 times if they can). Certainly this particular hobbit is the        |
    | 18               | ['hobbit']        | 0.2581988897471611 | spring. Though The Hobbit is lighter in tone than the trilogy that follows, it has, like |
    +------------------+-------------------+--------------------+------------------------------------------------------------------------------------------+ 
'''

regex_word = re.compile(u'[\w\-\<\>\(\)]+')

class Search(DLGControl):
    def __init__(
            self,
            objp4=None,
            maxrows=None,
            **kwargs
    ):
        kwargs = ZDict(kwargs)
        self.maxrows = maxrows or 20
        self.mergeitems = lambda a, b: (a + b)
        self.objp4 = objp4
        if (self.objp4 is None):
            self.objp4 = ZDict()
        loglevel = kwargs.loglevel \
            if (self.objp4 is None) \
            else self.objp4.loglevel or 'DEBUG'
        loggername = Lst(__name__.split('.'))(-1)
        super(Search, self).__init__(
            loggername=loggername,
            loglevel=loglevel
        )
        super(Search, self).__init__(loggername, loglevel)

    def parse(self, text):
        if (type(text) is bytes):
            text = text.decode('utf8')
        return regex_word.findall(text.lower())

    def get_words(self, item):
        return self.parse(item) \
            if (isinstance(item, str)) \
            else reduce(
                        self.mergeitems,
                        [
                            self.get_words(i) for i in item
                         ]
                        ) \
            if (isinstance(item, list)) \
            else reduce(self.mergeitems, item.values()) \
            if (isinstance(item, dict)) \
            else self.parse(repr(item))

    def subwords(self, word, d=3):
        n = len(word)
        for i in range(
                0,
                (n - d + 1)
        ):
            for j in range(
                    (i + d),
                    (n + 1)
            ):
                if (j > i):
                    yield word[i:j]

    def bin(self, words, subs=True):
        (
            hist,
            parents
        ) = \
            (
                ZDict(),
                ZDict()
            )
        for word in set(words):
            hist[word] = ((hist[word] or 0) + 1)
            for subword in self.subwords(word) \
                    if (subs is True) \
                    else [word]:
                parents[subword] = (parents[subword] or []) + [word]
        norm = (sum((c * c) for c in hist.getvalues()) ** 0.5)
        if (norm > 0.0):
            norm = (norm ** (-1))
        return (hist, parents, norm)

    def create_index(self, items):
        (
            index,
            docinfo
        ) = \
            (
                ZDict(),
                ZDict()
            )
        for (idx, item) in enumerate(items):
            words = self.get_words(item)
            (hist, parents, norm) = self.bin(words)
            docinfo[idx] = (
                hist,
                parents,
                norm
            )
            for subword in parents:
                index[subword] = ((index[subword] or []) + [idx])
        return index, docinfo

    def parseresults(
            self,
            index,
            docinfo,
            terms
    ):
        acc = ZDict()
        (hist, parents, norm) = self.bin(terms, subs=False)
        '''  so they cannot be repeated
        '''
        for key in parents:
            indexkey = index[key] or []
            for number in indexkey:
                count1 = (norm * hist[key])
                (dhist, dparents, dnorm) = docinfo[number]
                count2 = (dnorm * sum(dhist[word] for word in dparents[key]))
                acc[number] = ((acc[number] or 0.0) + (count1 * count2))
        '''
        results = Lst()
        recordcounter = 0
        for (number, relevance) in iter(acc.items()):
            recordcounter += 1
            if (self.maxrows is not None):
                if (recordcounter <= self.maxrows):
                    results.append(Lst([relevance, number]))
                else:
                    break
            else:
                results.append([relevance, number])
        '''
        results = Lst(
            Lst(
                [relevance, number]) for (number, relevance) in iter(acc.items())
        )
        results.sort(reverse=True)
        return results

    def parser(self, terms):
        words = self.parse(terms)
        self.last_search = words
        return self.parseresults(
            self.index,
            self.docinfo,
            words
        )

    def printer(self, results):
        cols = [
            '[__IDX/LINE_]',
            '[__SEARCH_TERM__]',
            '[__SCORE__]',
            '[__CONTEXT__]'
        ]
        vecTable = PrettyTable(cols)
        vecTable.align = 'l'
        [vecTable.add_row(result.getvalues()) for \
         result in results if (len(results) > 0)]
        print(vecTable)

    def expand_results(self, results):
        keys = [
            'idx',
            'terms',
            'score',
            'context'
        ]
        records = Lst()
        init_results = Lst(
            [
                result(0),
                self.last_search,
                result(1)
            ] for result in results
        )
        if (len(results) > 0):
            for result in init_results:
                result = Lst(result)
                try:
                    (
                        record,
                        idx,
                        context
                    ) = \
                        (
                            [
                                result(1),
                                result(0)
                            ],
                            result(2),
                            self.source(result(2))
                    )
                    context = context.lstrip().rstrip()
                    record.insert(0, idx)
                    record.insert(3, context)
                    searchrecord = ZDict(zip(keys, record))
                    records.append(searchrecord)
                except Exception as err:
                    bail(err)
        return records

    def __call__(
            self,
            source,
            *sTerms,
            key='data'
    ):
        ''' search terms
        '''
        sTerms = Lst(sTerms)
        ''' search source
        '''
        if (len(source) == 0):
            bail(
                'invalid source: can not execute a search on nothing...'
            )
        if (isinstance(source, (list, tuple, deque, set))):
            ''' arrays of strings
            '''
            source = Lst(source)
            if (len(source) == 1):
                source = source(0)
        if (isinstance(source, str)):
            if (ispath(source) is True):
                oFile = open(source, 'r')
                try:
                    source = Lst(oFile.readlines())
                finally:
                    oFile.close()
            elif ('\n' in source):
                source = Lst(source.split('\n'))
        elif (hasattr(source, 'readlines')):
            source = Lst(source.readlines())
        elif (
                (isinstance(source(0), dict)) &
                (key is not None)
        ):
            source = Lst([' '.join(str(v) for v in i[key]) for i in source])
        elif (isinstance(source, (list, tuple, deque, set))):
            source = Lst(source)
        elif (isinstance(source, dict)):
            source = Lst([' '.join(str(v) for v in i[key]) for i in source])
        else:
            source = Lst()
        self.source = source
        ''' build source index
        '''
        (self.index, self.docinfo) = self.create_index(self.source)
        ''' search terms
        '''
        sTerms = '' if (len(sTerms) == 0) else sTerms(0) \
            if (len(sTerms) == 1) \
            else ' '.join(sTerms)
        ''' parsed - a list of [score, idx/line#] pairs    
        '''
        parsed = self.parser(sTerms)
        ''' initial search results - [idx/line, score, context]
        '''
        search_results = self.expand_results(parsed)
        if (self.maxrows is not None):
            search_results = search_results[0: self.maxrows]
        return search_results


def main():
    LOTR_excerpt = '''
The hobbit-hole in question belongs to Bilbo Baggins, a very respected hobbit.
He is, like most of his kind, well off, well fed, and best pleased when sitting
by his own fire with a pipe, a glass of good beer, and a meal to look forward
to (which they take 6 times if they can). Certainly this particular hobbit is the
last person one would expect to see set off on a hazardous journey; indeed, when
the wizard Gandalf the Grey stops by one morning, "looking for someone to share in
an adventure," Baggins fervently wishes the wizard elsewhere.

No such luck however; soon 13 fortune-seeking dwarves have arrived on the hobbit's
doorstep in search for a master burglar, and before he can even grab his hat,
handkerchiefs or even an umbrella, Bilbo Baggins is swept out his door and into
a dangerous and long adventure. The dwarves' goal is to return to their ancestral
home in the Lonely Mountains and reclaim a stolen fortune from the dragon Smaug.
Along the way, they and their reluctant companion meet giant spiders, hostile elves,
ravening wolves--and, most perilous of all, a subterranean creature named Gollum
from whom Bilbo wins a magical ring in a riddling contest. It is from this life-or-death
game in the dark that J.R.R. Tolkien's masterwork, The Lord of the Rings, would eventually
spring. Though The Hobbit is lighter in tone than the trilogy that follows, it has, like
Bilbo Baggins himself, unexpected iron at its core. Don't be fooled by its fairy-tale demeanor;
this is very much a story for adults, though older children will enjoy it, too. By the time
Bilbo returns to his comfortable hobbit-hole, he is a different person altogether, well
primed for the bigger adventures to come and so is the reader.'''

    """---  SEARCH

                SOURCE can be:
                        text
                        file
                        file(-like) object
                        list of lines
                        list of revs

                * a search object is callable and take the following keyword args (and default values):

                %>>> SOURCE = LOTR_excerpt
                %>>> TERMS = "bilbo hobbit" or ["bilbo","hobbit"]
                %>>> results = Search(SOURCE)(*TERMS,**{'showid': True, 'showcontext': True, 'display': True})

                +--------------+-----------------------+----------------+-------------------------------------------------------------------------------------------------+
                | [__ID/IDX__] | [__SEARCH_TERM__]     | [__SCORE__]    | [__LINE__]                                                                                      |
                +--------------+-----------------------+----------------+-------------------------------------------------------------------------------------------------+
                | 1            | [u'bilbo', u'hobbit'] | 0.612372435696 | The hobbit-hole in question belongs to Bilbo Baggins, a very respected hobbit.                  |
                | 21           | [u'bilbo', u'hobbit'] | 0.392232270276 | Bilbo returns to his comfortable hobbit-hole, he is a different person altogether, well         |
                | 16           | [u'bilbo', u'hobbit'] | 0.188982236505 | from whom Bilbo wins a magical ring in a riddling contest. It is from this life-or-death        |
                | 11           | [u'bilbo', u'hobbit'] | 0.188982236505 | handkerchiefs or even an umbrella, Bilbo Baggins is swept out his door and into                 |
                | 9            | [u'bilbo', u'hobbit'] | 0.188982236505 | No such luck however; soon 13 fortune-seeking dwarves have arrived on the hobbit's              |
                | 4            | [u'bilbo', u'hobbit'] | 0.188982236505 | to (which they take 6 times if they can). Certainly this particular hobbit is the               |
                | 19           | [u'bilbo', u'hobbit'] | 0.182574185835 | Bilbo Baggins himself, unexpected iron at its core. Don't be fooled by its fairy-tale demeanor; |
                | 18           | [u'bilbo', u'hobbit'] | 0.182574185835 | spring. Though The Hobbit is lighter in tone than the trilogy that follows, it has, like        |
                +--------------+-----------------------+----------------+-------------------------------------------------------------------------------------------------+


                %>>> SOURCE = LOTR_excerpt
                %>>> TERMS = "master burglar"
                %>>> objSearch = Search(SOURCE)
                %>>> objSearch(TERMS)

                +--------------+-------------------------+----------------+--------------------------------------------------------------------------------------------+
                | [__ID/IDX__] | [__SEARCH_TERM__]       | [__SCORE__]    | [__LINE__]                                                                                 |
                +--------------+-------------------------+----------------+--------------------------------------------------------------------------------------------+
                | 10           | [u'master', u'burglar'] | 0.36514837167  | doorstep in search for a master burglar, and before he can even grab his hat,              |
                | 17           | [u'master', u'burglar'] | 0.182574185835 | game in the dark that J.R.R. Tolkien's masterwork, The Lord of the Rings, would eventually |
                +--------------+-------------------------+----------------+--------------------------------------------------------------------------------------------+
    """

    oSearch = Search()
    results = oSearch(LOTR_excerpt, *["bilbo", "hobbit"])
    pprint(results)
    print()
    oSearch.printer(results)
    print()
    search_results = oSearch(LOTR_excerpt, "bilbo hobbit")
    oSearch.printer(search_results)
    print()
    search_results = oSearch(LOTR_excerpt, *["gollum", "umbrella"])
    oSearch.printer(search_results)
    print()
    oSearch = Search()
    search_results = oSearch(LOTR_excerpt, "master burglar")
    oSearch.printer(search_results)

    """
    [{'context': 'The hobbit-hole in question belongs to Bilbo Baggins, a very '
             'respected hobbit.',
      'id': 1,
      'score': 0.6123724356957945,
      'terms': ['bilbo', 'hobbit']},
     {'context': 'Bilbo returns to his comfortable hobbit-hole, he is a different '
                 'person altogether, well',
      'id': 21,
      'score': 0.39223227027636803,
      'terms': ['bilbo', 'hobbit']},
     {'context': 'from whom Bilbo wins a magical ring in a riddling contest. It is '
                 'from this life-or-death',
      'id': 16,
      'score': 0.1889822365046136,
      'terms': ['bilbo', 'hobbit']},
     {'context': 'handkerchiefs or even an umbrella, Bilbo Baggins is swept out '
                 'his door and into',
      'id': 11,
      'score': 0.1889822365046136,
      'terms': ['bilbo', 'hobbit']},
     {'context': 'No such luck however; soon 13 fortune-seeking dwarves have '
                 "arrived on the hobbit's",
      'id': 9,
      'score': 0.1889822365046136,
      'terms': ['bilbo', 'hobbit']},
     {'context': 'to (which they take 6 times if they can). Certainly this '
                 'particular hobbit is the',
      'id': 4,
      'score': 0.1889822365046136,
      'terms': ['bilbo', 'hobbit']},
     {'context': "Bilbo Baggins himself, unexpected iron at its core. Don't be "
                 'fooled by its fairy-tale demeanor;',
      'id': 19,
      'score': 0.18257418583505533,
      'terms': ['bilbo', 'hobbit']},
     {'context': 'spring. Though The Hobbit is lighter in tone than the trilogy '
                 'that follows, it has, like',
      'id': 18,
      'score': 0.18257418583505533,
      'terms': ['bilbo', 'hobbit']}]

    +------------------+---------------------+---------------------+-------------------------------------------------------------------------------------------------+
    | [__ID/IDX/LINE_] | [__SEARCH_TERM__]   | [__SCORE__]         | [__CONTEXT__]                                                                                   |
    +------------------+---------------------+---------------------+-------------------------------------------------------------------------------------------------+
    | 1                | ['bilbo', 'hobbit'] | 0.6123724356957945  | The hobbit-hole in question belongs to Bilbo Baggins, a very respected hobbit.                  |
    | 21               | ['bilbo', 'hobbit'] | 0.39223227027636803 | Bilbo returns to his comfortable hobbit-hole, he is a different person altogether, well         |
    | 16               | ['bilbo', 'hobbit'] | 0.1889822365046136  | from whom Bilbo wins a magical ring in a riddling contest. It is from this life-or-death        |
    | 11               | ['bilbo', 'hobbit'] | 0.1889822365046136  | handkerchiefs or even an umbrella, Bilbo Baggins is swept out his door and into                 |
    | 9                | ['bilbo', 'hobbit'] | 0.1889822365046136  | No such luck however; soon 13 fortune-seeking dwarves have arrived on the hobbit's              |
    | 4                | ['bilbo', 'hobbit'] | 0.1889822365046136  | to (which they take 6 times if they can). Certainly this particular hobbit is the               |
    | 19               | ['bilbo', 'hobbit'] | 0.18257418583505533 | Bilbo Baggins himself, unexpected iron at its core. Don't be fooled by its fairy-tale demeanor; |
    | 18               | ['bilbo', 'hobbit'] | 0.18257418583505533 | spring. Though The Hobbit is lighter in tone than the trilogy that follows, it has, like        |
    +------------------+---------------------+---------------------+-------------------------------------------------------------------------------------------------+

    +------------------+---------------------+---------------------+-------------------------------------------------------------------------------------------------+
    | [__ID/IDX/LINE_] | [__SEARCH_TERM__]   | [__SCORE__]         | [__CONTEXT__]                                                                                   |
    +------------------+---------------------+---------------------+-------------------------------------------------------------------------------------------------+
    | 1                | ['bilbo', 'hobbit'] | 0.6123724356957945  | The hobbit-hole in question belongs to Bilbo Baggins, a very respected hobbit.                  |
    | 21               | ['bilbo', 'hobbit'] | 0.39223227027636803 | Bilbo returns to his comfortable hobbit-hole, he is a different person altogether, well         |
    | 16               | ['bilbo', 'hobbit'] | 0.1889822365046136  | from whom Bilbo wins a magical ring in a riddling contest. It is from this life-or-death        |
    | 11               | ['bilbo', 'hobbit'] | 0.1889822365046136  | handkerchiefs or even an umbrella, Bilbo Baggins is swept out his door and into                 |
    | 9                | ['bilbo', 'hobbit'] | 0.1889822365046136  | No such luck however; soon 13 fortune-seeking dwarves have arrived on the hobbit's              |
    | 4                | ['bilbo', 'hobbit'] | 0.1889822365046136  | to (which they take 6 times if they can). Certainly this particular hobbit is the               |
    | 19               | ['bilbo', 'hobbit'] | 0.18257418583505533 | Bilbo Baggins himself, unexpected iron at its core. Don't be fooled by its fairy-tale demeanor; |
    | 18               | ['bilbo', 'hobbit'] | 0.18257418583505533 | spring. Though The Hobbit is lighter in tone than the trilogy that follows, it has, like        |
    +------------------+---------------------+---------------------+-------------------------------------------------------------------------------------------------+

    +------------------+------------------------+---------------------+----------------------------------------------------------------------------------+
    | [__ID/IDX/LINE_] | [__SEARCH_TERM__]      | [__SCORE__]         | [__CONTEXT__]                                                                    |
    +------------------+------------------------+---------------------+----------------------------------------------------------------------------------+
    | 15               | ['gollum', 'umbrella'] | 0.21320071635561041 | ravening wolves--and, most perilous of all, a subterranean creature named Gollum |
    | 11               | ['gollum', 'umbrella'] | 0.1889822365046136  | handkerchiefs or even an umbrella, Bilbo Baggins is swept out his door and into  |
    +------------------+------------------------+---------------------+----------------------------------------------------------------------------------+

    +------------------+-----------------------+---------------------+--------------------------------------------------------------------------------------------+
    | [__ID/IDX/LINE_] | [__SEARCH_TERM__]     | [__SCORE__]         | [__CONTEXT__]                                                                              |
    +------------------+-----------------------+---------------------+--------------------------------------------------------------------------------------------+
    | 10               | ['master', 'burglar'] | 0.36514837167011066 | doorstep in search for a master burglar, and before he can even grab his hat,              |
    | 17               | ['master', 'burglar'] | 0.18257418583505533 | game in the dark that J.R.R. Tolkien's masterwork, The Lord of the Rings, would eventually |
    +------------------+-----------------------+---------------------+--------------------------------------------------------------------------------------------+
    """

if (__name__ == '__main__'):
   main()
