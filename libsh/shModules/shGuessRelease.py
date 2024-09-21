from libdlg.dlgStore import Lst
from libjnl.jnlGuess import GuessRelease

'''  [$File: //dev/p4dlg/libsh/shModules/shGuessRelease.py $] [$Change: 411 $] [$Revision: #5 $]
     [$DateTime: 2024/06/25 07:02:28 $]
     [$Author: mart $]
'''

__all__=['GuessVersion']

class GuessVersion(object):
    def __init__(self,schemaxmldocs='../schemaxml'):
        self.schemaxmldocs=schemaxmldocs

    def __call__(self,*args,**kwargs):
        '''
                * guessing p4 releases return's a list of versions
                * take the latest version (max) in the list
        '''
        args=Lst(args)
        oGuess=GuessRelease()
        if (args(0) is not None):
            probable_releases=oGuess.guess(args(0))
            likely_release=max(self.probable_releases)
            return (probable_releases,likely_release)
        return (Lst(),None)

def main():
    oRel=GuessVersion('../../schemaxml')()
    print(oRel.likely_release)
#if __name__=='__main__':main()