"""
SUM

recs = jnl(~(jnl.rev.depotFile.len() > 64)).select(jnl.rev.change.sum())
recs = p4(~(p4.files.depotFile.len() > 64)).select(p4.files.change.sum())



"""