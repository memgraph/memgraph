# Read the PriorityThreadPool park state from a live/attached memgraph.
#   gdb bins/cls_pure/memgraph -p <PID> -batch -x scripts/park_probe.gdb
# Walks threads for a Worker::operator() frame (so memgraph:: types resolve), derives the pool
# address from Worker::productive_pending_, then prints the park counters. The question we want:
#   parked_admissions_ length == 0  -> the hung UNIQUE is ORPHANED (never parked / lost after a kick)
#   parked_admissions_ length >  0  -> it IS parked and the monitor kick is failing to rescue it
set sysroot /
set auto-load safe-path /
set pagination off
python
import gdb
def find_frame(pred):
    for thr in gdb.inferiors()[0].threads():
        try: thr.switch()
        except Exception: continue
        f = gdb.newest_frame()
        while f is not None:
            nm = f.name() or ''
            if pred(nm):
                f.select(); return (thr, f)
            try: f = f.older()
            except Exception: break
    return (None, None)

# The REAL worker frame's name STARTS WITH the operator; the condition_variable::wait frame merely
# mentions it as a template argument, so anchor on the prefix.
thr, w = find_frame(lambda nm: nm.startswith('memgraph::utils::PriorityThreadPool::Worker::operator'))
print('=== worker frame:', (w.name()[:70] if w else 'NOT FOUND'))
if w is not None:
    pool = None
    # `this->productive_pending_` is a raw int64* into the pool; the hot_threads ref param is often
    # optimized out, so this is the reliable handle. Subtract the member offset to get the pool base.
    for expr, memb in (('(long)this->productive_pending_', 'productive_pending_'),
                       ('(long)&hot_threads', 'hot_threads_')):
        try:
            addr = int(gdb.parse_and_eval(expr))
            off  = int(gdb.parse_and_eval('(long)&((memgraph::utils::PriorityThreadPool*)0)->%s' % memb))
            pool = addr - off
            print('derived pool via %-18s -> %s (member off %d)' % (memb, hex(pool), off))
            break
        except gdb.error as e:
            print('  (%s failed: %s)' % (memb, e))
    if pool is not None:
        base = '((memgraph::utils::PriorityThreadPool*)%d)->' % pool
        def rd(e):
            try: return gdb.parse_and_eval(e)
            except gdb.error as x: return 'ERR: %s' % x
        print('productive_pending    =', rd('%sproductive_pending_._M_i' % base))
        print('has_parked_           =', rd('%shas_parked_._M_base._M_i' % base))
        print('draining_admissions_  =', rd('%sdraining_admissions_._M_base._M_i' % base))
        st = rd('%sparked_admissions_._M_impl._M_start._M_cur' % base)
        fi = rd('%sparked_admissions_._M_impl._M_finish._M_cur' % base)
        empty = (str(st) == str(fi))
        print('parked_admissions_    = %s (start=%s finish=%s)' % ('EMPTY' if empty else 'NON-EMPTY', st, fi))
        if not empty:
            print('  -> parked entries present; dumping deque:')
            print(rd('%sparked_admissions_' % base))
end
