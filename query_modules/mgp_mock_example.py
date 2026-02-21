# https://memgraph.com/docs/custom-query-modules/python
# changes_logger.py OR can be directly pasted into the Lab
import mgp_mock as mgp

@mgp.read_proc
def log(
  context: mgp.ProcCtx,
  data: mgp.Nullable[mgp.Any]
) -> mgp.Record(done=bool):
  # https://memgraph.com/docs/custom-query-modules/python/python-api#logger-objects
  logger = mgp.Logger()
  for changes in data:
    for item in changes:
      logger.info(str(item))
  return mgp.Record(done=True)

log(mgp.ProcCtx, ["bla"])
