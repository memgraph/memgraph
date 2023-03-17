import mgp
import mgp_mock
import test_utils


@mgp.read_proc
def compare_apis(ctx: mgp.ProcCtx) -> mgp.Record(results_dict=mgp.Map):
    results = dict()

    record = mgp.Record(a=1, b=2.0, c="3")
    mock_record = mgp_mock.Record(a=1, b=2.0, c="3")

    results["fields"] = test_utils.all_equal(
        record.fields,
        mock_record.fields,
    )

    return mgp.Record(results_dict=results)
