# Raw CI mgbench results (single iteration each, QPS)

Source: `Release / Benchmark` job logs of benchmark-only Diff `workflow_dispatch` runs (`release_benchmark=true`, all else false). S = signal set, C = control set.

| set | query | pre-#4577 `88c90e77c`<br>(doctor-doom) | #4577 `1233eabb6`<br>(doctor-doom) | E0 master `8902f6768`<br>(doctor-doom) | E1 revert `f9361d25c`<br>(firebird) | E2 DoRead probe `e359aa182`<br>(firebird) |
|---|---|---:|---:|---:|---:|---:|
| C | arango/allshortest_paths | 2079.1 | 2101.6 | 2066.3 | 2057.9 | 2021.4 |
| C | arango/expansion_2 | 918.4 | 925.7 | 918.3 | 899.1 | 922.2 |
| C | arango/expansion_2_with_filter | 1426.5 | 1435.3 | 1435.1 | 1385.4 | 1426.0 |
| C | arango/expansion_3 | 74.4 | 74.5 | 74.3 | 73.1 | 75.7 |
| C | arango/neighbours_2 | 965.9 | 975.9 | 974.3 | 954.4 | 958.4 |
| C | arango/neighbours_2_with_data_and_filter | 880.2 | 887.8 | 895.0 | 874.1 | 870.0 |
| C | arango/neighbours_2_with_filter | 1499.1 | 1522.1 | 1518.6 | 1483.5 | 1506.5 |
| C | arango/shortest_path_with_filter | 3834.4 | 3830.6 | 3890.2 | 3889.2 | 3943.1 |
| S | arango/expansion_1_with_filter | 17623.0 | 17198.8 | 16940.9 | 17432.3 | 17267.4 |
| S | arango/shortest_path | 5707.6 | 5579.1 | 5621.3 | 5662.8 | 5661.2 |
| S | arango/single_edge_write | 19947.4 | 19595.0 | 19186.7 | 19833.2 | 19721.5 |
| S | arango/single_vertex_read | 25857.3 | 24908.0 | 24899.0 | 26496.2 | 26324.8 |
| S | arango/single_vertex_write | 23602.9 | 23138.1 | 22495.8 | 23255.4 | 23214.9 |
| S | create/pattern | 28248.8 | 26726.9 | 27150.5 | 27796.0 | 27068.0 |
| S | create/vertex | 30417.7 | 30033.5 | 30285.7 | 31689.1 | 30687.1 |
| S | create/vertex_big | 22026.0 | 21500.4 | 21503.6 | 21720.0 | 22163.6 |
| S | match/pattern_long | 21362.1 | 20774.0 | 20983.8 | 21453.8 | 21025.5 |
| S | match/pattern_short | 23137.7 | 22856.8 | 22889.8 | 23065.8 | 23052.5 |
| S | match/vertex_on_label_property_index | 26338.0 | 25830.1 | 25175.3 | 25793.8 | 25390.2 |
| S | match/vertex_on_property | 25642.3 | 24922.8 | 25138.2 | 25142.6 | 25309.4 |
|  | aggregation/count | 164.0 | 165.1 | 146.7 | 115.1 | 109.7 |
|  | aggregation/min_max_avg | 63.4 | 67.0 | 68.7 | 67.2 | 55.7 |
|  | arango/aggregate | 122.1 | 119.0 | 127.4 | 117.2 | 123.8 |
|  | arango/aggregate_with_distinct | 126.1 | 128.3 | 129.4 | 124.3 | 130.4 |
|  | arango/aggregate_with_filter | 96.2 | 94.6 | 93.4 | 91.1 | 94.3 |
|  | arango/expansion_1 | 15556.4 | 15407.6 | 15106.6 | 15385.3 | 15652.5 |
|  | arango/expansion_3_with_filter | 77.3 | 76.0 | 75.7 | 74.7 | 77.1 |
|  | arango/expansion_4 | 3.1 | 3.2 | 3.2 | 3.1 | 3.2 |
|  | arango/expansion_4_with_filter | 1.8 | 1.8 | 1.7 | 1.7 | 1.7 |
|  | arango/neighbours_2_with_data | 524.9 | 516.4 | 510.2 | 503.2 | 506.3 |
|  | arango/unwind_range_vertex_write | 3354.7 | 3327.4 | 3577.2 | 3663.2 | 3694.6 |
|  | create/edge | 22153.7 | 23587.8 | 21898.1 | 22218.7 | 22061.5 |
|  | match/pattern_cycle | 12834.0 | 12539.5 | 12295.0 | 12572.8 | 12588.3 |
|  | match/vertex_on_label_property | 123.3 | 121.6 | 130.6 | 118.4 | 120.3 |
|  | update/vertex_on_property | 194.6 | 192.4 | 169.3 | 163.1 | 172.7 |

## Pairwise summary (signal median / control median / signal − control)

| comparison | signal | control | S − C |
|---|---:|---:|---:|
| #4577 `1233eabb6` vs pre-#4577 `88c90e77c` (doctor-doom vs doctor-doom) | -2.3% | +0.8% | **-3.1%** |
| E0 master `8902f6768` vs pre-#4577 `88c90e77c` (doctor-doom vs doctor-doom) | -3.0% | +0.7% | **-3.8%** |
| E0 master `8902f6768` vs #4577 `1233eabb6` (doctor-doom vs doctor-doom) | +0.1% | -0.2% | **+0.3%** |
| E1 revert `f9361d25c` vs E0 master `8902f6768` (firebird vs doctor-doom) | +2.4% | -2.1% | **+4.5%** |
| E2 DoRead probe `e359aa182` vs E0 master `8902f6768` (firebird vs doctor-doom) | +1.1% | -0.7% | **+1.8%** |
| E2 DoRead probe `e359aa182` vs E1 revert `f9361d25c` (firebird vs firebird) | -0.6% | +1.5% | **-2.1%** |
