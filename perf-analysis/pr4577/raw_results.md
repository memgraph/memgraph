# Raw CI mgbench results (single iteration each, QPS)

Benchmark-only Diff `workflow_dispatch` runs (`release_benchmark=true`). S = signal set, C = control set.

| set | query | pre-#4577 `88c90e77c`<br>(doctor-doom) | #4577 `1233eabb6`<br>(doctor-doom) | E0 master run1<br>(doctor-doom) | E0 master run2<br>(firebird) | E1 revert `f9361d25c`<br>(firebird) | E2 DoRead probe `e359aa182`<br>(firebird) |
|---|---|---:|---:|---:|---:|---:|---:|
| C | arango/allshortest_paths | 2079.1 | 2101.6 | 2066.3 | 2075.1 | 2057.9 | 2021.4 |
| C | arango/expansion_2 | 918.4 | 925.7 | 918.3 | 921.5 | 899.1 | 922.2 |
| C | arango/expansion_2_with_filter | 1426.5 | 1435.3 | 1435.1 | 1429.8 | 1385.4 | 1426.0 |
| C | arango/expansion_3 | 74.4 | 74.5 | 74.3 | 74.8 | 73.1 | 75.7 |
| C | arango/neighbours_2 | 965.9 | 975.9 | 974.3 | 973.5 | 954.4 | 958.4 |
| C | arango/neighbours_2_with_data_and_filter | 880.2 | 887.8 | 895.0 | 901.8 | 874.1 | 870.0 |
| C | arango/neighbours_2_with_filter | 1499.1 | 1522.1 | 1518.6 | 1525.3 | 1483.5 | 1506.5 |
| C | arango/shortest_path_with_filter | 3834.4 | 3830.6 | 3890.2 | 3892.7 | 3889.2 | 3943.1 |
| S | arango/expansion_1_with_filter | 17623.0 | 17198.8 | 16940.9 | 17128.1 | 17432.3 | 17267.4 |
| S | arango/shortest_path | 5707.6 | 5579.1 | 5621.3 | 5635.3 | 5662.8 | 5661.2 |
| S | arango/single_edge_write | 19947.4 | 19595.0 | 19186.7 | 19589.9 | 19833.2 | 19721.5 |
| S | arango/single_vertex_read | 25857.3 | 24908.0 | 24899.0 | 25145.1 | 26496.2 | 26324.8 |
| S | arango/single_vertex_write | 23602.9 | 23138.1 | 22495.8 | 22660.1 | 23255.4 | 23214.9 |
| S | create/pattern | 28248.8 | 26726.9 | 27150.5 | 27152.6 | 27796.0 | 27068.0 |
| S | create/vertex | 30417.7 | 30033.5 | 30285.7 | 29592.8 | 31689.1 | 30687.1 |
| S | create/vertex_big | 22026.0 | 21500.4 | 21503.6 | 21668.6 | 21720.0 | 22163.6 |
| S | match/pattern_long | 21362.1 | 20774.0 | 20983.8 | 20868.9 | 21453.8 | 21025.5 |
| S | match/pattern_short | 23137.7 | 22856.8 | 22889.8 | 22649.2 | 23065.8 | 23052.5 |
| S | match/vertex_on_label_property_index | 26338.0 | 25830.1 | 25175.3 | 24875.3 | 25793.8 | 25390.2 |
| S | match/vertex_on_property | 25642.3 | 24922.8 | 25138.2 | 25365.0 | 25142.6 | 25309.4 |
|  | aggregation/count | 164.0 | 165.1 | 146.7 | 128.9 | 115.1 | 109.7 |
|  | aggregation/min_max_avg | 63.4 | 67.0 | 68.7 | 64.5 | 67.2 | 55.7 |
|  | arango/aggregate | 122.1 | 119.0 | 127.4 | 127.3 | 117.2 | 123.8 |
|  | arango/aggregate_with_distinct | 126.1 | 128.3 | 129.4 | 128.1 | 124.3 | 130.4 |
|  | arango/aggregate_with_filter | 96.2 | 94.6 | 93.4 | 93.3 | 91.1 | 94.3 |
|  | arango/expansion_1 | 15556.4 | 15407.6 | 15106.6 | 15538.1 | 15385.3 | 15652.5 |
|  | arango/expansion_3_with_filter | 77.3 | 76.0 | 75.7 | 76.1 | 74.7 | 77.1 |
|  | arango/expansion_4 | 3.1 | 3.2 | 3.2 | 3.2 | 3.1 | 3.2 |
|  | arango/expansion_4_with_filter | 1.8 | 1.8 | 1.7 | 1.7 | 1.7 | 1.7 |
|  | arango/neighbours_2_with_data | 524.9 | 516.4 | 510.2 | 506.7 | 503.2 | 506.3 |
|  | arango/unwind_range_vertex_write | 3354.7 | 3327.4 | 3577.2 | 3583.7 | 3663.2 | 3694.6 |
|  | create/edge | 22153.7 | 23587.8 | 21898.1 | 21654.0 | 22218.7 | 22061.5 |
|  | match/pattern_cycle | 12834.0 | 12539.5 | 12295.0 | 12233.5 | 12572.8 | 12588.3 |
|  | match/vertex_on_label_property | 123.3 | 121.6 | 130.6 | 125.2 | 118.4 | 120.3 |
|  | update/vertex_on_property | 194.6 | 192.4 | 169.3 | 180.7 | 163.1 | 172.7 |

## Pairwise summary

| comparison | machines | signal | control | S − C |
|---|---|---:|---:|---:|
| #4577 `1233eabb6` vs pre-#4577 `88c90e77c` | doctor-doom / doctor-doom | -2.3% | +0.8% | **-3.1%** |
| E0 master run1 vs pre-#4577 `88c90e77c` | doctor-doom / doctor-doom | -3.0% | +0.7% | **-3.8%** |
| E0 master run1 vs #4577 `1233eabb6` | doctor-doom / doctor-doom | +0.1% | -0.2% | **+0.3%** |
| E1 revert `f9361d25c` vs E0 master run1 | firebird / doctor-doom | +2.4% | -2.1% | **+4.5%** |
| E1 revert `f9361d25c` vs E0 master run2 | firebird / firebird | +2.1% | -2.3% | **+4.4%** |
| E2 DoRead probe `e359aa182` vs E0 master run1 | firebird / doctor-doom | +1.1% | -0.7% | **+1.8%** |
| E2 DoRead probe `e359aa182` vs E0 master run2 | firebird / firebird | +1.3% | -0.8% | **+2.0%** |
| E2 DoRead probe `e359aa182` vs E1 revert `f9361d25c` | firebird / firebird | -0.6% | +1.5% | **-2.1%** |
