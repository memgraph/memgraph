set dgrid3d 30,30
set hidden3d
set label "shards"   at 20,-1,10000
set label "threads"  at -10,5,10000
splot "data.dat" u 1:2:3 with lines
pause mouse close
