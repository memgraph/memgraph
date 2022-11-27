set dgrid3d 30,30
set hidden3d
set label "pool size"   at 8, 11000, 0
set label "multiframe size"  at 20,6000,0
set label "execution time (ms)"  at 14,0,5000
splot "frames1.dat" u 1:2:3 with lines
pause mouse close
