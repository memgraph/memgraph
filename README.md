# Memgraph quality assurance

In order to test dressipi's queries agains memgraph the following commands have
to be executed:
    1. ./init [Dxyz] # downloads query implementations + memgraph
                     # (the auth is manually for now) + optionally user can
                     # define arcanist diff which will be applied on the
                     # memgraph source code
    2. ./run         # compiles and runs database instance, also runs the
                     # test queries

TODO: automate further
