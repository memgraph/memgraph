#!/bin/bash

if [ ! -d "websockify-js" ]; then
    git clone https://github.com/novnc/websockify-js.git
fi
cd websockify-js/websockify
pnpm install --frozen-lockfile
./websockify.js 9999 :7687
