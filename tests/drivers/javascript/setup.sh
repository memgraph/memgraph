#!/bin/bash

if [ ! -d "websockify-js" ]; then
    git clone https://github.com/novnc/websockify-js.git
fi
cd websockify-js/websockify
npm install
./websockify.js 9999 :7687
