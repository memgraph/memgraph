#!/bin/bash

# DownloadProject
git clone https://github.com/Crascit/DownloadProject
download_project_tag="master"
cd DownloadProject
git checkout ${download_project_tag}
cd ..
