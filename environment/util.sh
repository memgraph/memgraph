#!/bin/bash

operating_system() {
    grep -E '^(VERSION_)?ID=' /etc/os-release | sort | cut -d '=' -f 2- | sed 's/"//g' | paste -s -d '-'
}
