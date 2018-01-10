#!/usr/bin/env bash
./daemon.py start scheduler
./daemon.py start topic_seeder
./daemon.py start weibo_seeder
./daemon.py start spider
