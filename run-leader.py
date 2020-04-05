#!/usr/bin/env python3

import subprocess

if __name__ == "__main__":
    browser = subprocess.Popen("/opt/bin/start-selenium-standalone.sh")
    worker = subprocess.Popen(["python3", "/usr/leader/app.py", "--workerMode"],stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
