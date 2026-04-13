# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

bind = '0.0.0.0:8959'
workers = 4
pidfile = 'pidfile'
errorlog = 'errorlog'
loglevel = 'info'
accesslog = 'accesslog'
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'
capture_output = True
enable_stdio_inheritance = True
certfile = '/etc/letsencrypt/live/bank-recon.borland.com.ph/fullchain.pem'
keyfile = '/etc/letsencrypt/live/bank-recon.borland.com.ph/privkey.pem'