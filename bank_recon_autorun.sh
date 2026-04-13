#!/bin/bash
screen -dmS br
screen -S br -X stuff 'cd /home/admin/apps/bank_recon/development/recon\n'
screen -S br -X stuff '. env/bin/activate\n'
screen -S br -X stuff 'python3 manage.py runserver_plus 0.0.0.0:8979 --cert-file /home/admin/apps/bank_recon/development/recon/core/.env/certificate.crt\n'