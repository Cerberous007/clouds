#!/bin/bash
. /etc/default/puppet

export PYTHONPATH="/home/d.kurlov/contact-center-dataflow"

/usr/bin/python3 /home/d.kurlov/contact-center-dataflow/ETL/populate_contact_center_usedesk_tickets.py
