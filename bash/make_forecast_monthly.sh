#!/bin/bash
. /etc/default/puppet

export PYTHONPATH="/home/d.kurlov/contact-center-dataflow"

/usr/bin/python3 /home/d.kurlov/contact-center-dataflow/ETL/make_forecast_monthly.py
