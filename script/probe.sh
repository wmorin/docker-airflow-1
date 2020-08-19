#!/usr/bin/env bash

STATUS=$(curl -sS https://dwh-airflow.prod.snapcart.ai/health | jq .scheduler.status);
if [[ $STATUS == "\"healthy\"" ]]; then
  exit 0;
else
  exit 1;
fi;