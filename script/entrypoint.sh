#!/usr/bin/env bash
# Our script to donwload secrets
[ -z "${ENVIRONMENT}" ] && eval $@ && exit 0;

aws configure set region us-east-1

account_alias=$(aws iam list-account-aliases | jq --raw-output '.AccountAliases[0]')

aws s3 cp s3://agentiq-demo4-secrets/aiq-airflow-encrypted.env aiq-airflow-encrypted.env

aws kms decrypt --ciphertext-blob fileb://aiq-airflow-encrypted.env --output text --query Plaintext | base64 -d > decrypted.env

source ./decrypted.env

eval $@ 
