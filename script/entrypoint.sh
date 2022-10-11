#!/usr/bin/env bash
# Our script to donwload secrets
[ -z "${ENVIRONMENT}" ] && eval $@ && exit 0;

aws configure set region us-east-1

#account_alias=$(aws iam list-account-aliases | jq --raw-output '.AccountAliases[0]')

aws s3 cp "s3://agentiq-${ENVIRONMENT}-secrets/aiq-airflow-encrypted.env" aiq-airflow-encrypted.env

aws kms decrypt --ciphertext-blob fileb://aiq-airflow-encrypted.env --output text --query Plaintext | base64 -d > decrypted.env

source ./decrypted.env

files=("webserver_config.py" "airflow.cfg")
echo "--- config file importing started ---"
for i in ${files[@]}; do
  aws s3 cp "s3://agentiq-${ENVIRONMENT}-secrets/${i}" encrypted-$i
  aws kms decrypt --ciphertext-blob fileb://encrypted-$i --output text --query Plaintext | base64 -d > $i
done
echo "--- config files importing completed ---"

eval $@ 
