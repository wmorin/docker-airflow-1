SECRET_PATH=secret/cryoem/cryoem-daq--dev

etc:
	mkdir -p etc

passwords: etc
	# db
	vault kv get --field=root-password -format=json $(SECRET_PATH)/postgres > etc/root-password
	vault kv get --field=primary-password -format=json $(SECRET_PATH)/postgres > etc/primary-password
	vault kv get --field=user-password -format=json $(SECRET_PATH)/postgres > etc/password
	# redis
	vault kv get --field=redis-config -format=json $(SECRET_PATH)/redis > etc/redis-config
	# airflow
	vault kv get --field=fernet-key -format=json $(SECRET_PATH)/airflow > etc/fernet-key
	vault kv get --field=auth -format=json $(SECRET_PATH)/airflow > etc/auth

apply:
	kubectl apply -k .
