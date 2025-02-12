apt-get update && apt-get install -y curl gnupg2 apt-transport-https unixodbc-dev
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update && apt-get install -y msodbcsql17
pip install -r requirements.txt

fastapi dev main.py
