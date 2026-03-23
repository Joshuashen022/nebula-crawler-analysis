```shell
# Install Python 3
brew install python
# Confirm it works
python3 --version

cd ~/code/project/phd/crawler

# Create virtual environment
python3 -m venv .venv

# Activate it
source .venv/bin/activate
# (you should now see (.venv) at the start of your terminal prompt)

# Install pandas
pip install pandas

export data
docker exec -t nebula-postgres pg_dump -U joshua nebula_local > nebula_backup.sql

scp -i ../.ssh/crawler.pem root@8.216.32.203:~/results_bundle.tar ~/Downloads/
```