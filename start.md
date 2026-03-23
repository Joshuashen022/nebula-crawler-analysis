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
pg_dump -U joshua -d nebula_local > nebula_backup.sql

docker exec -t nebula-postgres pg_dump -U joshua nebula_local > nebula_backup.sql
scp -i ../.ssh/crawler.pem root@8.216.32.203:~/results_bundle.tar ~/Downloads/
```


local data:
https://bafybeico6jh7tqh4iavib73lv6mgk263vq4wmb727frskv34okdgwt2lc4.ipfs.dweb.link?filename=results_local.tar.gz
QmTepp4mfnTRAfTjW2ytV1VoSv9hE8tLDUDDpb3nby6CWr
server data:
https://bafybeichuhco23no4oeaqc25kti6vgj4mi2nvv3wu5qffi4wm6om5uv5wq.ipfs.dweb.link?filename=results_server_server.tar
QmTAGj3FAJVMbpVbCuRXwJGX1r5wJLX6kDxvwDy8cGwVN3