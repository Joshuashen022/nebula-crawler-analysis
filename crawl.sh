# start a dry run crawl
# ./dist/nebula --dry-run crawl 

# start a crawl and store results in a json file
# ./dist/nebula --json-out ./results/ crawl --neighbors

# not working
# ./dist/nebula --json-out ./results/ crawl --bootstrap-peers https://probelab.io/tools/nebula/

# start a crawl on db
./dist/nebula --db-user joshua --db-name nebula_local crawl --neighbors

# ./dist/nebula  --db-user joshua --db-name nebula_local monitor

caffeinate -disu python3 src/start.py