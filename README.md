Feed-Fetcher
============

It is syncing RSS feed into sql table using a timestamped Model. The code can be setup using a cron. It will check the last successful fetch by reading the maximum timestamp for a particular feed and reading from the site for time greater than that. 
