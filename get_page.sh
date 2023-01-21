rm -rf test.html 
#curl 'https://www.avvo.com/attorneys/36067-al-bradley-hawley-4308504.html' -H 'User-Agent: wut' -H 'Accept-Language: en-GB,en;q=0.5' -H 'Referer: https://www.avvo.com' -H 'TE: trailers' --output test.html
curl -v -L 'https://www.avvo.com/all-lawyers/sitemap.xml?page=0' --http1.0 -H 'User-Agent: notanagent' -H 'Accept-Language: en-GB,en;q=0.5'  -H 'Upgrade-Insecure-Requests: 1' -H 'TE: trailers' --output test.html

