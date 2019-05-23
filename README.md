# Traffic Parser

Parser for *.csv.gz traffic files. Requires [node.js](https://nodejs.org) 12 or later.

Example usage:

    $ git clone https://github.com/bbar/traffic-parser.git traffic-parser
    $ cd traffic-parser
    $ yarn install # (or npm install)
    $ node parse.js \
        --sources="/some/path/a.csv.gz /some/path/b.csv.gz /some/path/c.csv.gz /some/path/d.csv.gz" \
        --destination="/some/path/data/parsed/intervals" \
        --batch=5000

|Argument|Required|Type|Default|Description|
|--|--|--|--|--|
|sources|yes|String||Files to parse|
|destination|yes|String||Directory where parsed files are placed|
|batch|no|Int|2000|Max lines written to any file at once|