# Traffic Parser

Parser for *.csv.gz traffic files. Requires [node.js](https://nodejs.org) 12 or later.

Example usage:

    $ git clone https://github.com/bbar/traffic-parser.git traffic-parser
    $ cd traffic-parser
    $ yarn install # (or npm install)
    $ node parse.js \
        --sources="/some/path/a.csv.gz /some/path/b.csv.gz /some/path/c.csv.gz /some/path/d.csv.gz" \
        --destination="/some/path/data/parsed/intervals" \
        --batch=625

|Argument|Required|Type|Default|Description|
|--|--|--|--|--|
|sources|yes|String||Files to parse|
|destination|yes|String||Directory where parsed files are placed|
|batch|no|Int|625|Max lines written to any file at once|

A quick note about the **batch** argument... A value of 0 will disable it completely, but this means you'll be keeping the entire parsed file contents in memory until the last line of the file is read. If this doesn't cause V8 to explode with a memory error, that means you're running this on a Godzilla machine. Similarly, if you set a batch limit of `30000` or so and haven't specifically increased node's limit with `--max-old-space-size`, you'll run into the same issue. Surprisingly (to me, anyway) setting a larger batch size doesn't mean better performance. I started at `20000` and kept cutting it in half until I saw the best performance, which was around 625. This means the code will parse 625 lines from a `*.csv.gz` file, write those to disk, then parse another 625 lines, write to disk, ..., until it's done.