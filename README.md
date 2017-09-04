# CSV to Lineprotocol

This is a simple tool to convert CSV files to Lineprotocol files which are readable by InfluxDB or Kapacitor.
The CSV file to read should be formatted like:
```
timestamp0,measurement0
timestamp1,measurement1
timestamp2,measurement2
...
```

The available flags are:
```
  -db string
    	database name (only for replay file) (default "mydb")
  -importFile
    	create an influx import file (.txt)
  -input string
    	CSV file or directory to convert (default "path/to/file.csv")
  -output string
    	output directory (default "converted/")
  -replay
    	create a replay file (.srpl)
  -rp string
    	database retention policy (only for replay file) (default "autogen")
  -table string
    	table name (default "table_test")
  -time
    	Add a fake timestamp to the data
```
The converted `.txt` file can be imported by InfluxDB with `influx -import -path=FILENAME.txt` and `.srpl` replay files can be imported by Kapacitor (have to be moved to Kapacitors recordings directory)