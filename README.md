#### Getting started

1. Configure `config.json` with your email and password
2. Ensure that Java 8 or higher is installed
3. Ensure that Maven is installed
4. Create an executable jar file using Maven:
````
mvn package
````
5. Run the jar file (take note of the filename, which is static):
````
java -jar ./target/TravelTimeExample-jar-with-dependencies.jar
````
6. A xlsx file should be downloaded containing one sheet that corresponds to each travel time query in `example_params.json`.
7. (OPTIONAL) If you want the output in a different format (csv, or json), you can specify this with the `--format` argument:
```
java -jar ./target/TravelTimeExample-jar-with-dependencies.jar --format csv
```
csv and json formats will return a .zip file containg the reports, instead of a single spreadsheet.
8. (OPTIONAL) If you want to specify a params file other than `example_params.json`, you can use the `--params` argument:
```
java -jar ./target/TravelTimeExample-jar-with-dependencies.jar --params other_params.json
```

#### Parameters

Any number of travel time queries can be made by a single job.
Each travel time query is for one segment, for one time period, with travel times aggregated over a given interval.

`ul_node_id` is the unique identifier UrbanLogiq assigns to segments.


  The parameters in `example_params.json` are structured as follows:

```
example_params = [
    # Example Query 1
    # segment: Wellington Street SB between King Street and Horton Street (ul_node_id="058kjDnuc+CDAfyiLei+Cw==")
    # interval: 1 hour
    # time period: Fri Feb 5 2021 19:00:00 EST (London time) -  Sat Feb 06 2021 18:00:00 EST (London time)
    {
        "ul_node_id": "058kjDnuc+CDAfyiLei+Cw==",  # This will be mapped to the segment name in the segments.csv file generated previously
        "start_time": 1612569600000,  # UTC milliseconds, Sat Feb 06 2021 00:00:00 UTC -- this is Fri Feb 5 2021 19:00:00 EST (London time)
        "end_time": 1612652400000,  # UTC milliseconds, Sat Feb 06 2021 23:00:00 UTC -- this is Sat Feb 06 2021 18:00:00 EST (London time)
        "interval": 3600,  # number of seconds -- this indicates that data should be aggregated over 1 hour intervals.
    },
    # Example Query 2
    # segment: Hamilton Road EB between Egerton Street and Highbury Avenue
    # interval: 15 minutes
    # time period: Fri Feb 5 2021 19:00:00 EST (London time) -  Sat Feb 06 2021 18:00:00 EST (London time)
    {
        "ul_node_id": "acGBlAS6mOixck/4YKSMwA==",  # This will be mapped to the segment name in the segments.csv file generated previously
        "start_time": 1612569600000,  # UTC milliseconds, Sat Feb 06 2021 00:00:00 UTC -- this is Fri Feb 5 2021 19:00:00 EST (London time)
        "end_time": 1612652400000,  # UTC milliseconds, Sat Feb 06 2021 23:00:00 UTC -- this is Sat Feb 06 2021 18:00:00 EST (London time)
        "interval": 900,  # number of seconds -- this indicates that data should be aggregated over 15-minute intervals.
    },
]
```
