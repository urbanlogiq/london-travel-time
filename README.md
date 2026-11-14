# Travel Time Example

This Java example requests travel times for London, Ontario. It sends one request with a start time, an end time, and an interval.
The CSV response contains one row for each road segment and interval with valid measurements.
Each row includes the average travel time in seconds and the average speed in kilometres per hour.

Change the sample values in `TravelTimeExample.main`. Times are milliseconds from 1970-01-01 00:00:00 UTC.
Both ends of the time range are included. The interval is in seconds; the sample value is one hour.
The program writes the CSV response without changing its contents.

## Getting started

1. Install Java 21 or later and Maven.
2. Install version `0.3.1` of the [UrbanLogiq SDK](https://github.com/urbanlogiq/ulsdk) in your local Maven repository.
3. Set these environment variables before you run the example:

   | Variable | Value |
   | --- | --- |
   | `USER_ID` | Your UrbanLogiq user ID |
   | `ACCESS_KEY` | Your access key ID |
   | `SECRET_KEY` | Your secret key |

   The key must permit access to the London travel time data in the Canadian production system.

4. Build the program:

   ```sh
   mvn package
   ```

5. Run the program and save the result:

   ```sh
   java -jar target/TravelTimeExample-jar-with-dependencies.jar > travel-time.csv
   ```
