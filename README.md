# InfluxDB2.jl
Simple reading from and writing to InfluxDB v2 .

## Writing
Create a Tables.jl object, e.g., a DataFrame. The Table needs to have a column "timestamp", columns with names that start with "f_" will be used as fields ("f_" is removed before writing), and columns starting with "t_" are used as tags ("t_" is removed).

Example
```julia
org = ...
token = ...
influx = InfluxServer("http://localhost:8086", org, token)

data = DataFrame(Dict("timestamp" = [now(UTC)-Second(1), now(UTC)], "f_somefield" = [1.0, 2.0]))
writetable(influx, data)
```

## Reading
Queries return a DataFrame for each table in the response.
Two possibilities for queries
1) `simplequery` does not require the flux language but is quite limited. Returns a dataframe for each field.
2) writing a flux query string and using `fluxquery`

Example:
```julia
org = ...
token = ...
influx = InfluxServer("http://localhost:8086", org, token)

# A query 
bucket = "testbucket"
measurement = "mymeasurement"
fields = ["somefield", "anotherfield"]
utcfrom = now(UTC)-Day(2)
utcto = now(UTC)
dataframes = simplequery(influx, bucket, measurement, fields, (utcfrom, utcto); tags = ["sometag"=>"TagValue"])

# The same query using the flux language
querystring = "from(bucket: \"testbucket\")
|>range(start: $(Dates.format(now(UTC)-Day(2), "yyyy-mm-ddTHH:MM:SS.sZ")), stop: $(Dates.format(now(UTC), "yyyy-mm-ddTHH:MM:SS.sZ")))
|>filter(fn: (r) => r[\"_measurement\"] == \"mymeasurement\")
|>filter(fn: (r) =>r[\"_field\"] == \"somefield\" or r[\"_field\"] == \"anotherfield\")
|>filter(fn: (r) =>r[\"sometag\"] == \"TagValue\")
|>group(columns: [\"_field\"], mode: \"by\")"
dataframes = fluxquery(influx, querystring)
```