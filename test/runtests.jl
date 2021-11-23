using CSV, DataFrames, InfluxDB2, Random

# Lineprotocol
testtable = CSV.File(joinpath(@__DIR__, "testtable.csv"))
testtable = CSV.File(joinpath(@__DIR__, "testtable_notag.csv"))
testtable = CSV.File(joinpath(@__DIR__, "testtable_missing.csv"))

iobuffer = table2lineprotocol("testmeasurement", testtable; precision = :ms)
print(String(take!(iobuffer)))


# Create InfluxDB2 object for following operations
token = "mROH4PvnsKOaDRbZfFH6u5wLxGRg1ZGPdAuE-pjNWZ80pX2FUYSkNeLH53hPhA1UIMOiWjokPT_2E0ZkT3LGxA=="
influx = InfluxServer("http://localhost:8086", "Home", token)

# Writing
ndata = 10
testtable = DataFrame(("timestamp"=>[now(UTC)-Second(ndata)+Second(i) for i = 1:ndata], "f_Floatfield"=>randn(ndata), "f_Intfield"=>rand(1:10, ndata), "f_Stringfield"=>[randstring(5) for _ =1:ndata], "f_Boolfield"=>rand((false, true), ndata), "t_tag1"=>[rand(["hello", "world"]) for _ =1:ndata]))
measurementname = "juliatest"
bucket = "Test"
resp = writetable(influx, bucket, measurementname, testtable)

# Reading
querystring = "from(bucket: \"Test\")
|> range(start: -3d)
|> filter(fn: (r) => r[\"_measurement\"] == \"juliatest\")"

data = fluxquery(influx, querystring)

querystring = "from(bucket: \"Test\")
  |> range(start: -7d)
  |> filter(fn: (r) => r[\"_measurement\"] == \"juliatest\")
  |> filter(fn: (r) => r[\"_field\"] == \"b\" or r[\"_field\"] == \"c\" or r[\"_field\"] == \"e\")
  |> yield(name: \"mean\")"