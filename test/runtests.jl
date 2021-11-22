using CSV, DataFrames, InfluxDB2

# Lineprotocol
testtable = CSV.File(joinpath(@__DIR__, "testtable.csv"))
testtable = CSV.File(joinpath(@__DIR__, "testtable_notag.csv"))
testtable = CSV.File(joinpath(@__DIR__, "testtable_missing.csv"))

iobuffer = table2lineprotocol("testmeasurement", testtable; precision = :ms)
print(String(take!(iobuffer)))

# Writing
ndata = 10
testtable = DataFrame(("timestamp"=>[now(UTC)-Second(ndata)+Second(i) for i = 1:ndata], "f_Floatfield"=>randn(ndata), "f_Intfield"=>rand(1:10, ndata), "f_Stringfield"=>[randstring(5) for _ =1:ndata], "f_Boolfield"=>rand((false, true), ndata), "t_tag1"=>[rand(["hello", "world"]) for _ =1:ndata]))
token = "mROH4PvnsKOaDRbZfFH6u5wLxGRg1ZGPdAuE-pjNWZ80pX2FUYSkNeLH53hPhA1UIMOiWjokPT_2E0ZkT3LGxA=="
influx = InfluxDB2("http://localhost:8086", "Home", token)
measurementname = "juliatest"
bucket = "Test"
resp = writetable(influx, bucket, measurementname, testtable)

# Reading
querystring = "from(bucket: \"Test\")
|> range(start: -1h)
|> filter(fn: (r) => r[\"_measurement\"] == \"juliatest\")"

data = DataFrame( query(influx, querystring) )