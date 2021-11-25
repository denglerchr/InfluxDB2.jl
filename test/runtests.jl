using CSV, DataFrames, InfluxDB2, Random, Dates

# Lineprotocol
testtable = CSV.File(joinpath(@__DIR__, "testtable.csv"))
testtable = CSV.File(joinpath(@__DIR__, "testtable_notag.csv"))
testtable = CSV.File(joinpath(@__DIR__, "testtable_missing.csv"))

iobuffer = table2lineprotocol("testmeasurement", testtable; precision = :ms)
print(String(take!(iobuffer)))


# Create InfluxDB2 object for following operations
token = read(joinpath(@__DIR__, "token.txt"), String)
#"mROH4PvnsKOaDRbZfFH6u5wLxGRg1ZGPdAuE-pjNWZ80pX2FUYSkNeLH53hPhA1UIMOiWjokPT_2E0ZkT3LGxA=="
influx = InfluxServer("http://localhost:8086", "home", token)

# Writing
ndata = 10
testtable = DataFrame(Dict("timestamp"=>[now(UTC)-Second(ndata)+Second(i) for i = 1:ndata], "f_Floatfield"=>randn(ndata), "f_Intfield"=>rand(1:10, ndata), "f_Stringfield"=>[randstring(5) for _ =1:ndata], "f_Boolfield"=>rand((false, true), ndata), "t_tag1"=>[rand(["hello", "world"]) for _ =1:ndata]))
measurementname = "juliatest"
bucket = "test"
resp = writetable(influx, bucket, measurementname, testtable)

# Reading
data = simplequery(influx, bucket, measurementname, ["Floatfield", "Intfield"], (now(UTC)-Day(2), now(UTC)); tags = ["tag1"=>"hello"])


querystring = "from(bucket: \"test\")
|>range(start: $(Dates.format(now(UTC)-Day(2), "yyyy-mm-ddTHH:MM:SS.sZ")), stop: $(Dates.format(now(UTC), "yyyy-mm-ddTHH:MM:SS.sZ")))
|>filter(fn: (r) => r[\"_measurement\"] == \"juliatest\")
|>filter(fn: (r) =>r[\"_field\"] == \"Floatfield\" or r[\"_field\"] == \"Intfield\")
|>filter(fn: (r) =>r[\"tag1\"] == \"hello\")
|>group(columns: [\"_field\"], mode: \"by\")"

data = fluxquery(influx, querystring)

