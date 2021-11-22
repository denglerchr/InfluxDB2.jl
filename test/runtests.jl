using CSV, DataFrames, InfluxDB2

testtable = CSV.File(joinpath(@__DIR__, "testtable.csv"))
testtable = CSV.File(joinpath(@__DIR__, "testtable_notag.csv"))
testtable = CSV.File(joinpath(@__DIR__, "testtable_missing.csv"))

iobuffer = table2lineprotocol("testmeasurement", testtable; precision = :ms)
print(String(take!(iobuffer)))

token = "mROH4PvnsKOaDRbZfFH6u5wLxGRg1ZGPdAuE-pjNWZ80pX2FUYSkNeLH53hPhA1UIMOiWjokPT_2E0ZkT3LGxA=="
influx = InfluxDB2("http://localhost:8086", "Home", token)
measurementname = "juliatest"
bucket = "Test"
resp = writetable(influx, bucket, measurementname, testtable)