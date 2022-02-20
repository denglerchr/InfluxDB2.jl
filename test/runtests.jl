using CSV, DataFrames, InfluxDB2, Dates, Test, HTTP

# Lineprotocol
@testset "lineprotocol" begin
    testtable = CSV.File(joinpath(@__DIR__, "testtable.csv"))

    iobuffer = InfluxDB2.table2lineprotocol("testmeasurement", testtable; precision = :ms)
    correctlinep = "testmeasurement,c=hello b=2.0 1637496010000\ntestmeasurement,c=world b=4.0 1637496070000\ntestmeasurement,c=hello b=6.0 1637496130000\n"
    @test String(take!(iobuffer)) == correctlinep
end


if isfile(joinpath(@__DIR__, "influxsettings.txt"))
    token, org, bucket = readlines(joinpath(@__DIR__, "influxsettings.txt"))
    influx = InfluxServer("http://localhost:8086", org, token)
    measurementname = "juliatest"
    ndata = 10
    
    @testset "writing" begin
        # write without compression
        testtable = DataFrame(Dict("timestamp"=>[now(UTC)-Second(ndata)+Second(i) for i = 1:ndata], "f_Floatfield"=>randn(ndata), "f_Intfield"=>rand(1:10, ndata), "f_Stringfield"=>[rand(("String1", "String2", "String3")) for _ =1:ndata], "f_Boolfield"=>rand((false, true), ndata), "t_tag1"=>[rand(["hello", "world"]) for _ =1:ndata]))
        writeresp = writetable(influx, bucket, measurementname, testtable)
        @test writeresp.status == 204

        # write using gzip compression
        testtable2 = DataFrame(Dict("timestamp"=>[now(UTC)-Second(ndata*2)+Second(i) for i = 1:ndata], "f_Floatfield"=>randn(ndata), "f_Intfield"=>rand(1:10, ndata), "f_Stringfield"=>[rand(("String1", "String2", "String3")) for _ =1:ndata], "f_Boolfield"=>rand((false, true), ndata), "t_tag1"=>[rand(["hello", "world"]) for _ =1:ndata]))
        writeresp = writetable(influx, bucket, measurementname, testtable2; compression = :gzip)
        @test writeresp.status == 204
    end

    @testset "reading" begin
        # Query without compression
        data = simplequery(influx, bucket, measurementname, ["Floatfield", "Intfield", "Stringfield", "Boolfield"], (now(UTC)-Day(2), now(UTC)); tags = ["tag1"=>"hello", "tag2" =>"hi"])
        @test length(data) == 4 # we expect 4 tables, one for each field

        # Query foor gzip compressed data
        data = simplequery(influx, bucket, measurementname, ["Floatfield", "Intfield", "Stringfield", "Boolfield"], (now(UTC)-Day(2), now(UTC)); compression = :gzip)
        @test length(data) == 4
        @test size(data[1], 1) >= ndata

        querystring = "from(bucket: \"$bucket\")
        |>range(start: $(Dates.format(now(UTC)-Day(2), "yyyy-mm-ddTHH:MM:SS.sZ")), stop: $(Dates.format(now(UTC), "yyyy-mm-ddTHH:MM:SS.sZ")))
        |>filter(fn: (r) => r[\"_measurement\"] == \"$measurementname\")
        |>filter(fn: (r) =>r[\"_field\"] == \"Floatfield\" or r[\"_field\"] == \"Intfield\")
        |>filter(fn: (r) =>r[\"tag1\"] == \"hello\")
        |>drop(columns: [\"_start\", \"_stop\"])
        |>group(columns: [\"_field\"], mode: \"by\")
        |>sort(columns: [\"_time\"])"

        data = fluxquery(influx, querystring)
        @test length(data) == 2
    end
end
