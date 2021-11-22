using CSV, DataFrames, Random, Dates

ndata = 10000
data = DataFrame(("timestamp"=>[now()-Second(ndata)+Second(i) for i = 1:ndata], "f_Floatfield"=>randn(ndata), "f_Intfield"=>rand(1:10, ndata), "f_Stringfield"=>[randstring(5) for _ =1:ndata], "f_Boolfield"=>rand((false, true), ndata), "t_tag1"=>[rand(["hello", "world"]) for _ =1:ndata]))

CSV.write(joinpath(@__DIR__, "largetable.csv"), data)

temp = CSV.File(joinpath(@__DIR__, "largetable.csv"))
buffer =  table2lineprotocol("testmeasurement", data; precision = :ms)
write( joinpath(@__DIR__, "lineprotocol.txt"), take!(buffer))