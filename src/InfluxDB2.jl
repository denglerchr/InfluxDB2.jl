module InfluxDB2

include("lineprotocol.jl")
include("http.jl")
export Client_v2, writetable, simplequery, fluxquery, delete

end # module
