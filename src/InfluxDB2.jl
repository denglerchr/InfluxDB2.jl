module InfluxDB2

include("lineprotocol.jl")
include("http.jl")
export InfluxServer, writetable, simplequery, fluxquery, delete

end # module
