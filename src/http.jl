# https://docs.influxdata.com/influxdb/v2.1/api/
using HTTP, JSON3, CSV

import Base.write

struct InfluxDB2
    url::String
    org::String
    token::String
end


function writetable(influx::InfluxDB2, bucket::String, measurement, table; precision::Union{Symbol, String} = :ms)
    buffer = table2lineprotocol(measurement, table, precision = precision)
    return write(influx, bucket, buffer; precision = precision)
end


function write(influx::InfluxDB2, bucket::String, body::IOBuffer; precision::Union{Symbol, String} = :ms )
    url = influx.url*"/api/v2/write?org="*influx.org*"&bucket="*bucket*"&precision="*string(precision)
    # TODO add gzip possibility
    headers = ["Authorization"=>"Token "*influx.token, "Accept"=>"application/json", "Content-Encoding"=>"identity", "Content-Type"=>"text/plain"]
    HTTP.post(url, headers, body)
end


function listbuckets(influx::InfluxDB2)
    url = influx.url*"/api/v2/buckets?org="*influx.org
    headers = ["Authorization"=>"Token "*influx.token, "Accept"=>"application/json"]
    resp =  HTTP.get(url, headers)
    jsonobj = JSON3.read(resp.body)
    return [b.name for b in jsonobj.buckets]
end


function fluxquery(influx::InfluxDB2, querystring::String)
    url = influx.url*"/api/v2/query?org="*influx.org
    # TODO add gzip possibility
    headers = ["Authorization"=>"Token "*influx.token, "Content-Type"=>"application/json", "Accept"=>"application/csv", "Accept-Encoding"=>"identity"]
    body = "{\"query\":"*JSON3.write(querystring)*",\"dialect\":{\"annotations\": [\"datatype\"],\"commentPrefix\": \"#\"}}"
    # ",\"dialect\":{\"annotations\": [\"datatype\"],\"commentPrefix\": \"#\",\"dateTimeFormat\": \"RFC3339\",\"delimiter\": \",\",\"header\": true}}"
    JSON3.pretty(body)
    resp = HTTP.post(url, headers, body)
    return String(resp.body)#CSV.File( IOBuffer(resp.body) )
end

function query(influx::InfluxDB2, bucket::String, measurement::String, fields::Vector{String}, timerange::Tuple{DateTime, DateTime})
    @assert length(fields)>0
    from = Dates.format(timerange[1], "yyyy-mm-ddTHH:MM:SS.sZ")
    to = Dates.format(timerange[2], "yyyy-mm-ddTHH:MM:SS.sZ")

    # Create flux query
    querybuffer = IOBuffer()
    write(querybuffer, "from(bucket: \"$bucket\")\n")
    write(querybuffer, "|>range(start: $from, stop: $to)\n")
    write(querybuffer, "|>filter(fn: (r) => r[\"_measurement\"] == \"$measurement\")\n")
    write(querybuffer, "|>filter(fn: (r) =>r[\"_field\"] == \"$(fields[1])\"")
    for f in fields[2:end]
        write(querybuffer, " or r[\"_field\"] == \"$f\"")
    end
    write(querybuffer, ')')
    return fluxquery(influx, String(take!(querybuffer)))
end