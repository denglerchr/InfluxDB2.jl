using HTTP, JSON3

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
    seekstart(body)
    HTTP.post(url, headers, body)
end


function listbuckets(influx::InfluxDB2)
    url = influx.url*"/api/v2/buckets?org="*influx.org
    headers = ["Authorization"=>"Token "*influx.token, "Accept"=>"application/json"]
    resp =  HTTP.get(url, headers)
    jsonobj = JSON3.read(resp.body)
    return [b.name for b in jsonobj.buckets]
end