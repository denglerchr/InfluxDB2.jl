# https://docs.influxdata.com/influxdb/v2.1/api/
using HTTP, JSON3, DataFrames, Dates, CSV

import Base.write

struct InfluxServer
    url::String
    org::String
    token::String
end


function writetable(influx::InfluxServer, bucket::String, measurement, table; precision::Union{Symbol, String} = :ms)
    buffer = table2lineprotocol(measurement, table, precision = precision)
    return write(influx, bucket, buffer; precision = precision)
end


function write(influx::InfluxServer, bucket::String, body::IOBuffer; precision::Union{Symbol, String} = :ms )
    url = influx.url*"/api/v2/write?org="*influx.org*"&bucket="*bucket*"&precision="*string(precision)
    # TODO add gzip possibility
    headers = ["Authorization"=>"Token "*influx.token, "Accept"=>"application/json", "Content-Encoding"=>"identity", "Content-Type"=>"text/plain"]
    HTTP.post(url, headers, body)
end


function listbuckets(influx::InfluxServer)
    url = influx.url*"/api/v2/buckets?org="*influx.org
    headers = ["Authorization"=>"Token "*influx.token, "Accept"=>"application/json"]
    resp =  HTTP.get(url, headers)
    jsonobj = JSON3.read(resp.body)
    return [b.name for b in jsonobj.buckets]
end


function fluxquery(influx::InfluxServer, querystring::String)
    url = influx.url*"/api/v2/query?org="*influx.org
    # TODO add gzip possibility
    headers = ["Authorization"=>"Token "*influx.token, "Content-Type"=>"application/json", "Accept"=>"application/csv", "Accept-Encoding"=>"identity"]
    body = "{\"query\":"*JSON3.write(querystring)*",\"dialect\":{\"annotations\": [\"datatype\"],\"commentPrefix\": \"#\", \"dateTimeFormat\": \"RFC3339\"}}"
    resp = HTTP.post(url, headers, body)
    return parseinfluxresp( String(resp.body) )
end


function simplequery(influx::InfluxServer, bucket::String, measurement::String, fields::Vector{String}, timerange::Tuple{DateTime, DateTime}; tags = nothing)
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
    write(querybuffer, ")\n")
    if !isnothing(tags)
        (k, v) = first(tags)
        write(querybuffer, "|>filter(fn: (r)=>r[\"$(string(k))\"]==\"$(string(v))\"")
        for (k,v) in tags[2:end]
            write(querybuffer, " or (r)=>r[\"$(string(k))\"]==\"$(string(v))\"")
        end
        write(querybuffer, ")\n")
    end
    write(querybuffer, "|>group(columns: [\"_field\"], mode: \"by\")")
    return fluxquery(influx, String(take!(querybuffer)))
end



"""
Parses the annotated CSV table responses from Influx.
Return a DataFrame for each table.
Annotation needs to be "datatype".
"""
function parseinfluxresp(respstr)
    influxtypesdict = Base.ImmutableDict("string"=>String,"long"=>Int64, "boolean"=>Bool, "dateTime:RFC3339"=>String, "double"=>Float64)

    # split in tables and remove last empty entry
    tablesstr = split(respstr, "\r\n\r\n")[1:end-1]

    # Transform each table string into a DataFrame
    Out = Vector{DataFrame}(undef, length(tablesstr))
    for (itab, tab) in enumerate(tablesstr)
        rows = split(tab, "\r\n"; limit = 2)
        typesinflux = split(rows[1], ',')
        typesjulia = [get(influxtypesdict, tstr, String) for tstr in typesinflux]

        dframe = CSV.read(IOBuffer(rows[2]), DataFrame; types = typesjulia , drop = [1])
        # Correct DateTimes
        ind = findall(typesinflux.=="dateTime:RFC3339")
        for i in ind
            dframe[!,i-1] = parseRFC3339.(dframe[:,i-1])
        end
        Out[itab] = dframe
    end

    return Out
end

# A lot faster than using DateTime parser
function parseRFC3339(dtiso)
    strlen = length(dtiso)
    dtiso2 = ifelse( strlen<24, dtiso[1:end-1]*"0000", dtiso)
    year = parse(Int,dtiso2[1:4])
    month = parse(Int,dtiso2[6:7])
    day = parse(Int,dtiso2[9:10])
    hour = parse(Int,dtiso2[12:13])
    minute = parse(Int,dtiso2[15:16])
    second = parse(Int,dtiso2[18:19])
    millisecond = parse(Int,dtiso2[21:23])
    DateTime(year, month, day, hour, minute, second, millisecond)
end