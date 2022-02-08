# https://docs.influxdata.com/influxdb/v2.1/api/
using HTTP, JSON3, DataFrames, Dates, CSV

import Base.write

struct InfluxServer
    url::String
    org::String
    token::String
    function InfluxServer(url::String, org::String, token::String)
        resp = HTTP.get(url*"/ping"; status_exception = false, connect_timeout = 3, readtimeout = 5)
        if !(resp.status in (200, 204))
            println("Could not ping influxdb server. Response: $(String(resp.body))")
        end
        return new(url, org, token)
    end
end


"""
    writetable(influx::InfluxServer, bucket::String, measurement, table; precision::Union{Symbol, String} = :ms)

Write a table to the influx database. Fields and tags need to be
specified using prefixes \"f_\" and \"t_\" respectively. A column namd timestamp needs to
be provided which contains Int or DateTime entries.
"""
function writetable(influx::InfluxServer, bucket::String, measurement, table; precision::Union{Symbol, String} = :ms)
    buffer = table2lineprotocol(measurement, table, precision = precision)
    return write(influx, bucket, buffer; precision = precision)
end


"""
    writelineprotocol(influx::InfluxServer, bucket::String, linep::String; precision::Union{Symbol, String} = :ms)

Write data using the lineprotocol.
"""
function writelineprotocol(influx::InfluxServer, bucket::String, linep::String; precision::Union{Symbol, String} = :ms)
    return write(influx, bucket, IOBuffer(linep); precision = precision)
end


function write(influx::InfluxServer, bucket::String, body::IOBuffer; precision::Union{Symbol, String} = :ms )
    url = influx.url*"/api/v2/write?org="*HTTP.escape(influx.org)*"&bucket="*HTTP.escape(bucket)*"&precision="*string(precision)
    # TODO add gzip possibility
    headers = ["Authorization"=>"Token "*influx.token, "Accept"=>"application/json", "Content-Encoding"=>"identity", "Content-Type"=>"text/plain"]
    HTTP.post(url, headers, body)
end


listbuckets(influx::InfluxServer) = fluxquery(influx, "buckets()")
listmeasurements(influx::InfluxServer, bucket::String) = fluxquery(influx, "import \"influxdata/influxdb/schema\"\nschema.measurements(bucket: \"$bucket\")")
function listfields(influx::InfluxServer, bucket::String, measurement::String)
    querystring = "import \"influxdata/influxdb/schema\"\n
    schema.measurementFieldKeys(\n
    bucket: \"$bucket\",\n
    measurement: \"$measurement\")"
    return fluxquery(influx, querystring)
end


"""
    listorgs(influx::InfluxServer)

Return tuples containing the name and id of the organisations.
"""
function listorgs(influx::InfluxServer)
    url = influx.url*"/api/v2/orgs"
    headers = ["Authorization"=>"Token "*influx.token]
    resp =  HTTP.get(url, headers)
    jsonobj = JSON3.read(resp.body)
    return [(o.name, o.id) for o in jsonobj.orgs]
end


function getorgid(influx::InfluxServer)
    url = influx.url*"/api/v2/orgs?org="*HTTP.escape(influx.org)
    headers = ["Authorization"=>"Token "*influx.token]
    resp =  HTTP.get(url, headers)
    jsonobj = JSON3.read(resp.body)
    return jsonobj.orgs[1].id
end


"""
    fluxquery(influx::InfluxServer, querystring::String)

Query data and return a DataFrame for each table returned by InfluxDB.
"""
function fluxquery(influx::InfluxServer, querystring::String)
    url = influx.url*"/api/v2/query?org="*HTTP.escape(influx.org)
    # TODO add gzip possibility
    headers = ["Authorization"=>"Token "*influx.token, "Content-Type"=>"application/json", "Accept"=>"application/csv", "Accept-Encoding"=>"identity"]
    body = "{\"query\":"*JSON3.write(querystring)*",\"dialect\":{\"annotations\": [\"datatype\"],\"commentPrefix\": \"#\", \"dateTimeFormat\": \"RFC3339\"}}"
    resp = HTTP.post(url, headers, body)
    return parseinfluxresp( String(resp.body) )
end


"""
    simplequery(influx::InfluxServer, bucket::String, measurement::String, fields::Vector{String}, timerange::Tuple{DateTime, DateTime}; tags = nothing)
"""
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
        i = iterate(tags)
        (k, v) = i[1]
        write(querybuffer, "|>filter(fn: (r)=>r[\"$(string(k))\"]==\"$(string(v))\"")
        i = iterate(tags, i[2])
        while !isnothing(i)
            (k, v) = i[1]
            write(querybuffer, " or r[\"$(string(k))\"]==\"$(string(v))\"")
            i = iterate(tags, i[2])
        end
        write(querybuffer, ")\n")
    end
    write(querybuffer, "|>group(columns: [\"_field\"], mode: \"by\")")
    write(querybuffer, "|>sort(columns: [\"_time\"])")
    return fluxquery(influx, String(take!(querybuffer)))
end



"""
    parseinfluxresp(respstr::AbstractString)

Parses the annotated CSV table responses from Influx.
Return a DataFrame for each table.
Annotation needs to be "datatype".
"""
function parseinfluxresp(respstr::AbstractString)
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


ping(influx::InfluxServer) = HTTP.get(influx.url*"/ping"; connect_timeout = 3, readtimeout = 5)
