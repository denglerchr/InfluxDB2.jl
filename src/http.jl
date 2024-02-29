# https://docs.influxdata.com/influxdb/v2.1/api/
using HTTP, JSON3, DataFrames, Dates, CSV, CodecZlib

import Base.write, Base.show

struct InfluxServer
    url::String
    org::String
    token::String
    function InfluxServer(url::String, org::String, token::String)

        # remove tailing / if existing 
        if endswith(url, '/')
            url = chop(url)
        end
        influx = new(url, org, token)

        # try pinging it
        resp = ping(influx)
        if resp.status != 204
            @warn("Ping status, expected 204, got $(resp.status)")
        end
        # TODO check version, but receive empty string?
        #headers = Dict(resp.headers)
        #headers["X-Influxdb-Version"] < v"2.0" && @warn "..."
        return influx
    end
end

show(io::IO, ::MIME"text/plain", influx::InfluxServer) = print(io, "InfluxServer(url:\"$(influx.url)\",org:\"$(influx.org)\",token:\"$(influx.token[1:4])...\")")
show(io::IO, influx::InfluxServer) = show(io::IO, MIME("text/plain"), influx::InfluxServer)


"""
    writetable(influx::InfluxServer, bucket::String, table; precision::Union{Symbol, String} = :ms, compression::Symbol = :identity)

Write a table to the influx database. Fields and tags need to be
specified using prefixes \"f_\" and \"t_\" respectively. A column namd "measurement" and "timestamp" need to
be provided. The "timestamp" column can contain Int or DateTime entries.
Compression can be set to :gzip to compress the line protocol before sending.
"""
function writetable(influx::InfluxServer, bucket::String, table; precision::Union{Symbol, String} = :ms, compression::Symbol = :identity)
    buffer = table2lineprotocol(table, precision = precision)
    resp = write(influx, bucket, buffer; precision = precision, compression = compression)
    close(buffer)
    return resp
end


"""
    writelineprotocol(influx::InfluxServer, bucket::String, linep::String; precision::Union{Symbol, String} = :ms, compression::Symbol = :identity)

Write data using the lineprotocol.
Compression can be set to :gzip to compress the line protocol before sending.
"""
function writelineprotocol(influx::InfluxServer, bucket::String, linep::String; precision::Union{Symbol, String} = :ms, compression::Symbol = :identity)
    buffer = IOBuffer(linep)
    resp = write(influx, bucket, buffer; precision = precision, compression = compression)
    close(buffer)
    return resp
end


function write(influx::InfluxServer, bucket::String, body::IOBuffer; precision::Union{Symbol, String} = :ms, compression::Symbol = :identity)
    @assert(compression in (:identity, :gzip), "\"compression\" must be either :identity or :gzip.")
    url = influx.url*"/api/v2/write?org="*HTTP.escapeuri(influx.org)*"&bucket="*HTTP.escapeuri(bucket)*"&precision="*string(precision)
    headers = ["Authorization"=>"Token "*influx.token, "Accept"=>"application/json", "Content-Encoding"=>string(compression), "Content-Type"=>"text/plain"]

    if compression == :gzip
        # gzip compression before sending
        compressed_body = GzipCompressorStream(body)
        return HTTP.post(url, headers, compressed_body)

    else
        # no compression
        return HTTP.post(url, headers, body)
    end
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
    url = influx.url*"/api/v2/orgs?org="*HTTP.escapeuri(influx.org)
    headers = ["Authorization"=>"Token "*influx.token]
    resp =  HTTP.get(url, headers)
    jsonobj = JSON3.read(resp.body)
    return jsonobj.orgs[1].id
end

"""
    delete(influx::InfluxServer, timerange::Tuple{DateTime, DateTime}, predicate::String = "")

Delete data in InfluxDB in a given timerange. Predicate can be used to specify which data to delete. Quotes need to be escaped properly in *predicate*. 
E.g.: _measurement=\"example-measurement\" AND exampleTag=\"exampleTagValue\"
"""
function delete(influx::InfluxServer, bucket::String, timerange::Tuple{DateTime, DateTime}, predicate::String = "")
    url = influx.url*"/api/v2/delete?org="*HTTP.escapeuri(influx.org)*"&bucket="*HTTP.escapeuri(bucket)
    headers = ["Authorization"=>"Token "*influx.token]

    bodydict = Dict{String, String}(["start"=>string( timerange[1] ) * "Z", "stop"=>string( timerange[2] ) * "Z"])
    !isempty(predicate)  && ( bodydict["predicate"] = predicate )
    body = JSON3.write(bodydict)
    return HTTP.post(url, headers, body)
end


"""
    fluxquery(influx::InfluxServer, querystring::String; compression::Symbol = :identity)

Query data and return a DataFrame for each table returned by InfluxDB.
"""
function fluxquery(influx::InfluxServer, querystring::String; compression::Symbol = :identity)
    @assert(compression in (:identity, :gzip), "\"compression\" must be either :identity or :gzip.")
    url = influx.url*"/api/v2/query?org="*HTTP.escapeuri(influx.org)
    headers = ["Authorization"=>"Token "*influx.token, "Content-Type"=>"application/json", "Accept"=>"application/csv", "Accept-Encoding"=>string(compression)]

    body = "{\"query\":"*JSON3.write(querystring)*",\"dialect\":{\"annotations\": [\"datatype\"],\"commentPrefix\": \"#\", \"dateTimeFormat\": \"RFC3339\"}}"
    resp = HTTP.post(url, headers, body)
    
    # Check if we received teh encoding we asked for (no Content-Encoding returned for identity)
    resp_encoding = Symbol( get( Dict(resp.headers), "Content-Encoding", "identity") )
    if resp_encoding != compression
        @warn "Received Content-Encoding $resp_encoding but asked for $compression"
    end

    # Decompress answer if necessary
    if resp_encoding == :gzip
        influxans = String(transcode(GzipDecompressor, resp.body))
    else
        influxans = String(resp.body)
    end

    return parseinfluxresp( influxans )
end


"""
    simplequery(influx::InfluxServer, bucket::String, measurement::String, fields::Vector{String}, timerange::Tuple{DateTime, DateTime}; tags = nothing, compression::Symbol = :identity)
"""
function simplequery(influx::InfluxServer, bucket::String, measurement::String, fields::Vector{String}, timerange::Tuple{DateTime, DateTime}; tags = nothing, compression::Symbol = :identity)
    @assert length(fields)>0
    from = Dates.format(timerange[1], "yyyy-mm-ddTHH:MM:SS.sZ")
    to = Dates.format(timerange[2], "yyyy-mm-ddTHH:MM:SS.sZ")

    # Create flux query
    querybuffer = IOBuffer()
    Base.write(querybuffer, "from(bucket: \"$bucket\")\n")
    Base.write(querybuffer, "|>range(start: $from, stop: $to)\n")
    Base.write(querybuffer, "|>filter(fn: (r) => r[\"_measurement\"] == \"$measurement\")\n")
    Base.write(querybuffer, "|>filter(fn: (r) =>r[\"_field\"] == \"$(fields[1])\"")
    for f in fields[2:end]
        Base.write(querybuffer, " or r[\"_field\"] == \"$f\"")
    end
    Base.write(querybuffer, ")\n")
    if !isnothing(tags)
        i = iterate(tags)
        (k, v) = i[1]
        Base.write(querybuffer, "|>filter(fn: (r)=>r[\"$(string(k))\"]==\"$(string(v))\"")
        i = iterate(tags, i[2])
        while !isnothing(i)
            (k, v) = i[1]
            Base.write(querybuffer, " or r[\"$(string(k))\"]==\"$(string(v))\"")
            i = iterate(tags, i[2])
        end
        Base.write(querybuffer, ")\n")
    end
    Base.write(querybuffer, "|>drop(columns: [\"_measurement\", \"_start\", \"_stop\"])")
    Base.write(querybuffer, "|>group(columns: [\"_field\"], mode: \"by\")")
    Base.write(querybuffer, "|>sort(columns: [\"_time\"])")
    return fluxquery(influx, String(take!(querybuffer)); compression = compression)
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
    Out = Vector{DataFrame}(undef, 0)
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
        dfsgrouped = groupby(dframe, "table")
        append!(Out, [DataFrame(df) for df in dfsgrouped])
    end

    return Out
end

# A lot faster than using DateTime parser
function parseRFC3339(dtiso::String)
    strlen = length(dtiso)
    strlen < 20 && error("Invalid DateTime string.")
    dtiso2 = ifelse( strlen==20, dtiso[1:end-1]*".000", dtiso[1:end-1]*"000")
    year = parse(Int,dtiso2[1:4])
    month = parse(Int,dtiso2[6:7])
    day = parse(Int,dtiso2[9:10])
    hour = parse(Int,dtiso2[12:13])
    minute = parse(Int,dtiso2[15:16])
    second = parse(Int,dtiso2[18:19])
    millisecond = parse(Int,dtiso2[21:23])
    DateTime(year, month, day, hour, minute, second, millisecond)
end


ping(influx::InfluxServer) = HTTP.get(influx.url*"/ping"; status_exception = false, connect_timeout = 3, readtimeout = 5)
