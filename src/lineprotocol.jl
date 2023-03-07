# https://docs.influxdata.com/influxdb/v2.1/reference/syntax/line-protocol/

using Tables, Dates

const precisiondict = Base.ImmutableDict(:ns=>10.0^9, :us=>10.0^6, :ms=>10.0^3, :s=>1.0)

"""
    table2lineprotocol(datatable; precision = :ms)

Translate a Table into a line protocoll, required by InfluxDB. The table needs to contain columns "timestamp" and "measurement".
Returns an IOBuffer.
"""
function table2lineprotocol(datatable; precision::Union{Symbol, String} = :ms)

    # basic checks
    @assert Tables.istable(datatable)
    precision = Symbol(precision)
    @assert(precision in keys(precisiondict), "Precision must be one of :ns, :us, :ms, :s")

    # get column names and make sure they are ok
    colnames = Tables.columnnames(datatable)
    fieldnames = filter(x->startswith(string(x), "f_") && length(string(x))>2, colnames)

    tagnames = filter(x->startswith(string(x), "t_") && length(string(x))>2, colnames)
    for tname in tagnames
        T = Tables.columntype(datatable, tname)
        @assert(T<:Union{AbstractString, Missing}, "Tags must be of type String, got $T in tag $(string(tname)[3:end])")
    end

    # Check column types for timestamp and measurement
    @assert(:timestamp in colnames, "The table needs to have a column \"timestamp\".")
    @assert( Tables.columntype(datatable, :timestamp)<:Union{Dates.DateTime, Integer}, "The column \"timestamp\" has to be of eltype DateTime or Integer." )
    @assert(:measurement in colnames, "The table needs to have a column \"measurement\".")
    @assert( Tables.columntype(datatable, :measurement)<:AbstractString, "The column \"measurement\" has to contain elements of type T<:AbstractString only." )
    @assert( !isempty(fieldnames), "Atleast one field must be provided, i.e., a column name starting with \"f_\"")
    sort!(fieldnames)
    sort!(tagnames)

    # Create strings for fields, tags and timestamps
    rows = Tables.rows(datatable)

    # accumulate string in a buffer
    buffer = IOBuffer()
    for row in rows
        fieldnamestring = getfieldsstring(row, fieldnames)
        length(fieldnamestring) == 0 && continue # must have atleast one field
        tagnamestring = gettagsstring(row, tagnames)
        timestampstring = gettimestampstring( Tables.getcolumn(row, :timestamp); precision = precision )

        write(buffer, string(Tables.getcolumn(row, :measurement)) )
        if length(tagnamestring)>0
            write(buffer, ',')
            write(buffer, tagnamestring)
        end
        write(buffer, ' ')
        write(buffer, fieldnamestring)
        write(buffer, ' ')
        write(buffer, timestampstring)
        write(buffer, '\n')
    end
    seekstart(buffer)
    return buffer
end

# Returns the string of "fieldname1=value1,fieldname2=value2,..." for a given row of the table.
function getfieldsstring(data, colnames::Vector{Symbol})
    Out = Vector{String}(undef, length(colnames))
    i = 1
    for coln in colnames
        # check if we have a missing, else we insert name=val
        val = Tables.getcolumn(data, coln)
        ismissing(val) && continue

        # take colname[3:end] since we dont want to include "f_" or "t_"
        Out[i] = string(coln)[3:end]*"="*jl2linepstr( val )
        i += 1
    end
    i == 1 && return ""
    return join( view(Out, 1:i-1), ',')
end

jl2linepstr(x::Integer) = string(x)*'i'
jl2linepstr(x::AbstractString) = '\"'*x*'\"'
jl2linepstr(x::Union{Bool, AbstractFloat}) = string(x)
jl2linepstr(x::T) = throw("Only numbers, strings and boolean are supported in fields, got $T")

function gettagsstring(data, colnames::Vector{Symbol})
    Out = Vector{String}(undef, length(colnames))
    i = 1
    for coln in colnames
        # check if we have a missing, else we insert name=val
        val = Tables.getcolumn(data, coln)
        ismissing(val) && continue

        # take colname[3:end] since we dont want to include "f_" or "t_"
        Out[i] = string(coln)[3:end]*"="*string( val )
        i += 1
    end
    i == 1 && return ""
    return join( view(Out, 1:i-1), ',')
end

gettimestampstring(timestamp::T; precision::Symbol = :ms) where {T<:Integer} = string(timestamp)

function gettimestampstring(timestamp::T; precision::Symbol = :ms) where {T<:Dates.DateTime}
    factor = precisiondict[precision]
    return string( round(UInt64, datetime2unix(timestamp)*factor) )
end
