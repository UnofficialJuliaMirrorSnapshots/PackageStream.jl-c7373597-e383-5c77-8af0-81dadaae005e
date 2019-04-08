########################################################################
# This package is based on the neo4j-contrib/boltkit package on GitHub #
# It is also greatly influence by the JuliaIO/MsgPack.jl Julia package #
########################################################################

__precompile__()
module PackageStream

# Imports/Exports
import Base.showerror
export pack, unpack

# Type of packable values
Atomic = Union{Void, Bool, Integer, AbstractFloat, AbstractString}
"""
    Packable type of data values
    1) Atomic values: `Union{Void, Bool, Integer, AbstractFloat, AbstractString}`
    2) Packable values: `Union{Atomic, Vector, Dict, Tuple}`
"""
Packable = Union{Atomic, Vector, Dict, Tuple}

# Dictionary to handle markers and dispatch associated with packed items
const markers = Dict{Symbol, Tuple{Vector{UInt8}, Function}}(
    :Nothing    => (b"\xC0", s -> nothing),
    :False      => (b"\xC2", s -> false),
    :True       => (b"\xC3", s -> true),
    :Int8       => (b"\xC8", s -> readi(s, Int8)),
    :Int16      => (b"\xC9", s -> readi(s, Int16)),
    :Int32      => (b"\xCA", s -> readi(s, Int32)),
    :Int64      => (b"\xCB", s -> readi(s, Int64)),
    :Float      => (b"\xC1", s -> readn(s, Float64)),
    :String8    => (b"\xD0", s -> unpackstring(s, readn(s, UInt8))),
    :String16   => (b"\xD1", s -> unpackstring(s, readn(s, UInt16))),
    :String32   => (b"\xD2", s -> unpackstring(s, readn(s, UInt32))),
    :List8      => (b"\xD4", s -> unpacklist(s, readn(s, UInt8))),
    :List16     => (b"\xD5", s -> unpacklist(s, readn(s, UInt16))),
    :List32     => (b"\xD6", s -> unpacklist(s, readn(s, UInt32))),
    :Map8       => (b"\xD8", s -> unpackmap(s, readn(s, UInt8))),
    :Map16      => (b"\xD9", s -> unpackmap(s, readn(s, UInt16))),
    :Map32      => (b"\xDA", s -> unpackmap(s, readn(s, UInt32))),
    :Struct8    => (b"\xDC", s -> unpackstruct(s, readn(s, UInt8))),
    :Struct16   => (b"\xDD", s -> unpackstruct(s, readn(s, UInt16))),
)

# Error specific to the package
struct ValueError <: Exception
    content::AbstractString
end
Base.showerror(io::IO, e::ValueError) = print(io, e.content)

# Function to write an item that is big enough to need a head marker
function writehead(
    stream::IOBuffer,
    head::Tuple{Vector{UInt8}, Function},
    value::Packable
    )
    writehead(stream, head[1], value)
end
function writehead(
    stream::IOBuffer,
    head::Vector{UInt8},
    value::Packable
    )
    write(stream, head)
    write(stream, hton(value))
end

# Function pack with dispatch over Void value
function pack(
    stream::IOBuffer,
    value::Void
    )
    write(stream, markers[:Nothing][1])
end

# Function pack with dispatch over Bool value
function pack(
    stream::IOBuffer,
    value::Bool
    )
    value ? write(stream, markers[:True][1]) : write(stream, markers[:False][1])
end

# Function pack with dispatch over Integer value
function pack(
    stream::IOBuffer,
    value::Integer
    )
    if -2^4 ≤ value ≤ typemax(Int8) # Tiny int
        write(stream, Int8(value))
    elseif typemin(Int8) ≤ value ≤ typemax(Int8) # Int8
        writehead(stream, markers[:Int8], Int8(value))
    elseif typemin(Int16) ≤ value ≤ typemax(Int16) # Int16
        writehead(stream, markers[:Int16], Int16(value))
    elseif typemin(Int32) ≤ value ≤ typemax(Int32) # Int32
        writehead(stream, markers[:Int32], Int32(value))
    elseif typemin(Int64) ≤ value ≤ typemax(Int64) # Int64
        writehead(stream, markers[:Int64], Int64(value))
    else
        throw(ValueError("Integer value out of packable range"))
    end
end

# Function pack with dispatch over Float value
function pack(
    stream::IOBuffer,
    value::AbstractFloat
    )
    writehead(stream, markers[:Float], value)
end

# Function pack with dispatch over String value
function pack(
    stream::IOBuffer,
    value::AbstractString
    )
    utf_8 = transcode(String, value)
    l = length(utf_8)
    if l < 0x10
        write(stream, UInt8(0x80 + l))
    elseif l < 0x100
        writehead(stream, markers[:String8], UInt8(l))
    elseif l < 0x10000
        writehead(stream, markers[:String16], UInt16(l))
    elseif l < 0x100000000
        writehead(stream, markers[:String32], UInt32(l))
    else
        throw(ValueError("String too long to pack"))
    end
    write(stream, utf_8)
end

# Function pack with dispatch over list (Vector) value
function pack(
    stream::IOBuffer,
    value::Vector
    )
    l = length(value)
    if l < 0x10
        write(stream, UInt8(0x90 + l))
    elseif l < 0x100
        writehead(stream, markers[:List8], UInt8(l))
    elseif l < 0x10000
        writehead(stream, markers[:List16], UInt16(l))
    elseif l < 0x100000000
        writehead(stream, markers[:List32], UInt32(l))
    else
        throw(ValueError("List too long to pack"))
    end
    foreach(x -> write(stream, pack(x)), value)
end

# Function pack with dispatch over map (Dict) value
function pack(
    stream::IOBuffer,
    value::Dict
    )
    l = length(value)
    if l < 0x10
        write(stream, UInt8(0xA0 + l))
    elseif  l < 0x100
        writehead(stream, markers[:Map8], UInt8(l))
    elseif l < 0x10000
        writehead(stream, markers[:Map16], UInt16(l))
    elseif l < 0x100000000
        writehead(stream, markers[:Map32], UInt32(l))
    else
        throw(ValueError("Dictionary too long to pack"))
    end
    foreach(x -> (write(stream, pack(x[1])); write(stream, pack(x[2]))), value)
end

# Function pack with dispatch over struct (Tuple) value
function pack(
    stream::IOBuffer,
    value::Tuple
    )
    signature, fields = value[1], value[2:end]
    l = length(fields)
    if l < 0x10
        write(stream, UInt8(0xB0 + l))
    elseif l < 0x100
        writehead(stream, markers[:Struct8], UInt8(l))
    elseif l < 0x10000
        writehead(stream, markers[:Struct16], UInt16(l))
    else
        throw(ValueError("Structure too big to pack"))
    end
    write(stream, UInt8(signature))
    foreach(x -> write(stream, pack(x)), fields)
end

"""
    pack(value::Packable)

Return a Byte Array (`Vector{UInt8}`) encoded through the PackageStream
specification.

The value type must be either of the following:
* The Nil value `nothing`
* A boolean value `true` or `false`
* An integer up to 64 bits `Int64`
* A float up to 64 bits `Float64`
* A string (up to 2^32 characters) `<:AbstractString`
* A list (up to 2^32 elements) `Vector`
* A map of keys and values (up to 2^32 elements) `Dict`
* A structure (signature, field 1, ..., field n) (up to 2^32 fields) `Tuple`
"""
function pack(
    value::Packable
    )
    stream = IOBuffer()
    pack(stream, value)
    return take!(stream)
end

# Read functions from big-endian to little-endian
function readn(
    stream::IOBuffer,
    t::DataType
    )
    return ntoh(read(stream, t))
end
function readi(
    stream::IOBuffer,
    t::DataType
    )
    return Int64(readn(stream, t))
end

# Sub-routine to unpack specific items: strings, lists, maps, structs
function unpackstring(
    stream::IOBuffer,
    length_values::Integer
    )
    return String(read(stream, length_values))
end
function unpacklist(
    stream::IOBuffer,
    length_values::Integer
    )
    return [unpack(stream) for i in 1:length_values]
end
function unpackmap(
    stream::IOBuffer,
    length_values::Integer
    )
    out = Dict()
    for i in 1:length_values
        k = unpack(stream)
        v = unpack(stream)
        out[k] = v
    end
    return out
end
function unpackstruct(
    stream::IOBuffer,
    length_values::Integer
    )
    return Tuple([unpack(stream) for i in 1:length_values+1])
end

# Unpack core dispatch (stream argument)
function unpack(
    stream::IOBuffer
    )
    headbyte = read(stream, UInt8)
    for (mark, dispatch) in values(markers)
        if mark[1] == headbyte
            return dispatch(stream)
        end
    end
    if -2^4 ≤ (hb = reinterpret(Int8, headbyte); hb) ≤ typemax(Int8)
        return Int64(hb)
    elseif 0x80 ≤ headbyte < 0x90
        unpackstring(stream, headbyte - 0x80)
    elseif 0x90 ≤ headbyte < 0xa0
        unpacklist(stream, headbyte - 0x90)
    elseif 0xa0 ≤ headbyte < 0xb0
        unpackmap(stream, headbyte - 0xa0)
    elseif 0xb0 ≤ headbyte < 0xc0
        unpackstruct(stream, headbyte - 0xb0)
    else
        throw(ValueError("Unknow marker byte $headbyte"))
    end
end

"""
    unpack(byte_array)

Argument is a Byte Array (`Vector{UInt8}`) following the encoding the
PackageStream specification for Neo4j.

The return value type can be either of the following:
* The Nil value `nothing`
* A boolean value `true` or `false`
* An integer up to 64 bits `Int64`
* A float up to 64 bits `Float64`
* A string (up to 2^32 characters) `<:AbstractString`
* A list (up to 2^32 elements) `Vector`
* A map of keys and values (up to 2^32 elements) `Dict`
* A structure (signature, field 1, ..., field n) (up to 2^32 fields) `Tuple`
"""
function unpack(
    stream::Vector{UInt8}
    )
    return unpack(IOBuffer(stream))
end

end # module
