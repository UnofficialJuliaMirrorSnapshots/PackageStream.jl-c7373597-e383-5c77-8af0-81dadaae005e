using PackageStream
using Base.Test

# Test functions
pack_unpack(a::Any) = unpack(pack(a)) == a
unpack_pack(b::Vector{UInt8}) = pack(unpack(b)) == b
function test_packing(a::Any, b::Vector{UInt8})
    return pack(a) == b && unpack(b) == a && pack_unpack(a) && unpack_pack(b)
end

# Test : Nil, Bool
@test test_packing(nothing, b"\xc0")
@test test_packing(false, b"\xc2")
@test test_packing(true, b"\xc3")

# Test : Integer
@test pack_unpack((r = rand(-2^4:typemax(Int8)); println("tiny = $r"); r))
@test pack_unpack((r = rand(typemin(Int8):-2^4-1); println("int8 = $r"); r))
@test pack_unpack((r = rand(typemax(Int8)+1:typemax(Int16)); println("int16+ = $r"); r))
@test pack_unpack((r = rand(typemin(Int16):typemin(Int8)-1); println("int16- = $r"); r))
@test pack_unpack((r = rand(typemax(Int16)+1:typemax(Int32)); println("int32+ = $r"); r))
@test pack_unpack((r = rand(typemin(Int32):typemin(Int16)-1); println("int32- = $r"); r))
@test pack_unpack((r = rand(typemax(Int32)+1:typemax(Int64)); println("int64+ = $r"); r))
@test pack_unpack((r = rand(typemin(Int64):typemin(Int32)-1); println("int64- = $r"); r))

# Test : String
strings = [
    str_tiny = "tiny",
    str_8 = "My string is not tiny but within 8 bytes",
    str_16 = "My string is long enough, not tiny, above 8 bytes and below 16. "^4,
    str_32 = "My string is long enough, not tiny, above 16 bytes and below 32. "^1024
]
@test mapreduce(pack_unpack, &, true, strings)
alphabet_bytes = b"\xd0\x1a\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f\x50\x51\x52\x53\x54\x55\x56\x57\x58\x59\x5a"
@test test_packing("ABCDEFGHIJKLMNOPQRSTUVWXYZ", alphabet_bytes)

# Test : List (Vector)
@test pack_unpack(Vector(1:10))
@test pack_unpack(Vector(1:100))
@test pack_unpack(Vector(1:1000))
@test pack_unpack(Vector(1:100000))
heterogeneous_list_bytes =
    b"\x93\x01\xC1\x40\x00\x00\x00\x00\x00\x00\x00\x85\x74\x68\x72\x65\x65"
@test test_packing([1, 2., "three"], heterogeneous_list_bytes)

# Test : Map (Dict)
@test pack_unpack(Dict([(i, 2i) for i in 1:10]))
@test pack_unpack(Dict([(i, 2i) for i in 1:100]))
@test pack_unpack(Dict([(i, 2i) for i in 1:1000]))
@test pack_unpack(Dict([(i, 2i) for i in 1:100000]))

# Test : Structure (Tuple)
@test pack_unpack(Tuple(1:10))
@test pack_unpack(Tuple(1:100))
@test pack_unpack(Tuple(1:1000))
