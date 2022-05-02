import std/streams

import tiny_sqlite
import cbor
import cligen
import zstd/compress

const FileHeaderText = "VIAEMSLOG1"
const FileFooterText = "VIAEMSLOGFOOTER1"

type
  ChunkType = enum
    Ignore = 0
    Header = 1
    Footer = 2
    Meta = 3
    Feed = 4
    FeedAggr100 = 5

  ValueType = enum
    Unknown = 0x0
    Uint32 = 0x1
    Float32 = 0x2

  LogChunk = object
    first: uint64
    last: uint64
    keys: seq[string]
    types: seq[ValueType]
    rows: uint64
    points: seq[uint8]

  LogChunkIndex = object
    start: uint64
    stop: uint64
    chunktype: uint64
    offset: uint64
    size: uint64

proc write[T](buffer: var seq[uint8], value: T) =
  var tmp: array[sizeof(T), uint8]
  var valueCopy = value
  copyMem(addr tmp[0], addr valueCopy, sizeof(T))
  buffer.add(tmp)

proc write_sql_row(log: var LogChunk, dbrow: ResultRow) =
  let realtime_ns = dbrow["realtime_ns"].fromDbValue(uint64)
  log.points.write(realtime_ns)

  if log.keys.len == 0:
    log.first = realtime_ns
    for k, value in dbrow.values[1 .. dbrow.len - 1]:
      log.keys.add(dbrow.columns[k + 1])
      if value.kind == sqliteReal:
        log.types.add(ValueType.Float32)
      if value.kind == sqliteInteger:
        log.types.add(ValueType.Uint32)

  log.last = realtime_ns

  for value in dbrow.values[1 .. dbrow.len - 1]:
    if value.kind == sqliteReal:
      log.points.write(float32(value.floatVal))
    if value.kind == sqliteInteger:
      log.points.write(uint32(value.intVal))
  log.rows += 1

proc write_file_header(o: Stream) =
  const fileHeaderLen = sizeof(uint64) + sizeof(uint64) + sizeof(FileHeaderText)
  o.write(uint64(fileHeaderLen))
  o.write(uint64(ChunkType.Header))
  o.write(FileHeaderText)

proc generate_datachunk_header(chunk: LogChunk): string =
  type Column = tuple[name: string, typ: int]
  type ChunkHeader = object
    compression: string
    columns: seq[Column]

  var cols: seq[Column]
  for k, key in chunk.keys:
    let val : Column = (name: key, typ: int(chunk.types[k]))
    cols.add(val)
  
  var headerstream = newStringStream("")
  let header = ChunkHeader(compression: "zstd", columns: cols)
  headerstream.writeCbor(header)
  headerstream.flush()
  headerstream.setPosition(0)
  result = headerstream.readAll()

proc write_datachunk(o: Stream, chunk: LogChunk): LogChunkIndex =
  var compressed_data = compress(chunk.points)
  let headerstr = generate_datachunk_header(chunk)
  let chunklen = uint64(sizeof(uint64) + sizeof(uint64) + sizeof(uint64) +
      headerstr.len + compressed_data.len)
  let offset = uint64(o.getPosition())
  o.write(uint64(chunklen))
  o.write(uint64(ChunkType.Feed))
  o.write(uint64(headerstr.len))
  o.write(headerstr)
  o.writeData(addr compressed_data[0], compressed_data.len)

  result = LogChunkIndex(start: chunk.first,
      stop: chunk.last,
      chunktype: uint64(ChunkType.Feed),
      offset: offset,
      size: chunklen)

proc write_footer(o: Stream, metaoffset: uint64) =
  const footersize : uint64 = sizeof(uint64) * 3 + FileFooterText.len
  o.write(footersize)
  o.write(uint64(ChunkType.Footer))
  o.write(metaoffset)
  o.write(FileFooterText)


proc write_metachunk(o: Stream, previous_meta: uint64, index: seq[LogChunkIndex]) =
  let chunksize = uint64((4 * sizeof(uint64)) + (5 * sizeof(uint64) * index.len))
  o.write(chunksize)
  o.write(uint64(ChunkType.Meta))
  o.write(previous_meta)
  o.write(uint64(index.len))
  for i in index:
    o.write(i.start)
    o.write(i.stop)
    o.write(i.chunktype)
    o.write(i.offset)
    o.write(i.size)


proc convert(sqlFilename: string, outputFilename: string): void =
  var outfile = newFileStream(outputFilename, fmWrite)
  write_file_header(outfile)

  var chunk: LogChunk
  var index: seq[LogChunkIndex]
  let db = openDatabase(sqlFilename)
  for row in db.iterate("SELECT * FROM points ORDER BY realtime_ns ASC"):
    chunk.write_sql_row(row)
    if chunk.rows mod 100000 == 0:
      index.add(write_datachunk(outfile, chunk))
      chunk = LogChunk()
      echo "wrote: " & $index.len
  let last_meta_at = outfile.getPosition()
  write_metachunk(outfile, 0, index)
  write_footer(outfile, uint64(last_meta_at))


dispatch(convert)
