
import tiny_sqlite
import cligen

type Point = object
  time: uint64
  rpm: uint32
  map: float32
  ve: float32
  lambda: float32
  ego: float32
  

proc get_rows(filename: string, last_hours: uint): seq[Point] =
  var rows : seq[Point] = @[]
  let db = openDatabase("../flviaems/build/log.vlog")
  for row in db.rows("SELECT realtime_ns, rpm, \"sensor.map\", ve, lambda, \"sensor.ego\" from points limit 1000000"):
    let (time, rpm, map, ve, lambda, ego) = row.unpack((uint64, uint32, float32, float32, float32, float32))
    rows.add(Point(time: time, rpm: rpm, map: map, ve: ve, lambda: lambda, ego: ego))
  echo rows.len()
  return rows


proc analyze(filename: string, last_hours: uint = 24): void = 
  discard get_rows(filename, last_hours)

dispatch(analyze)
