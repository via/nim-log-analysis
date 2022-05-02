
import tiny_sqlite
import cligen
import times

type Point = object
  time: uint64
  rpm: uint32
  map: float32
  ve: float32
  lambda: float32
  ego: float32
  ae: float32
  te: float32

proc get_rows(filename: string, start_time: int64, end_time: int64): seq[Point] =
  var rows: seq[Point] = @[]
  echo "Open DB:", filename
  let db = openDatabase(filename)
  for row in db.rows("SELECT realtime_ns, rpm, \"sensor.map\", ve, lambda, \"sensor.ego\", accel_enrich_percent, temp_enrich_percent FROM points WHERE realtime_ns > ? and realtime_ns < ?",
      start_time, end_time):
    let (time, rpm, map, ve, lambda, ego, ae, te) = row.unpack((uint64, uint32,
        float32, float32, float32, float32, float32, float32))
    rows.add(Point(time: time, rpm: rpm, map: map, ve: ve, lambda: lambda,
        ego: ego, ae: ae, te: te))
  echo rows.len()
  return rows


proc analyze(filename: string, last_hours: uint = 24): void =

  let end_time = now()
  let start_time = end_time - initDuration(hours = (int64)last_hours)

  let epoch = dateTime(1970, mJan, 1, zone = utc())
  let end_time_ns = (end_time - epoch).inNanoseconds()
  let start_time_ns = (start_time - epoch).inNanoseconds()

  let rows = get_rows(filename, start_time_ns, end_time_ns)
  var avg_rpm: int = 0
  for r in rows:
    avg_rpm += (int)r.rpm;
  echo avg_rpm / rows.len()



dispatch(analyze)
