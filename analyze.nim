
import tiny_sqlite
import cligen
import times
import std/strformat

type Point* = object
  realtime_ns: int64
  rpm: uint32
  ve: float32
  lambda: float32
  `sensor.map`: float32
  `sensor.ego`: float32
  `sensor.tps`: float32

proc getRows*[T](db: DbConn, start_time: int64, end_time: int64): seq[T] =
  const example = T()
  var rows: seq[T] = @[]

  var fieldnames : seq[string]
  for x, _ in example.fieldPairs:
    let name = x # Can't use x directly
    fieldnames.add(&"\"{name}\"")
  let fieldnames_str = fieldnames.join(", ")
  let query = &"SELECT realtime_ns AS _realtime_ns, {fieldnames_str} FROM points where _realtime_ns > ? and _realtime_ns < ?"

  for row in db.iterate(query, start_time, end_time):
    var colidx = 1
    var rowT = T()
    for name, val in rowT.fieldPairs:
      val = row[colidx].fromDbValue(typedesc(val))
      colidx += 1
    rows.add(rowT)

  return rows

type Interest = object
  duration: float
  rpm: float
  map: float
  ve: float
  lambda: float
  ego: float

type TimeRange = tuple[valid: bool, start: int, stop: int]

proc inRange(target: float, value: float, percent: float): bool =
  if abs(target - value) / target * 100 <= percent:
    return true
  return false


proc buildWindow(points: seq[Point], start_idx: int, minLength: float, maxLength: float): TimeRange =
  # - If the window is not stable in the first 0.1 seconds, start over
  # - If the window is stable for the first 0.1 s, end the window when

  let start_time = points[start_idx].realtime_ns
  let rpm = points[start_idx].rpm
  let map = points[start_idx].`sensor.map`
  let tps = points[start_idx].`sensor.tps`
  var valid = true

  # Iterate through the points, comparing to the start of the window
  for idx in start_idx..points.high:
    let point = points[idx]
    let was_valid = valid
    if not inRange((float)rpm, (float)point.rpm, 5):
      valid = false
    if not inRange(map, point.`sensor.map`, 5):
      valid = false
    if not inRange(tps, point.`sensor.tps`, 5):
      valid = false

    if not valid and (point.realtime_ns - start_time) > (int64)(minLength * 1000000000):
      return (was_valid, start_idx, idx)
    # But regardless, don't go longer than maxLength
    if (point.realtime_ns - start_time) > (int64)(maxLength * 1000000000):
      return (true, start_idx, idx)

  return (valid, start_idx, points.high)


proc windowToInterest(points: seq[Point], window: TimeRange): Interest =

  var interest = Interest()
  for idx in window.start..window.stop:
    interest.rpm += (float)points[idx].rpm
    interest.map += points[idx].`sensor.map`
    interest.ve += points[idx].ve
    interest.lambda += points[idx].lambda
    interest.ego += points[idx].`sensor.ego`

  let count = window.stop - window.start + 1
  interest.duration = (float)(points[window.stop].realtime_ns - points[window.start].realtime_ns) / 1000000000
  interest.rpm /= (float)count
  interest.map /= (float)count
  interest.ve /= (float)count
  interest.lambda /= (float)count
  interest.ego /= (float)count

  return interest



proc buildInterests*(points: seq[Point]): seq[Interest] =
  var window_start = 0
  while window_start < points.high:
    let window = buildWindow(points, window_start, 0.1, 1.0)
    if window.valid:
      let interest = windowToInterest(points, window)
      result.add(interest)

    window_start = window.stop + 1
