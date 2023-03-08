type Point* = object
  realtime_ns: int64
  rpm: uint32
  ve: float32
  lambda: float32
  `sensor.map`: float32
  `sensor.ego`: float32
  `sensor.tps`: float32


type Interest* = object
  duration*: float
  rpm*: float
  map*: float
  ve*: float
  lambda*: float
  ego*: float

proc inRange(target: float, value: float, percent: float): bool =
  if abs(target - value) / target * 100 <= percent:
    return true
  return false


proc toWindows*(points: iterator(): Point, minLength: float, maxLength: float): iterator(): (bool, seq[Point]) =
  # - If the window is not stable in the first 0.1 seconds, start over
  # - If the window is stable for the first 0.1 s, end the window when

  var valid = false
  var currentWindow : seq[Point] = @[]

  # Iterate through the points, comparing to the start of the window
  return iterator(): (bool, seq[Point]) =
    for point in points():
      currentWindow.add(point)
      if currentWindow.len == 1:
        valid = true
    
      let first = currentWindow[0]
      let start_time = first.realtime_ns
      let rpm = first.rpm
      let map = first.`sensor.map`
      let tps = first.`sensor.tps`
      let ego = first.`sensor.ego`
      let was_valid = valid

      if not inRange((float)rpm, (float)point.rpm, 5):
        valid = false
      if not inRange(map, point.`sensor.map`, 2):
        valid = false
      if not inRange(tps, point.`sensor.tps`, 2):
        valid = false
      if not inRange(ego, point.`sensor.ego`, 2):
        valid = false

      if not valid and (point.realtime_ns - start_time) > (int64)(minLength * 1000000000):
        yield (was_valid, currentWindow)
        currentWindow = @[]
        continue
      # But regardless, don't go longer than maxLength
      if (point.realtime_ns - start_time) > (int64)(maxLength * 1000000000):
        yield (true, currentWindow)
        currentWindow = @[]
        continue

    yield (valid, currentWindow)


proc toInterest*(points: seq[Point]): Interest =

  var interest = Interest()
  for point in points:
    interest.rpm += (float)point.rpm
    interest.map += point.`sensor.map`
    interest.ve += point.ve
    interest.lambda += point.lambda
    interest.ego += point.`sensor.ego`

  let count = len(points)
  interest.duration = (float)(points[points.high].realtime_ns - points[points.low].realtime_ns) / 1000000000
  interest.rpm /= (float)count
  interest.map /= (float)count
  interest.ve /= (float)count
  interest.lambda /= (float)count
  interest.ego /= (float)count

  return interest

proc findClosest(val: int, buckets: openArray[int]): int =
  # find closest rpm
  var closest = buckets[0]
  var delta = abs(val - closest)
  for bucket in buckets:
    let new_delta = abs(val - bucket)
    if new_delta <= delta:
      delta = new_delta
      closest = bucket
      continue
    else:
      break

  return closest


proc toBucket*(interest: Interest, rpms: openArray[int], maps: openArray[int]): (int, int) =
  return (findClosest((int)interest.rpm, rpms), findClosest((int)interest.map, maps))

