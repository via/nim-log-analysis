import tiny_sqlite
import cligen
import std/strformat
import std/times
import std/tables
import sequtils

import analyze

proc fromEpoch(epochTime: int64): DateTime =
    let epoch = dateTime(1970, mJan, 1, zone = utc())
    let delta = initDuration(nanoseconds = (int64)epochTime)
    result = epoch + delta

proc toEpoch(dt: DateTime): int64 =
    let epoch = dateTime(1970, mJan, 1, zone = utc())
    let delta = dt - epoch
    result = delta.inNanoseconds

type Session = object
    start: int64
    stop: int64
    count: Natural

proc getRows*[T](db: DbConn, start_time: int64, end_time: int64): iterator(): T =
  when T is Table:
    let query = &"SELECT * FROM points where _realtime_ns > ? and _realtime_ns < ?"
    return iterator(): T =
      yield {"": 1}.toTable
  else:
    const example = T()
    var fieldnames : seq[string]
    for x, _ in example.fieldPairs:
      let name = x # Can't use x directly
      fieldnames.add(&"\"{name}\"")
    let fieldnames_str = fieldnames.join(", ")
    let query = &"SELECT realtime_ns AS _realtime_ns, {fieldnames_str} FROM points where _realtime_ns > ? and _realtime_ns < ?"

    return iterator(): T =
      for row in db.iterate(query, start_time, end_time):
        var colidx = 1
        var rowT = T()
        for name, val in rowT.fieldPairs:
          val = row[colidx].fromDbValue(typedesc(val))
          colidx += 1
        yield rowT


proc getSessions(db: DbConn): seq[Session] =
    let query = "SELECT realtime_ns from points ORDER BY realtime_ns ASC"
    var sessions : seq[Session] = @[]
    
    var startTime : int64 = 0
    var last : int64 = 0
    var startSeen = false
    var count = 0

    for row in db.iterate(query):
        let (time) = row.unpack((int64, ))
        count += 1

        if not startSeen:
            startSeen = true
            startTime = time
            last = time
            continue

        if time - last > 10 * 1000 * 1000 * 1000:
            # startSeen through last is a session
            let sesh = Session(start: startTime, stop: last, count: count)
            sessions.add(sesh)
            startTime = time
            last = time
            count = 0
            continue

        last = time

    # Capture any unended session
    sessions.add(Session(start: startTime, stop: last, count: count))

    return sessions

proc list(filename: string): void =
  let db = openDatabase(filename)
  let sessions = getSessions(db)
  var idx = 0
  for sesh in sessions:
    let minutes = (int)(sesh.stop - sesh.start) / (1000000000 * 60)
    let start = fromEpoch(sesh.start)
    let stop = fromEpoch(sesh.stop)
    echo &"{idx}: {start} - {stop} ({sesh.count} points, {minutes:.1f} minutes)"
    idx += 1


proc analyzef(filename: string): void =
  let db = openDatabase(filename)
  let rows = getRows[Point](db, 0, 1800000000000000000)
  let windows = rows.toWindows(0.25, 1.0)
  var valids = 0.0
  for w in windows:
    if w[0]:
      let interest = w[1].toInterest
      let correction = interest.ego / interest.lambda;
      let new_ve = interest.ve * correction
      echo &"RPM: {(int)interest.rpm:4}    MAP: {(int)interest.map:3}    VE: {(int)interest.ve:2}    CORR: {(int)(correction*100):3}    NEWVE: {(int)new_ve:3}"

  echo valids

proc exportPoints(filename: string, sessionsStr = "", start = "", stop=""): void =
  var sessions: seq[Session]
  if sessionsStr == "":
    # Make a fake session from the time range
    let startDt = parse("yyyy-MM-dd'T'HH:mm:sszzz", start)
    let stopDt = parse("yyyy-MM-dd'T'HH:mm:sszzz", stop)
    sessions.add(Session(start: startDt.toEpoch, stop: stopDt.toEpoch, count: 0))
  





dispatchMulti([list], [analyzef], [exportPoints])
