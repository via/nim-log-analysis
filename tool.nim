
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

proc test(filename: string): void =
  let db = openDatabase(filename)
#  echo get_rows[Table](db, 0, 1800000000000000000)

proc analyzef(filename: string): void =
  let db = openDatabase(filename)
  let rows = get_rows[Point](db, 0, 1800000000000000000)
  let windows = buildWindows(rows, 0.1, 1.0)
  var valids = 0.0
  for w in windows:
    if w[0]:
      valids += windowToInterest(w[1]).duration

  echo valids

proc exportPoints(filename: string, sessionsStr = "", start = "", stop=""): void =
  var sessions: seq[Session]
  if sessionsStr == "":
    # Make a fake session from the time range
    let startDt = parse("yyyy-MM-dd'T'HH:mm:sszzz", start)
    let stopDt = parse("yyyy-MM-dd'T'HH:mm:sszzz", stop)
    sessions.add(Session(start: startDt.toEpoch, stop: stopDt.toEpoch, count: 0))
  





dispatchMulti([list], [test], [analyzef])
