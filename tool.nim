
import tiny_sqlite
import cligen
import std/strformat
import std/times

proc fromEpoch(epochTime: uint64): DateTime =
    let epoch = dateTime(1970, mJan, 1, zone = utc())
    let delta = initDuration(nanoseconds = (int64)epochTime)
    result = epoch + delta

type Session = object
    start: uint64
    stop: uint64
    count: Natural

proc getSessions(db: DbConn): seq[Session] =
    let query = "SELECT realtime_ns from points ORDER BY realtime_ns ASC"
    var sessions : seq[Session] = @[]
    
    var startTime : uint64 = 0
    var last : uint64 = 0
    var startSeen = false
    var count = 0

    for row in db.iterate(query):
        let (time) = row.unpack((uint64, ))
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

dispatchMulti([list])
