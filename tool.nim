
import tiny_sqlite
import cligen
import std/strformat
import std/times
import ggplotnim
import ggplotnim/ggplot_vega


import analyze

proc fromEpoch(epochTime: uint64): DateTime =
    let epoch = dateTime(1970, mJan, 1, zone = utc())
    let delta = initDuration(nanoseconds = (int64)epochTime)
    result = epoch + delta

type Session = object
    start: uint64
    stop: uint64
    count: Natural

proc bleh[T](thing: T): void =
  const example = T()
  for x, y in example.fieldPairs:
    echo x

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
  bleh(sessions[0])
  var idx = 0
  for sesh in sessions:
    let minutes = (int)(sesh.stop - sesh.start) / (1000000000 * 60)
    let start = fromEpoch(sesh.start)
    let stop = fromEpoch(sesh.stop)
    echo &"{idx}: {start} - {stop} ({sesh.count} points, {minutes:.1f} minutes)"
    idx += 1

proc test(filename: string): void =
  let db = openDatabase(filename)
  echo get_rows[analyze.Point](db, 0, 1800000000000000000)[0]

proc analyzef(filename: string): void =
  let db = openDatabase(filename)
  let rows = get_rows[analyze.Point](db, 0, 1800000000000000000)
  let interests = buildInterests(rows)
  var maps : seq[float]
  var rpms : seq[float]
  var corrections : seq[float]
  var ves : seq[float]
  var durs : seq[int]
  for idx in interests.low..interests.high:
    maps.add(interests[idx].map)
    rpms.add(interests[idx].rpm)
    corrections.add(interests[idx].correction)
    ves.add(interests[idx].corrected_ve)
    durs.add((int)(interests[idx].duration * 5))
  let df = toDf({"map": maps, "rpm": rpms, "delta": corrections, "newve": ves, "durations": durs})
#  ggplot(df, aes("rpm", "map", color="newve")) + geom_point() + ggsave("bleh.png", width=1280, height=1024)
  ggplot(df, aes("rpm", "map", color="newve")) + geom_point() + ggvega("bleh.html", width=1600, height=1200)

dispatchMulti([list], [test], [analyzef])
