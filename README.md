# json2mqtt

Fetch JSON or HTML tables over HTTP and publish selected values to MQTT.
Configuration is loaded from `/config/config.yaml` or `config.yaml`.

## Scheduling

Use `update_rate` for an interval in seconds (default: `600`), or `cron` for a
[croniter expression](https://github.com/pallets-eco/croniter#usage).
For example, `0 12 * * *` means every day at noon.

```yaml
mqtt_server: localhost
mqtt_username: username
mqtt_password: password

timezone: Europe/Berlin
update_rate: 600

requests:
  'https://example.com/daily.json':
    cron: '0 12 * * *'
    jitter: 600  # Random offset from -600 to +600 seconds: 11:50–12:10.
    topics:
      daily/value:
        path: [value]
        retain: true

  'https://example.com/frequent.json':
    update_rate: 60
    jitter: 10  # Subsequent requests are 50–70 seconds apart.
    topics:
      frequent/value:
        path: [value]
```

- `cron`, `update_rate`, `jitter`, and `timezone` can be set globally or per URL.
  A URL's own `cron` or `update_rate` overrides the global schedule. If both are
  set at the same level, `cron` takes precedence. `cron: null` disables an
  inherited cron schedule and uses the interval instead.
- `jitter` defaults to `0`. It is a non-negative number of seconds. Each deadline
  gets one uniformly random offset in `[-jitter, +jitter]`, retained until that
  run. For intervals, jitter must be smaller than `update_rate`.
- `timezone` accepts an IANA name such as `Europe/Berlin`. Without it, cron uses
  the process's local timezone (typically UTC in Docker). Named timezones need
  system timezone data; the Docker image includes it.
- Interval requests run immediately on startup. Cron requests wait for their
  next sampled deadline. Past sampled deadlines are skipped, including when
  jitter windows overlap; each nominal cron occurrence is considered only once
  per process. Deadlines are not persisted across restarts.
- Failed requests still advance the schedule. Delayed processing runs an
  already pending request once, then skips past deadlines instead of replaying
  all missed occurrences. Cron stays anchored to its nominal times, so jitter
  does not accumulate from day to day.

## Conditional topics

Add an optional `if` block to a topic to publish its value only when another
value in the same response equals the configured value:

```yaml
requests:
  'https://example.com/device.json':
    topics:
      device/power:
        path: [measurements, power]
        if:
          path: [status, enabled]
          equals: 1
        retain: true
      device/standby_power:
        path: [measurements, power]
        if:
          path: [status, enabled]
          equals: 0
```

For a response like
`{"status": {"enabled": 1}, "measurements": {"power": 42}}`, only
`device/power` is published, with a payload of `42`.

- Both paths start at the response root and use lists of object keys and/or
  integer array indices, for example `[devices, 0, enabled]`. An empty path `[]`
  selects the entire response. Conditions also work with parsed HTML tables,
  whose cell values are strings.
- `equals` is required and accepts YAML values, including numbers, strings,
  booleans, and `null`. Values are compared without string/number conversion:
  `equals: 1` matches the number `1`; `equals: "1"` matches the string `"1"`.
- If the condition does not match or its path cannot be read, that topic is
  skipped. An unreadable path is logged; other topics continue normally.
  A missing path does not match `equals: null`.
- The condition is checked on each fetched response, before extracting or
  applying `offset`/`factor` to the topic value. Topics without `if` publish
  normally. Skipping a topic does not clear a previously retained MQTT value.
- An invalid `if` block is rejected at startup. Omit the block to disable the
  condition.

## Tests

```sh
uv run python -m unittest discover -s tests -v
```
