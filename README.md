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

## Tests

```sh
uv run python -m unittest discover -s tests -v
```
