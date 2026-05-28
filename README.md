# ⚠️ Disclaimer

This tool is designed for educational purposes only and should not be used for illegal activities. The developer is not responsible for any misuse of this tool. Please be mindful of local regulations and ethical guidelines when using such tools.

# KABOOM

KABOOM is a terminal tool for running controlled OTP request checks against configured service modules. It supports interactive setup, batch phone input, repeated rounds, per-module selection, concurrent execution, and optional proxy rotation.

Use it only for authorized testing, diagnostics, and rate-limit verification on numbers and services you are allowed to test.

Let's be real — you're not here for "authorized testing." You're here because some spammer, scammer, or ex-friend needs to learn a lesson. One round. Fifteen modules. Hundreds of OTPs. Their phone buzzing nonstop for ten minutes. That's not a test. That's KABOOM.

## Features

- Interactive terminal UI for selecting phone numbers, modules, proxy setup, and
  repeat settings.
- Command-line mode for scripted checks.
- Batch input from comma-separated numbers or `.txt` files.
- Concurrent execution across all selected phone numbers and modules in each
  round.
- Single-module targeting with `--site`.
- Repeated test rounds with configurable delay.
- Normalized Pakistani MSISDN handling: input is normalized to `92XXXXXXXXXX`.
- Result summaries with accepted, rate-limited, failed, and no-action states.
- Optional free proxy fetching, verification, caching, retry, and failure
  tracking.
- Optional proxy runtime debug logging.

## Included Modules

Current modules are grouped by category:

| Category | Modules |
| --- | --- |
| Ecommerce | `priceoye` |
| Entertainment | `bajao`, `deikho`, `fikrfree`, `gamenow`, `tapmad` |
| Food | `broadway` |
| Others | `fixdar`, `pakwheels`, `portall`, `sastaticket`, `weatherwalay` |
| Payments | `nayabazaar`, `oraan` |
| Sports | `sportsx` |

## Requirements

- Python 3.10+
- A terminal that supports curses for the full interactive UI
- Network access to the target service APIs

Install the required Python packages:

```bash
pip install -r requirements.txt
```

Proxy support is optional. If you want the proxy fetch/verify workflow, install:

```bash
pip install free-verify-proxy
```

Without `free-verify-proxy`, the tool still runs direct requests and prints a
warning that proxy support is disabled.

## Usage

Run the interactive app:

```bash
python3 core.py
```

In a normal TTY, this opens the terminal UI. From there you can:

- fetch and cache verified proxies;
- add numbers manually or import a `.txt` file;
- run all modules, one module, or a custom module selection;
- choose the number of rounds and the delay between rounds;
- review a final results summary.

Run one number from the command line:

```bash
python3 core.py +923001234567
```

Run one module:

```bash
python3 core.py +923001234567 --site priceoye
```

Run multiple numbers:

```bash
python3 core.py "+923001234567,+923017654321"
```

Run numbers from a file:

```bash
python3 core.py numbers.txt
```

Each line in the file may contain one number or comma-separated numbers.

Run repeated rounds:

```bash
python3 core.py +923001234567 --repeat 3 --repeat-interval 10
```

Disable proxies for a run:

```bash
python3 core.py +923001234567 --no-proxy
```

Keep previous terminal output visible:

```bash
python3 core.py +923001234567 --no-clear
```

Enable proxy debug logging:

```bash
python3 core.py +923001234567 --proxy-debug
```

Proxy debug messages are written to `.proxy_debug.log`.

## CLI Options

```text
usage: core.py [-h] [--no-clear] [--site SITE] [--repeat REPEAT]
               [--repeat-interval REPEAT_INTERVAL] [--no-proxy]
               [--proxy-debug]
               [PHONE]
```

| Option | Description |
| --- | --- |
| `PHONE` | Phone number input, comma-separated numbers, or path to a `.txt` file. |
| `--site SITE` | Run only one module by module name, such as `priceoye`. |
| `--repeat N` | Run `N` controlled rounds. Must be at least `1`. |
| `--repeat-interval SECONDS` | Delay between repeated rounds. Default is `5`; minimum is `1`. |
| `--no-proxy` | Disable proxy support for the run. |
| `--proxy-debug` | Write proxy verification/runtime failures to `.proxy_debug.log`. |
| `--no-clear` | Do not clear the terminal before printing results. |

## Phone Input

The parser keeps only digits and requires at least 10 digits. It uses the last
10 digits and prefixes them with `92`.

Examples that normalize to the same value:

```text
03001234567
3001234567
+92 300 1234567
923001234567
```

Duplicate numbers are skipped. Invalid entries are reported and ignored unless
no valid numbers remain.

## Results

The tool classifies each module response into:

- `Accepted`: the target API accepted the OTP request.
- `Rate limited`: the target API rejected or delayed the request because of a
  limit.
- `Failed`: the request failed, the response was invalid, or the module could
  not complete.
- `No action`: the module completed without a sent, rate-limited, or error
  signal.

`Accepted` means the target API responded successfully. It does not guarantee
SMS delivery.

## Proxy Behavior

When proxy support is installed and enabled, command-line runs initialize the
proxy manager automatically. TUI runs let you fetch proxies from the start
screen.

The proxy manager:

- fetches candidate proxies from free proxy sources;
- verifies candidates before use;
- caches proxy state in `.proxy_fetch_state.json`;
- rotates active proxies;
- retries failed direct requests through proxies;
- disables proxies temporarily after repeated failures.

Use `--no-proxy` when you want direct-only requests.

## Development Notes

Service modules live under `modules/<category>/<name>.py`. A module exposes an
async function named the same as the file, accepts `(phone, client, out)`, and
appends a result dictionary to `out`.

Minimal result fields:

```python
{
    "name": "module_name",
    "domain": "example.com",
    "frequent_rate_limit": False,
    "rateLimit": False,
    "sent": True,
    "error": False,
}
```

Use `reason` for human-readable failure or rate-limit details.

# Inspired by

This project was inspired by OTP Bomber by Ashar Khalil. KABOOM is simply an extension of that concept, adding a few quality-of-life improvements like a terminal UI, concurrent execution, proxy rotation, repeat rounds, and broader module support. The original did the heavy lifting; this just tries to make it a little more flexible. Credit where it's due.