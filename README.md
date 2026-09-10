# Weft

Concurrent HTTP for PHP 8.4+. On a loom, the weft is the yarn that runs across the warp. Here, requests and workflows are the threads: they share one `curl_multi` handle and resume as they finish.

**Direct:** several independent requests, one hop. Pass them all to `send()`.

```php
use Weft\Client;
use Weft\Request;

$client = new Client();

[$a, $b] = $client->send(
	new Request(method: 'GET', url: 'https://example.com/a'),
	new Request(method: 'GET', url: 'https://example.com/b'),
);
```

**Composed:** several multi-step procedures that may branch between hops. Pass a closure per procedure to `run()`. Each closure is sequential; `send()` inside it suspends so the other closures can keep going.

```php
[$claimA, $claimB] = $client->run(
	fn() => $this->releaseClaim($client, $idA),
	fn() => $this->releaseClaim($client, $idB),
);

function releaseClaim(Client $client, string $id): Result {
	[$release] = $client->send(new Request(
		method: 'POST',
		url: "https://api.example.com/claims/{$id}/release",
	));
	if (!$release->isError()) return $release;

	[$retry] = $client->send(new Request(
		method: 'POST',
		url: "https://api.example.com/claims/{$id}/release",
		body: ['asRecordingOwner' => true],
	));
	return $retry;
}
```

Fan-out of requests → `send(...)`. Fan-out of procedures → `run(fn() => ...)`.

## Install

```bash
composer require weft/weft
```

Until the package is on Packagist, add the GitHub repo:

```json
{
	"repositories": [
		{
			"type": "vcs",
			"url": "https://github.com/starfruitsolutions/weft"
		}
	],
	"require": {
		"php": ">=8.4",
		"weft/weft": "dev-main"
	}
}
```

Requires PHP 8.4+, `ext-curl`, and `ext-json`.

## Requests

Bodies are JSON. `context` is opaque to the client — hooks may read keys they care about.

```php
$request = new Request(
	method: 'POST',
	url: 'https://api.example.com/items',
	body: ['name' => 'Track'],
	context: ['pool' => 'writes', 'label' => 'createItem'],
);
```

## Results

JSON object responses are `ArrayAccess`. Non-JSON bodies and empty responses become an `error` payload. `httpCode` is the transport status (`0` on failure).

```php
$result = $client->send($request)[0];

if ($result->isError()) {
	// $result['error']
}

$code = $result->httpCode;
$array = $result->toArray();
```

`Result::QuotaExceeded($pool, $retryAt)` builds a 429 without performing HTTP, for hooks that deny a send up front.

## Workflows

`run()` starts one fiber per callable. A workflow that throws is stored as that `Throwable` at its index; `run()` itself does not throw, so sibling results stay available.

Inside a workflow, `send(...$requests)` suspends until every handle in *that* call finishes. Other workflows keep transferring on the same `curl_multi`. A `send()` with several arguments is still one hop: those requests complete together before the closure continues.

`send()` outside `run()` is a one-shot of that same hop.

Workflow concurrency (not request count) defaults to 25:

```php
$client->setConcurrency(50);
```

## Retries

Extra attempts for transport failures (`httpCode` 0) and 5xx. Default is 0. Retries re-send; they do not re-run `SendHook::before()`.

```php
$client = new Client(retries: 2);
// or later: $client->setRetries(2);
```

## Hooks

`SendHook` is a before/after seam. `before()` returning a `Result` skips curl. `after()` runs only for requests that actually transferred.

```php
use Weft\Request;
use Weft\Result;
use Weft\SendHook;

final class RateLimitHook implements SendHook {
	public function before(Request $request): ?Result {
		$pool = $request->context['pool'] ?? '';
		if ($this->isExhausted($pool)) return Result::QuotaExceeded(pool: $pool);
		return null;
	}

	public function after(Request $request, Result $result, float $durationMs): void {
		$this->record($request, $result, $durationMs);
	}
}

$client = new Client(hook: new RateLimitHook());
```

Default headers are a callable invoked per handle (auth tokens, etc.):

```php
$client = new Client(
	defaultHeaders: fn(): array => ['Authorization: Bearer ' . $this->token()],
);
```

Timeouts default to 30s / 10s connect. Pass `timeout` and `connectTimeout` to the constructor. Transfers use HTTP/2 over TLS when curl supports it, and accept compressed encodings.

## License

MIT
