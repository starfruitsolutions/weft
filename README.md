# Weft

Concurrent HTTP for PHP 8.4+. On a loom, the weft is the yarn that runs across the warp. Here, workflows share one `curl_multi` handle and resume as their own transfers finish.

`request()` is one HTTP call. `run()` overlaps several callables that each call `request()`.

**Direct:** several independent requests. One `request()` per closure.

```php
use Weft\Weft;

$weft = new Weft();

[$a, $b] = $weft->run(
	fn() => $weft->request(method: 'GET', url: 'https://example.com/a'),
	fn() => $weft->request(method: 'GET', url: 'https://example.com/b'),
);
```

**Composed:** several multi-step procedures that may branch between hops. Each closure is sequential; `request()` inside it suspends so the other closures can keep going.

```php
[$orderA, $orderB] = $weft->run(
	fn() => $this->submitOrder($weft, $idA),
	fn() => $this->submitOrder($weft, $idB),
);

function submitOrder(Weft $weft, string $id): Result {
	$submit = $weft->request(
		method: 'POST',
		url: "https://api.example.com/orders/{$id}",
	);
	if (!$submit->isError()) return $submit;

	return $weft->request(
		method: 'POST',
		url: "https://api.example.com/orders/{$id}",
		body: ['force' => true],
	);
}
```

`request()` outside `run()` is a one-shot of a single call.

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

## request()

Bodies are JSON. `context` is opaque to Weft — hooks may read keys they care about.

```php
$result = $weft->request(
	method: 'POST',
	url: 'https://api.example.com/items',
	body: ['name' => 'Widget'],
	context: ['pool' => 'writes', 'label' => 'createItem'],
);
```

JSON object responses are `ArrayAccess`. Non-JSON bodies and empty responses become an `error` payload. `httpCode` is the transport status (`0` on failure).

```php
if ($result->isError()) {
	// $result['error']
}

$code = $result->httpCode;
$array = $result->toArray();
```

`Result::QuotaExceeded($pool, $retryAt)` builds a 429 without performing HTTP, for hooks that deny a request up front.

## Workflows

`run()` starts one fiber per callable. A workflow that throws is stored as that `Throwable` at its index; `run()` itself does not throw, so sibling results stay available.

Inside a workflow, `request()` suspends until that handle finishes. Other workflows keep transferring on the same `curl_multi`.

Workflow concurrency (not request count) defaults to 25:

```php
$weft->setConcurrency(50);
```

## Retries

Extra attempts for transport failures (`httpCode` 0) and 5xx. Default is 0. Retries re-send; they do not re-run `SendHook::before()`.

```php
$weft = new Weft(retries: 2);
// or later: $weft->setRetries(2);
```

## Hooks

`SendHook` is a before/after seam. `before()` returning a `Result` skips curl. `after()` runs only for requests that actually transferred. Hooks see the internal `Request` object.

```php
use Weft\Request;
use Weft\Result;
use Weft\SendHook;
use Weft\Weft;

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

$weft = new Weft(hook: new RateLimitHook());
```

Default headers are a callable invoked per handle (auth tokens, etc.):

```php
$weft = new Weft(
	defaultHeaders: fn(): array => ['Authorization: Bearer ' . $this->token()],
);
```

Timeouts default to 30s / 10s connect. Pass `timeout` and `connectTimeout` to the constructor. Transfers use HTTP/2 over TLS when curl supports it, and accept compressed encodings.

## License

MIT
