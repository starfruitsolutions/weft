# Weft

Concurrent HTTP for PHP 8.4+. Workflows are plain callables that call `send()`; each send suspends the fiber, joins one shared `curl_multi` handle, and resumes as soon as its own transfers finish — no barrier across workflows.

```php
use Weft\Client;
use Weft\Request;

$client = new Client();

$results = $client->run(
	fn() => $client->send(new Request(method: 'GET', url: 'https://example.com/a')),
	fn() => $client->send(new Request(method: 'GET', url: 'https://example.com/b')),
);
```

## Install

```bash
composer require weft/weft
```

## Requirements

- PHP 8.4+
- `ext-curl`
- `ext-json`

## License

MIT
