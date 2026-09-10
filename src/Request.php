<?php

namespace Weft;

/**
 * Immutable HTTP request for Client::send().
 *
 * $pool / $label are optional context for SendHooks (rate limits, metrics).
 * Callers that do not need them leave them empty.
 */
final class Request {
	/**
	 * @param array<string, mixed> $body JSON body (empty for GET/DELETE without body)
	 * @param string $pool Rate-limit / quota pool key for hooks
	 * @param string $label Display label for hooks (e.g. endpoint name)
	 */
	public function __construct(
		public readonly string $method,
		public readonly string $url,
		public readonly array $body = [],
		public readonly string $pool = '',
		public readonly string $label = '',
	) {}
}
