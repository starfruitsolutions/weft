<?php

namespace Weft;

/**
 * Immutable HTTP request for Client::send().
 *
 * $context is opaque to the client — SendHooks may read keys they care about.
 * Leave empty when unused.
 */
final class Request {
	/**
	 * @param array<string, mixed> $body JSON body (empty for GET/DELETE without body)
	 * @param array<string, mixed> $context Optional hook/caller metadata
	 */
	public function __construct(
		public readonly string $method,
		public readonly string $url,
		public readonly array $body = [],
		public readonly array $context = [],
	) {}
}
