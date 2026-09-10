<?php

namespace Weft;

use ArrayAccess;
use JsonSerializable;
use LogicException;

/**
 * Decoded HTTP response body. ArrayAccess mirrors a JSON object shape.
 *
 * @implements ArrayAccess<string, mixed>
 */
final class Result implements ArrayAccess, JsonSerializable {
	/**
	 * @param array<string, mixed> $body
	 */
	public function __construct(
		private array $body,
		public readonly ?int $httpCode = null,
	) {}

	/**
	 * @param array<string, mixed> $body
	 */
	public static function FromBody(array $body, ?int $httpCode = null): self {
		return new self(body: $body, httpCode: $httpCode);
	}

	/**
	 * Fail-fast quota denial (no HTTP performed).
	 *
	 * @param ?int $retryAt unix timestamp when the caller may try again
	 */
	public static function QuotaExceeded(string $pool, ?int $retryAt = null): self {
		$error = [
			'code' => 429,
			'message' => $pool !== ''
				? "API quota exceeded for pool {$pool}"
				: 'API quota exceeded',
		];
		if ($retryAt !== null) $error['retryAt'] = $retryAt;

		return new self(body: ['error' => $error], httpCode: 429);
	}

	/**
	 * @return array<string, mixed>
	 */
	public function toArray(): array {
		return $this->body;
	}

	public function isError(): bool {
		return isset($this->body['error']);
	}

	public function offsetExists(mixed $offset): bool {
		return array_key_exists($offset, $this->body);
	}

	public function offsetGet(mixed $offset): mixed {
		return $this->body[$offset] ?? null;
	}

	/**
	 * @throws LogicException Always — Result is immutable
	 */
	public function offsetSet(mixed $offset, mixed $value): void {
		throw new LogicException('Result is immutable');
	}

	/**
	 * @throws LogicException Always — Result is immutable
	 */
	public function offsetUnset(mixed $offset): void {
		throw new LogicException('Result is immutable');
	}

	/**
	 * @return array<string, mixed>
	 */
	public function jsonSerialize(): array {
		return $this->body;
	}
}
