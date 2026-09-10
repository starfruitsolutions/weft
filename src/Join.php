<?php

namespace Weft;

/**
 * Suspend value for a nested Weft::run() — parent waits until these child fibers finish.
 *
 * @internal
 */
final class Join {
	/**
	 * @param list<int> $childIds
	 */
	public function __construct(
		public readonly array $childIds,
	) {}
}
