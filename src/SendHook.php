<?php

namespace Weft;

/**
 * Optional before/after seam around each HTTP request.
 * before() returning a Result skips curl and delivers that result to the fiber.
 * after() runs only for requests that actually transferred.
 */
interface SendHook {
	/**
	 * @return Result|null null to proceed with HTTP
	 */
	public function before(Request $request): ?Result;

	/**
	 * @param float $durationMs wall time for the HTTP transfer
	 */
	public function after(Request $request, Result $result, float $durationMs): void;
}
