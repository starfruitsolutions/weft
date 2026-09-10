<?php

namespace Weft;

use CurlHandle;
use CurlMultiHandle;
use Fiber;
use Throwable;

/**
 * Fiber scheduler over one long-lived curl_multi handle.
 *
 * Workflows are plain callables that call request(); a request suspends the
 * workflow, its handle joins others already in flight, and it resumes as soon
 * as its own handle finishes — no barrier across workflows.
 *
 *   $weft->run(
 *       fn() => $weft->request(method: 'GET', url: 'https://example.com/a'),
 *       fn() => $weft->request(method: 'GET', url: 'https://example.com/b'),
 *   );
 */
final class Weft {
	public const int DEFAULT_CONCURRENCY = 25;
	public const float DEFAULT_TIMEOUT = 30.0;
	public const float DEFAULT_CONNECT_TIMEOUT = 10.0;
	private const float SELECT_TIMEOUT = 0.5;

	private ?CurlMultiHandle $multi = null;

	/**
	 * Fiber id => workflow state.
	 *
	 * @var array<int, array{
	 *   index: int,
	 *   fiber: Fiber,
	 *   result: ?Result,
	 *   pending: int,
	 * }>
	 */
	private array $tasks = [];

	/**
	 * Curl handle id => in-flight slot.
	 *
	 * @var array<int, array{
	 *   fiberId: int,
	 *   request: Request,
	 *   attempt: int,
	 *   startedAt: float,
	 * }>
	 */
	private array $inFlight = [];

	private int $concurrency = self::DEFAULT_CONCURRENCY;

	/**
	 * @param SendHook|null $hook Optional before/after seam (e.g. a rate-limit hook).
	 * @param int $retries Extra attempts for transport failures and 5xx responses.
	 * @param callable():list<string>|null $defaultHeaders Extra headers on every request (e.g. an auth header provider).
	 * @param float $timeout CURLOPT_TIMEOUT seconds.
	 * @param float $connectTimeout CURLOPT_CONNECTTIMEOUT seconds.
	 */
	public function __construct(
		private ?SendHook $hook = null,
		private int $retries = 0,
		private mixed $defaultHeaders = null,
		private float $timeout = self::DEFAULT_TIMEOUT,
		private float $connectTimeout = self::DEFAULT_CONNECT_TIMEOUT,
	) {
		$this->retries = max(0, $retries);
	}

	/**
	 * @api
	 */
	public function setRetries(int $retries): self {
		$this->retries = max(0, $retries);
		return $this;
	}

	/**
	 * Max workflows started at once inside run(). Caps tasks in flight, not requests.
	 *
	 * @api
	 */
	public function setConcurrency(int $concurrency): self {
		$this->concurrency = max(1, $concurrency);
		return $this;
	}

	/**
	 * One HTTP call. Inside run() this suspends the fiber until the handle
	 * finishes (and retries transport/5xx per setRetries). Outside run() it is
	 * a one-shot run of a single request.
	 *
	 * @param array<string, mixed> $body
	 * @param array<string, mixed> $context Opaque hook/caller metadata
	 */
	public function request(string $method, string $url, array $body = [], array $context = []): Result {
		$request = new Request(method: $method, url: $url, body: $body, context: $context);

		$fiber = Fiber::getCurrent();
		if ($fiber !== null && isset($this->tasks[spl_object_id($fiber)])) {
			return Fiber::suspend($request);
		}

		return $this->run(fn() => $this->request(
			method: $method,
			url: $url,
			body: $body,
			context: $context,
		))[0];
	}

	/**
	 * Run workflows concurrently. Each is a plain callable that calls request().
	 * Failed workflows yield their Throwable as the result for that index — run
	 * itself does not throw, so sibling results stay available.
	 *
	 * @return list<mixed> return values (or Throwable) in workflow order
	 */
	public function run(callable ...$workflows): array {
		if (!$workflows) return [];

		$this->multi ??= curl_multi_init();

		$queue = array_values($workflows);
		/** @var array<int, int> workflow index => fiber id */
		$active = [];
		$results = [];
		$next = 0;
		$total = count($queue);

		$startNext = function () use (&$queue, &$active, &$next, &$results, $total): void {
			while (count($active) < $this->concurrency && $next < $total) {
				$i = $next++;
				$fiber = new Fiber($queue[$i]);
				$fiberId = spl_object_id($fiber);
				$this->tasks[$fiberId] = [
					'index' => $i,
					'fiber' => $fiber,
					'result' => null,
					'pending' => 0,
				];
				$active[$i] = $fiberId;
				$this->advance(fiberId: $fiberId, step: fn(Fiber $f) => $f->start(), active: $active, results: $results);
			}
		};

		$startNext();

		while ($active || $this->inFlight) {
			if ($this->inFlight) $this->tick(active: $active, results: $results);
			$startNext();
		}

		ksort($results);
		return array_values($results);
	}

	/**
	 * @param array<int, int> $active
	 * @param array<int, mixed> $results
	 */
	private function advance(int $fiberId, callable $step, array &$active, array &$results): void {
		while (true) {
			$task = &$this->tasks[$fiberId];
			$fiber = $task['fiber'];

			try {
				$suspended = $step($fiber);
			} catch (Throwable $e) {
				$results[$task['index']] = $e;
				unset($active[$task['index']], $this->tasks[$fiberId]);
				return;
			}

			if ($fiber->isTerminated()) {
				$results[$task['index']] = $fiber->getReturn();
				unset($active[$task['index']], $this->tasks[$fiberId]);
				return;
			}

			$denied = $this->enqueue(fiberId: $fiberId, request: $suspended);
			if ($task['pending'] > 0) return;

			$step = fn(Fiber $f) => $f->resume($denied);
		}
	}

	/**
	 * @return Result|null Immediate result when the hook skips HTTP; null when a handle was queued.
	 */
	private function enqueue(int $fiberId, Request $request): ?Result {
		$task = &$this->tasks[$fiberId];
		$task['result'] = null;
		$task['pending'] = 0;

		if ($this->hook !== null) {
			$skip = $this->hook->before($request);
			if ($skip !== null) return $skip;
		}

		$this->addHandle(fiberId: $fiberId, request: $request, attempt: 1);
		return null;
	}

	private function addHandle(int $fiberId, Request $request, int $attempt): void {
		$handle = $this->createHandle(request: $request);
		curl_multi_add_handle($this->multi, $handle);
		$this->inFlight[spl_object_id($handle)] = [
			'fiberId' => $fiberId,
			'request' => $request,
			'attempt' => $attempt,
			'startedAt' => microtime(true),
		];
		$this->tasks[$fiberId]['pending']++;
	}

	/**
	 * @param array<int, int> $active
	 * @param array<int, mixed> $results
	 */
	private function tick(array &$active, array &$results): void {
		curl_multi_exec($this->multi, $running);

		$reaped = false;
		while ($info = curl_multi_info_read($this->multi)) {
			if ($info['msg'] !== CURLMSG_DONE) continue;
			$this->complete(handle: $info['handle'], active: $active, results: $results);
			$reaped = true;
		}

		if (!$reaped && $running) curl_multi_select($this->multi, timeout: self::SELECT_TIMEOUT);
	}

	/**
	 * @param array<int, int> $active
	 * @param array<int, mixed> $results
	 */
	private function complete(CurlHandle $handle, array &$active, array &$results): void {
		$id = spl_object_id($handle);
		$slot = $this->inFlight[$id];
		unset($this->inFlight[$id]);

		$body = curl_multi_getcontent($handle) ?: '';
		$httpCode = (int) curl_getinfo($handle, CURLINFO_RESPONSE_CODE);
		$durationMs = (microtime(true) - $slot['startedAt']) * 1000;
		curl_multi_remove_handle($this->multi, $handle);
		curl_close($handle);

		$result = $this->decode(body: $body, httpCode: $httpCode);
		$this->hook?->after($slot['request'], $result, $durationMs);

		$fiberId = $slot['fiberId'];
		$maxAttempts = $this->retries + 1;

		if ($slot['attempt'] < $maxAttempts && $this->isRetryable($result)) {
			$this->tasks[$fiberId]['pending']--;
			$this->addHandle(
				fiberId: $fiberId,
				request: $slot['request'],
				attempt: $slot['attempt'] + 1,
			);
			return;
		}

		$this->tasks[$fiberId]['result'] = $result;
		$this->tasks[$fiberId]['pending']--;
		$this->maybeResume(fiberId: $fiberId, active: $active, results: $results);
	}

	/**
	 * @param array<int, int> $active
	 * @param array<int, mixed> $results
	 */
	private function maybeResume(int $fiberId, array &$active, array &$results): void {
		if ($this->tasks[$fiberId]['pending'] > 0) return;

		$result = $this->tasks[$fiberId]['result'];
		$this->advance(
			fiberId: $fiberId,
			step: fn(Fiber $fiber) => $fiber->resume($result),
			active: $active,
			results: $results,
		);
	}

	/**
	 * No usable response (transport failure / timeout) or server error.
	 */
	private function isRetryable(Result $result): bool {
		$code = $result->httpCode ?? 0;
		return $code === 0 || $code >= 500;
	}

	private function decode(string $body, int $httpCode): Result {
		$decoded = json_decode($body, true);
		return Result::FromBody(
			body: is_array($decoded) ? $decoded : [
				'error' => '<pre>' . ($body !== '' ? $body : 'No response.') . '</pre>',
				'response_code' => $httpCode,
			],
			httpCode: $httpCode,
		);
	}

	private function createHandle(Request $request): CurlHandle {
		$headers = [];
		if ($this->defaultHeaders !== null) {
			$headers = ($this->defaultHeaders)();
		}

		$options = [
			CURLOPT_HTTPGET => true,
			CURLOPT_HTTPHEADER => $headers,
			CURLOPT_POST => false,
			CURLOPT_RETURNTRANSFER => true,
			CURLOPT_URL => $request->url,
			CURLOPT_TIMEOUT => $this->timeout,
			CURLOPT_CONNECTTIMEOUT => $this->connectTimeout,
			CURLOPT_ENCODING => '',
			CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_2TLS,
		];

		$method = strtoupper($request->method);
		$hasBody = $request->body !== [];

		if ($method !== 'GET' || $hasBody) {
			if ($method !== 'POST') $options[CURLOPT_CUSTOMREQUEST] = $method;
			if ($hasBody || $method === 'POST') {
				$postString = json_encode($request->body);
				$options[CURLOPT_POST] = true;
				$options[CURLOPT_POSTFIELDS] = $postString;
				$options[CURLOPT_HTTPHEADER][] = 'Content-Type: application/json';
				$options[CURLOPT_HTTPHEADER][] = 'Content-Length: ' . strlen($postString);
				if ($method !== 'POST') $options[CURLOPT_CUSTOMREQUEST] = $method;
			} elseif (in_array($method, ['DELETE', 'PATCH', 'PUT'], true)) {
				$options[CURLOPT_CUSTOMREQUEST] = $method;
			}
		}

		$handle = curl_init();
		curl_setopt_array($handle, $options);
		return $handle;
	}
}
