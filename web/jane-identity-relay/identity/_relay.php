<?php
declare(strict_types=1);

function ji_relay_root(): string
{
    return dirname(__DIR__);
}

function ji_relay_config(): array
{
    static $config = null;
    if (is_array($config)) {
        return $config;
    }

    $configPath = ji_relay_root() . DIRECTORY_SEPARATOR . 'config.php';
    $loaded = [];
    if (is_file($configPath)) {
        $candidate = require $configPath;
        if (is_array($candidate)) {
            $loaded = $candidate;
        }
    }

    $config = [
        'relay_token' => trim((string)($loaded['relay_token']
            ?? (getenv('JANE_IDENTITY_RELAY_API_TOKEN')
                ?: (getenv('JANE_IDENTITY_API_TOKEN') ?: '')))),
        'queue_path' => trim((string)($loaded['queue_path'] ?? '')),
        'retention_seconds' => max(300, (int)($loaded['retention_seconds'] ?? 21600)),
    ];
    return $config;
}

function ji_relay_queue_path(): string
{
    $config = ji_relay_config();
    $configured = trim((string)$config['queue_path']);
    if ($configured !== '') {
        return $configured;
    }
    return ji_relay_root() . DIRECTORY_SEPARATOR . 'identity-relay-data' . DIRECTORY_SEPARATOR . 'pending.jsonl';
}

function ji_relay_json(array $payload, int $status = 200): void
{
    http_response_code($status);
    header('Content-Type: application/json; charset=utf-8');
    header('Cache-Control: no-store');
    echo json_encode($payload, JSON_UNESCAPED_SLASHES);
    exit;
}

function ji_relay_text(string $title, string $body): void
{
    http_response_code(200);
    header('Content-Type: text/html; charset=utf-8');
    header('Cache-Control: no-store');
    $safeTitle = htmlspecialchars($title, ENT_QUOTES, 'UTF-8');
    echo '<!doctype html><html lang="en"><head><meta charset="utf-8">';
    echo '<meta name="viewport" content="width=device-width, initial-scale=1">';
    echo '<title>' . $safeTitle . '</title>';
    echo '<style>';
    echo 'body{margin:0;background:#f7f4ef;color:#242322;font-family:system-ui,-apple-system,Segoe UI,sans-serif;line-height:1.5}';
    echo 'main{max-width:760px;margin:0 auto;padding:48px 20px}';
    echo 'h1{font-size:32px;margin:0 0 16px}';
    echo 'a{color:#245c73}';
    echo '.panel{background:#fff;border:1px solid #ddd7ce;border-radius:8px;padding:24px}';
    echo '.muted{color:#69645d}';
    echo '</style></head><body><main><div class="panel">';
    echo '<h1>' . $safeTitle . '</h1>';
    echo $body;
    echo '</div></main></body></html>';
    exit;
}

function ji_relay_authorized(): bool
{
    $expected = (string)ji_relay_config()['relay_token'];
    if ($expected === '') {
        return false;
    }

    $headers = function_exists('getallheaders') ? getallheaders() : [];
    $authorization = '';
    foreach ($headers as $name => $value) {
        if (strtolower((string)$name) === 'authorization') {
            $authorization = trim((string)$value);
            break;
        }
    }

    $supplied = '';
    if (stripos($authorization, 'bearer ') === 0) {
        $supplied = trim(substr($authorization, 7));
    }
    if ($supplied === '') {
        $supplied = trim((string)($_SERVER['HTTP_X_JANE_IDENTITY_TOKEN'] ?? ''));
    }
    return $supplied !== '' && hash_equals($expected, $supplied);
}

function ji_relay_require_auth(): void
{
    if (!ji_relay_authorized()) {
        ji_relay_json(['ok' => false, 'error' => 'Unauthorized.'], 401);
    }
}

function ji_relay_ensure_queue_dir(): void
{
    $queuePath = ji_relay_queue_path();
    $dir = dirname($queuePath);
    if (!is_dir($dir) && !mkdir($dir, 0750, true) && !is_dir($dir)) {
        ji_relay_json(['ok' => false, 'error' => 'Could not create relay queue directory.'], 500);
    }
}

function ji_relay_read_rows($handle): array
{
    rewind($handle);
    $rows = [];
    while (($line = fgets($handle)) !== false) {
        $line = trim($line);
        if ($line === '') {
            continue;
        }
        $row = json_decode($line, true);
        if (is_array($row)) {
            $rows[] = $row;
        }
    }
    return $rows;
}

function ji_relay_write_rows($handle, array $rows): void
{
    rewind($handle);
    ftruncate($handle, 0);
    foreach ($rows as $row) {
        fwrite($handle, json_encode($row, JSON_UNESCAPED_SLASHES) . "\n");
    }
    fflush($handle);
}

function ji_relay_pruned_rows(array $rows): array
{
    $retention = (int)ji_relay_config()['retention_seconds'];
    $cutoff = time() - max(300, $retention);
    $out = [];
    foreach ($rows as $row) {
        $created = strtotime((string)($row['createdAt'] ?? '')) ?: time();
        if ($created >= $cutoff) {
            $out[] = $row;
        }
    }
    return $out;
}

function ji_relay_store_callback(array $payload): string
{
    ji_relay_ensure_queue_dir();
    $queuePath = ji_relay_queue_path();
    $relayId = bin2hex(random_bytes(16));
    $payload['relayId'] = $relayId;
    $payload['createdAt'] = gmdate('c');

    $handle = fopen($queuePath, 'c+');
    if ($handle === false) {
        ji_relay_json(['ok' => false, 'error' => 'Could not open relay queue.'], 500);
    }
    try {
        flock($handle, LOCK_EX);
        $rows = ji_relay_pruned_rows(ji_relay_read_rows($handle));
        $rows[] = $payload;
        ji_relay_write_rows($handle, $rows);
        flock($handle, LOCK_UN);
    } finally {
        fclose($handle);
    }
    return $relayId;
}

function ji_relay_pending_items(int $limit): array
{
    ji_relay_ensure_queue_dir();
    $queuePath = ji_relay_queue_path();
    $handle = fopen($queuePath, 'c+');
    if ($handle === false) {
        ji_relay_json(['ok' => false, 'error' => 'Could not open relay queue.'], 500);
    }
    try {
        flock($handle, LOCK_EX);
        $rows = ji_relay_pruned_rows(ji_relay_read_rows($handle));
        ji_relay_write_rows($handle, $rows);
        flock($handle, LOCK_UN);
    } finally {
        fclose($handle);
    }
    return array_slice($rows, 0, max(1, min($limit, 25)));
}

function ji_relay_ack_items(array $ids): int
{
    $wanted = [];
    foreach ($ids as $id) {
        $clean = trim((string)$id);
        if ($clean !== '') {
            $wanted[$clean] = true;
        }
    }
    if (!$wanted) {
        return 0;
    }

    ji_relay_ensure_queue_dir();
    $queuePath = ji_relay_queue_path();
    $handle = fopen($queuePath, 'c+');
    if ($handle === false) {
        ji_relay_json(['ok' => false, 'error' => 'Could not open relay queue.'], 500);
    }

    $acked = 0;
    try {
        flock($handle, LOCK_EX);
        $rows = ji_relay_pruned_rows(ji_relay_read_rows($handle));
        $kept = [];
        foreach ($rows as $row) {
            $relayId = trim((string)($row['relayId'] ?? ''));
            if ($relayId !== '' && isset($wanted[$relayId])) {
                $acked++;
                continue;
            }
            $kept[] = $row;
        }
        ji_relay_write_rows($handle, $kept);
        flock($handle, LOCK_UN);
    } finally {
        fclose($handle);
    }
    return $acked;
}
