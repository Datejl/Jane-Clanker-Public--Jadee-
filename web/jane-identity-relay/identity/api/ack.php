<?php
declare(strict_types=1);
require dirname(__DIR__) . '/_relay.php';

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    ji_relay_json(['ok' => false, 'error' => 'Method not allowed.'], 405);
}

ji_relay_require_auth();
$raw = file_get_contents('php://input') ?: '';
$payload = json_decode($raw, true);
if (!is_array($payload)) {
    ji_relay_json(['ok' => false, 'error' => 'Invalid JSON body.'], 400);
}
$ids = $payload['ids'] ?? [];
if (!is_array($ids)) {
    ji_relay_json(['ok' => false, 'error' => 'ids must be an array.'], 400);
}
$acked = ji_relay_ack_items($ids);
ji_relay_json([
    'ok' => true,
    'acked' => $acked,
]);

