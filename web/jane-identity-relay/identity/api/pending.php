<?php
declare(strict_types=1);
require dirname(__DIR__) . '/_relay.php';

if ($_SERVER['REQUEST_METHOD'] !== 'GET') {
    ji_relay_json(['ok' => false, 'error' => 'Method not allowed.'], 405);
}

ji_relay_require_auth();
$limit = (int)($_GET['limit'] ?? 10);
ji_relay_json([
    'ok' => true,
    'items' => ji_relay_pending_items($limit),
]);

