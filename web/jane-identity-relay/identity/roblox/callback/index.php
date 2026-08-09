<?php
declare(strict_types=1);
require dirname(__DIR__, 2) . '/_relay.php';

$state = trim((string)($_GET['state'] ?? ''));
$code = trim((string)($_GET['code'] ?? ''));
$error = trim((string)($_GET['error'] ?? ''));
$errorDescription = trim((string)($_GET['error_description'] ?? $error));

if ($state === '') {
    ji_relay_text(
        'Verification Could Not Continue',
        '<p>Roblox did not return a verification state. Please return to Discord and run <code>/verify</code> again.</p>'
    );
}

ji_relay_store_callback([
    'state' => $state,
    'code' => $code,
    'error' => $error,
    'errorDescription' => $errorDescription,
]);

ji_relay_text(
    'Return To Discord',
    '<p>Your Roblox response was received. You can return to Discord now.</p>'
    . '<p class="muted">Jane will finish the link shortly and DM you the result.</p>'
    . '<script>setTimeout(function(){try{window.close();}catch(e){}}, 750);</script>'
);

