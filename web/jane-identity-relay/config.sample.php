<?php
return [
    // Shared secret used by Jane when polling /identity/api/pending.php and /identity/api/ack.php.
    // Use a long random value and set the same value as Jane's JANE_IDENTITY_RELAY_API_TOKEN.
    'relay_token' => 'replace-with-a-long-random-token',

    // Optional. Defaults to ../identity-relay-data/pending.jsonl relative to /identity/_relay.php.
    // In Plesk, prefer a path outside the document root if you have one.
    'queue_path' => '',

    // Optional. Old unprocessed callbacks are pruned after this many seconds.
    'retention_seconds' => 21600,
];
