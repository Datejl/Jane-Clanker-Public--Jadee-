<?php
declare(strict_types=1);
require __DIR__ . '/_relay.php';

ji_relay_text(
    'Jane Identity Privacy Policy',
    '<p>Jane Identity stores the minimum data needed to connect Discord members with their Roblox accounts.</p>'
    . '<h2>Data Jane stores</h2>'
    . '<ul><li>Discord user ID.</li><li>Discord server ID where verification started.</li><li>Roblox user ID and username returned by Roblox OAuth.</li><li>Operational timestamps used for verification and maintenance.</li></ul>'
    . '<h2>How Jane uses the data</h2>'
    . '<ul><li>To verify that a Discord member controls a Roblox account.</li><li>To apply configured Discord nicknames and roles.</li><li>To provide authorized internal identity lookup for related bots.</li></ul>'
    . '<h2>Removal</h2>'
    . '<p>Users can run <code>/unlink</code> in Discord to remove their stored Jane Identity link.</p>'
);

