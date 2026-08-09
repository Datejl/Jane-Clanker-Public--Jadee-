<?php
declare(strict_types=1);
require __DIR__ . '/_relay.php';

ji_relay_text(
    'Jane Identity',
    '<p>Jane Identity links your Discord account to the Roblox account you authorize through Roblox OAuth.</p>'
    . '<p>Start verification from Discord with <code>/verify</code>. Jane will give you a one-time Roblox sign-in link.</p>'
    . '<p class="muted">Jane never asks for your Roblox password. Roblox handles sign-in and consent.</p>'
    . '<p><a href="/identity/privacy.php">Privacy</a> | <a href="/identity/terms.php">Terms</a></p>'
);

