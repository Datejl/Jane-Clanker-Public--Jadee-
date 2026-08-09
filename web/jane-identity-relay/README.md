# Jane Identity Relay

quick notes for the website version of jane identity

MEMORIAS already has a newer integrated relay under its own `identity/` folder. use that copy for MEMORIAS deployments; this folder is for a separate standalone host.

## upload

upload the stuff in this folder to the site root.

these should work after:

- `/identity/`
- `/identity/privacy.php`
- `/identity/terms.php`
- `/identity/roblox/callback/`
- `/identity/api/pending.php`
- `/identity/api/ack.php`

## config

copy:

```text
config.sample.php
```

to:

```text
config.php
```

then put the relay token in it.

Jane needs the same token. easiest way:

```text
!janesecrets setup-identity-relay https://your-site.example
```

Jane will spit out the `config.php` snippet. paste that into the site.

Jane keeps this shared value as `JANE_IDENTITY_RELAY_API_TOKEN`. it is separate from the token John uses to call Jane's own identity API.

also set these if they are missing:

```text
!janesecrets set roblox-oauth-client-id <id>
!janesecrets set roblox-oauth-client-secret <secret>
```

## roblox oauth urls

```text
Entry URL: https://your-site.example/identity/
Privacy URL: https://your-site.example/identity/privacy.php
Terms URL: https://your-site.example/identity/terms.php
Redirect URI: https://your-site.example/identity/roblox/callback/
```

the redirect uri has to match exactly.

## what this does

the website just catches roblox sending the user back.

jane polls `/identity/api/pending.php`, handles the actual roblox oauth stuff herself, then calls `/identity/api/ack.php` when done.

so the website does not need the roblox secret.
