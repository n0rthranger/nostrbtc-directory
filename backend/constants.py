"""Shared constants used across backend modules."""

# Reserved NIP-05 names that cannot be claimed by users
NIP05_RESERVED = {
    "admin", "administrator", "root", "support", "help", "info", "contact",
    "security", "abuse", "postmaster", "webmaster", "mail", "email",
    "nostrbtc", "relay", "api", "www", "ftp", "ssh", "test", "dev",
    "bot", "system", "noreply", "no_reply", "dobby",
}

# Reserved email usernames
EMAIL_RESERVED = {
    "admin", "administrator", "postmaster", "abuse", "root", "mail",
    "support", "help", "info", "contact", "security", "noreply", "test",
    "null", "bridge", "relay", "nostrbtc", "hostmaster", "webmaster",
    "billing", "newsletter", "mailer.daemon",
    "bot", "system", "www", "ftp", "ssh", "dev", "no_reply", "dobby",
}
