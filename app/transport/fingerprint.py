"""Per-account device fingerprints.

Telethon sends device_model / system_version / app_version on every connect, and
Telegram ties them to the session. Giving every account the same hand-made
signature (the old "FE AutoSender / Python") makes a set of accounts trivial to
cluster and ban as a farm. Instead each account gets a realistic, plausible
profile once, at login, and reuses it for the life of the session.

A profile is generated before the login request and persisted next to the
session, so the device the session was authorized with is exactly the one used
later. Existing accounts with no stored profile keep the legacy default rather
than switching device mid-session (a device change on a live session is itself
a flag).
"""

import json
import random

# (device_model, system_version, app_version) — common consumer phones and a
# recent Telegram client build for each, so the triple reads as a real install.
_PROFILES = [
    ("Samsung SM-G991B", "Android 13 (API 33)", "Telegram Android 10.14.5"),
    ("Samsung SM-A515F", "Android 12 (API 31)", "Telegram Android 10.9.2"),
    ("Samsung SM-S918B", "Android 14 (API 34)", "Telegram Android 10.14.4"),
    ("Xiaomi 2201123G", "Android 13 (API 33)", "Telegram Android 10.13.0"),
    ("Redmi Note 11", "Android 12 (API 31)", "Telegram Android 10.8.2"),
    ("Google Pixel 7", "Android 14 (API 34)", "Telegram Android 10.14.0"),
    ("OnePlus 9", "Android 13 (API 33)", "Telegram Android 10.11.0"),
    ("iPhone13,2", "iOS 17.1.2", "Telegram iOS 10.9"),
    ("iPhone14,5", "iOS 16.6.1", "Telegram iOS 10.4"),
    ("iPhone15,2", "iOS 17.2", "Telegram iOS 10.12"),
]

LEGACY_DEVICE = {
    "device_model": "FE AutoSender",
    "system_version": "Python",
    "app_version": "2.0",
}


def random_profile(rng=random):
    model, system, app = rng.choice(_PROFILES)
    return {"device_model": model, "system_version": system, "app_version": app}


def parse_profile(stored):
    """Return a stored JSON profile, or the legacy default for empty/invalid data."""
    if not stored:
        return dict(LEGACY_DEVICE)
    try:
        data = json.loads(stored)
    except (ValueError, TypeError):
        return dict(LEGACY_DEVICE)
    if not all(key in data for key in ("device_model", "system_version", "app_version")):
        return dict(LEGACY_DEVICE)
    return {key: str(data[key]) for key in ("device_model", "system_version", "app_version")}
