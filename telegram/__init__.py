"""Hosting compatibility shim (bothost.ru).

The hosting build template probes `python -c "import telegram"` for every
Python bot project and fails the image build when the module is missing.
This project runs on aiogram + Telethon and never imports
python-telegram-bot, so this inert package exists solely to satisfy that
build probe.

Keep it empty: anything here would shadow the real python-telegram-bot
package from the project working directory (it is listed in
requirements.txt only as part of the same hosting workaround).
"""
