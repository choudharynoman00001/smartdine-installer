# SmartDine POS - Windows installer

One-file Windows setup for SmartDine POS (restaurant QR ordering, kitchen display, cashier, inventory, reports; English/Urdu).

**Download the latest `SmartDinePOS-Setup.exe` from Releases, run it, then open SmartDine POS from the Start Menu or Desktop.**

- Installs per-user (no admin needed) to `%LOCALAPPDATA%\SmartDine` with a portable Node runtime - nothing else required.
- The app runs locally on your PC at http://localhost:8080 and keeps its data on your PC.
- **First run:** the app shows "First-time setup" - create the owner account (name + your own 4-8 digit PIN) on that computer. There are NO default passwords. Staff accounts (manager, cashier, waiter, kitchen) are added afterwards by the owner in Settings.
- **LOCAL USE ONLY:** run SmartDine on the restaurant's own computer and Wi-Fi. Do NOT expose it through a proxy, tunnel (ngrok/cloudflared/...), or the public internet - the first-run setup protection assumes direct local access only.
- The installer is not code-signed: Windows SmartScreen may warn. Click "More info" -> "Run anyway".
- Uninstall from `%LOCALAPPDATA%\SmartDine\Uninstall.exe`. Your data lives in `%LOCALAPPDATA%\SmartDine\app\data` - back it up first (owner login has a backup download; restore = close the app, replace the `smartdine.db` file with your backup, start again).

Developed by Chaudhry Noman - Powered by [Alpha Blue Digital](https://alphablue.digital)
