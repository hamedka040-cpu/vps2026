# Odoo Login Automation

یک سرویس کوچک و قابل اجرای Docker برای ورود خودکار به Odoo Runbot نسخه 19.4. سرویس با Playwright وارد صفحه ورود می‌شود و **فقط وضعیت ورود** را برمی‌گرداند؛ رمز عبور و کوکی‌ها به کلاینت برگردانده نمی‌شوند.

## راه‌اندازی با Docker

1. فایل تنظیمات را بسازید و مقدارها را با Secret Manager یا محیط امن پر کنید:

```bash
cp .env.example .env
# حداقل AUTOMATION_API_KEY و یکی از Vault یا ODOO_LOGIN/ODOO_PASSWORD را تنظیم کنید
```

2. اجرا:

```bash
docker build -t odoo-login-automation .
docker run --rm -p 8000:8000 --env-file .env -v odoo-session:/data odoo-login-automation
```

3. بررسی سلامت و اجرای ورود:

```bash
curl http://localhost:8000/health
curl -X POST http://localhost:8000/login \
  -H "X-API-Key: $AUTOMATION_API_KEY"
curl http://localhost:8000/session \
  -H "X-API-Key: $AUTOMATION_API_KEY"
```

فایل `storage_state` در volume مسیر `/data/odoo-session.json` نگهداری می‌شود تا در صورت نیاز، کارهای بعدی Playwright بتوانند از session استفاده کنند. این API را بدون HTTPS و API key روی اینترنت عمومی قرار ندهید.

## اعتبارنامه‌ها

اولویت با HashiCorp Vault است. برای KV v2، مسیر `VAULT_SECRET_PATH` باید فیلدهای `login` و `password` داشته باشد. اگر سه متغیر `VAULT_ADDR`، `VAULT_TOKEN` و `VAULT_SECRET_PATH` تنظیم نباشند، سرویس از `ODOO_LOGIN` و `ODOO_PASSWORD` استفاده می‌کند.

## نکات

- کپچا و 2FA دور زده نمی‌شوند؛ در چنین حالتی سرویس با خطای عمومی متوقف می‌شود.
- URL پیش‌فرض همان آدرس Runbot ارائه‌شده است و فقط HTTPS پذیرفته می‌شود.
- برای اجرای زمان‌بندی‌شده، از cron یا scheduler خودتان یک `POST /login` معتبر ارسال کنید.
