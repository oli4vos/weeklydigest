# Personal Knowledge Inbox (Foundation)

Een lokale Python-applicatie die inkomende berichten verzamelt, links opslaat en de basis klaarzet voor AI-verrijking en wekelijkse e-mailrapportage. Deze eerste versie focust op configuratie, database en een uitbreidbare projectstructuur.

## Vereisten
- Python 3.11+ (aanbevolen)
- Pip
- macOS of Linux (ontwikkeld op macOS)

## Installatie
1. **Projectmap openen**
   ```bash
   cd project_root
   ```
2. **Virtuele omgeving maken**
   ```bash
   python3 -m venv .venv
   ```
3. **Virtuele omgeving activeren**
   ```bash
   source .venv/bin/activate
   ```
4. **Dependencies installeren**
   ```bash
   pip install -r requirements.txt
   ```

## Configuratie
1. Kopieer `.env.example` naar `.env`.
   ```bash
   cp .env.example .env
   ```
2. Vul de waarden in voor database, Telegram, OpenAI en SMTP.
3. Laat `DATABASE_URL` op `sqlite:///./knowledge.db` voor lokale opslag of pas aan naar een andere database.
4. Nieuwe variabelen voor de web UI:
   - `DEFAULT_USER_ID`: standaard user voor handmatige runs (default `1`).
   - `DASHBOARD_USERNAME` / `DASHBOARD_PASSWORD`: Basic Auth credentials voor het dashboard (default `admin`/`admin` – wijzig dit!).
   - `INTERNAL_TRIGGER_TOKEN`: shared secret om de interne endpoints (`/internal/run/*`) te triggeren vanuit GitHub Actions of andere schedulers.
   - `EMAIL_VERIFICATION_CODE_TTL_MINUTES`: verloopduur van verificatiecodes voor Telegram onboarding (default `15`).

## Database initialiseren
Voer het init-script uit nadat de virtuele omgeving actief is en de `.env` klaar staat:
```bash
python scripts/init_db.py
```
Dit maakt `knowledge.db` in de projectroot aan en creëert alle tabellen.
Werk je met een bestaande database? Draai dan ook:
```bash
python scripts/migrate_user_onboarding.py
```

## Telegram-bot draaien
1. Vraag bij [@BotFather](https://t.me/BotFather) een bot aan en noteer de token.
2. Zet `TELEGRAM_BOT_TOKEN` in je `.env`.
3. Zorg dat de database is geïnitialiseerd (zie vorige stap).
4. Start de bot vanuit de projectroot:
   ```bash
   python scripts/run_bot.py
   ```
5. Stuur `/start` naar je bot om onboarding te starten (e-mail invoeren + code verifiëren).
   - Bij een foute of verlopen code kun je altijd `/resend` sturen.
6. Na onboarding worden tekstberichten opgeslagen in `raw_messages` met `source='telegram'`. Links worden gedetecteerd via regex en gemarkeerd in `contains_link`.

### Multi-user basis
- Elke unieke Telegram-gebruiker wordt automatisch aangemaakt in `users` op basis van `telegram_user_id`.
- Onboarding-statusflow: `new` → `awaiting_email` → `awaiting_email_verification` → `active`.
- Telefoonnummer is optioneel; delen gaat via Telegram contact share (`request_contact=True`).
- Berichten vóór verificatie worden al opgeslagen, maar digestmails gaan alleen naar users met `email_verified=true`.
- Bij upgrades op bestaande databases: run eenmalig `python scripts/migrate_user_onboarding.py` (additieve migratie, geen drop/reset).

### Teststappen (SQLite CLI)
1. Initialiseer opnieuw:
   ```bash
   rm -f knowledge.db
   python scripts/init_db.py
   ```
2. Start de bot, stuur een bericht vanaf Telegram en stop de bot weer:
   ```bash
   python scripts/run_bot.py
   ```
3. Controleer de database met `sqlite3 knowledge.db` en voer onderstaande statements exact uit:
   ```sql
   .schema users;
   .schema raw_messages;
   SELECT id, telegram_user_id, telegram_chat_id, telegram_username, display_name
   FROM users
   ORDER BY id DESC
   LIMIT 10;
   
   SELECT id, user_id, text, contains_link
   FROM raw_messages
   ORDER BY id DESC
   LIMIT 10;
   
   SELECT
     rm.id AS message_id,
     rm.text,
     rm.user_id,
     u.telegram_user_id,
     u.telegram_username,
     u.display_name
   FROM raw_messages rm
   JOIN users u ON rm.user_id = u.id
   ORDER BY rm.id DESC
   LIMIT 10;
   ```

## Links verwerken
Nadat er berichten met links zijn opgeslagen kun je de backlog verwerken en `resources` vullen.

1. Zorg dat `pip install -r requirements.txt` is uitgevoerd zodat `httpx`, `readability-lxml`, `beautifulsoup4` en `lxml` beschikbaar zijn.
   - Let op: `lxml-html-clean` is nu vereist; `pip install -r requirements.txt` installeert dit automatisch.
2. Voer het script uit:
   ```bash
   python scripts/process_backlog.py
   ```
3. Het script zoekt alle `raw_messages` met `contains_link = true`, detecteert unieke URLs per bericht en slaat voor iedere link metadata op in `resources` (`status`, `domain`, `title`, `extracted_text`, `final_url`). Mislukte downloads krijgen `status='failed'` zodat je ze later opnieuw kunt proberen.

## Deterministische extraction (zonder AI)
Deze stap classificeert iedere `raw_message` en gekoppelde resources technisch en haalt metadata + tekst op zonder gebruik te maken van de OpenAI API. Het script maakt eerst automatisch resource-records aan voor:
- plain text berichten (URL `telegram://message/<id>`; platform `telegram`; format `plain_text`);
- elke unieke link in `raw_messages.text` (duplicate raw_message_id+url combinaties worden overgeslagen en als `pending` gelabeld).

Ondersteunde `content_format` waarden:
- `plain_text` (telegrambericht zonder URL)
- `web_article`
- `generic_webpage`
- `instagram_post`
- `instagram_reel`
- `youtube_video`
- `youtube_short`
- `linkedin_post`
- `unknown_url` (fallback)

Uitvoer:
- `platform`, `content_format`, `extraction_method`
- `title`, `description`, `canonical_url`, `author`
- `raw_metadata_json`, `extraction_status`, `extraction_error`

Run het script:
```bash
python scripts/process_extraction.py
```

Batchoutput toont processed/success/partial/failed/skipped én hoeveel resources zijn aangemaakt voordat er geëxtraheerd wordt. Als je recente schemawijzigingen hebt (nieuwe velden in `resources`), verwijder dan `knowledge.db` en draai `python scripts/init_db.py` opnieuw voordat je het script uitvoert.

`process_extraction.py` voert bij elke run ook een herclassificatie uit voor bestaande LinkedIn- en YouTube-links (bijvoorbeeld eerder opgeslagen `youtube.com/shorts/...`). Als de automatische detectie een nieuw format aanwijst, worden de resources opnieuw op `pending` gezet zodat de extractors frisse metadata kunnen ophalen.

## Weekly digest (zonder AI)
De weekly-digest pipeline gebruikt uitsluitend `raw_messages` en `resources` (deterministische extraction) om een rapport te bouwen en op te slaan in `weekly_reports`. Omdat er nieuwe kolommen zijn toegevoegd, verwijder `knowledge.db` en voer `python scripts/init_db.py` uit als je vanuit een oudere versie komt.

### 1. Rapport genereren
Gebruik `scripts/generate_weekly_report.py` om handmatig een rapport te maken:
```bash
python scripts/generate_weekly_report.py --user-id 1 --days 7 --force
```
Optioneel kun je een specifieke periode aangeven met `--start-date` en `--end-date` of een preview wegschrijven met `--preview-file output.txt`. De e-mail bevat nu een coachende intro, highlights (mix van links en notities), thema’s, letterlijke eigen notities, een compacte links-en-bronnen sectie, directieve actiepunten, reflectie en meta-analyse.

### 2. Rapport versturen
Wanneer er al een rapport bestaat (status `generated`), verstuur je het met:
```bash
python scripts/send_weekly_report.py --report-id 3
# of het meest recente voor een gebruiker:
python scripts/send_weekly_report.py --latest --user-id 1
```
Gebruik `--force` om een eerder verzonden rapport opnieuw te sturen. SMTP-configuratie komt uit `.env` (`SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `EMAIL_FROM`, `EMAIL_TO`).
Zonder `--to` probeert het script automatisch te verzenden naar de geverifieerde e-mail van de report-user.

### 3. End-to-end test
`scripts/run_weekly_digest.py` combineert beide stappen:
```bash
python scripts/run_weekly_digest.py --user-id 1 --days 7 --force
# of voor alle geverifieerde users:
python scripts/run_weekly_digest.py --all-verified --days 7 --force
```
Gebruik `--dry-run` om alleen te genereren en de e-mailtekst te printen zonder te versturen.

### Rapportinhoud
- `highlights_json`: recent succesvolle resources
- `themes_json`: top platforms/content_format & domeinen
- `ideas_json`: losse notities/berichten zonder link
- `actions_json`: aanbevelingen/links om op te volgen
- `reflection` en `meta_analysis`: regelgebaseerde feedback, inclusief vergelijking met de vorige week indien beschikbaar

Rapporten blijven historisch opgeslagen; er zijn statusvelden (`generated`, `sent`) plus `generated_at`/`sent_at`. Hierdoor kun je lokaal meerdere weken aanmaken en later automatiseren.

### Regelgebaseerde intelligence-laag (zonder AI)
Digest-opbouw gebruikt nu extra heuristieken via `app/services/rules_engine.py`:
- tweetalige topic-classificatie (NL + EN), o.a. `ai`, `legal_tech`, `productivity`, `business`, `knowledge_management`
- item type classificatie: `idea`, `task`, `question`, `reflection`, `resource`, `inspiration`, `note`, `other`
- deterministische `importance_score` (0-10)
- signalen voor `is_consumption` vs `is_thinking`
- signalen voor `content_depth` (`short_form`, `long_form`, `unknown`)

Deze signalen worden gebruikt in highlights/thema’s/actiepunten/reflectie, zodat digestkwaliteit verbetert zonder modelkosten.

### Optionele AI digest enhancement
- De deterministische pipeline blijft leidend.
- Na heuristische digest-opbouw kan optioneel één OpenAI-call de formulering aanscherpen.
- Nieuwe variabelen:
  - `OPENAI_DIGEST_ENABLED=true|false` (default: `false`)
  - `OPENAI_DIGEST_MODEL=gpt-4o-mini`
  - `OPENAI_DIGEST_MAX_INPUT_TOKENS=6000`
  - `OPENAI_DIGEST_MAX_OUTPUT_TOKENS=1000`
  - `OPENAI_DAILY_TOKEN_LIMIT=50000`
  - `OPENAI_DAILY_CALL_LIMIT=5`
- Bij API-fouten of limietbereik valt het systeem automatisch terug op de bestaande heuristische digest zonder crash.

### Automatische runs (launchd)
Op macOS kun je de scripts automatisch laten draaien via `launchd`. In deze repo staan twee shell-scripts:

| Script | Beschrijving | Default log |
| --- | --- | --- |
| `run_daily_digest.sh` | Draait `run_weekly_digest.py --days 1 --force` en stuurt dus de dagmail (tot 21:00). | `logs/daily.log` + `daily_launchd.{out,err}.log` |
| `run_digest.sh` | Draait `run_weekly_digest.py --days 7 --force` en stuurt de wekelijkse zondagmail. | `logs/weekly.log` + `weekly_launchd.{out,err}.log` |

Bijbehorende LaunchAgents staan in `~/Library/LaunchAgents/`:

| Plist | Tijdstip | StartCalendarInterval |
| --- | --- | --- |
| `com.oliviervos.dailydigest.plist` | Dagelijks 21:00 | `Hour=21`, `Minute=0` |
| `com.oliviervos.weeklydigest.plist` | Zondag 11:00 | `Weekday=0`, `Hour=11`, `Minute=0` |

Herstarten of testen:
```bash
launchctl unload ~/Library/LaunchAgents/com.oliviervos.dailydigest.plist
launchctl load   ~/Library/LaunchAgents/com.oliviervos.dailydigest.plist
launchctl kickstart -k gui/$(id -u)/com.oliviervos.dailydigest
```
Pas de `ProgramArguments` aan als je naar een andere map wilt verwijzen. Op andere platformen kun je dezelfde scripts via cron of een scheduler laten lopen.

## Web UI & API (FastAPI)
Naast de CLI kun je nu een lichte FastAPI-server draaien die zowel een dashboard als webhook endpoints aanbiedt. Vereiste extra dependencies staan al in `requirements.txt` (`fastapi`, `uvicorn`, `jinja2`, `python-multipart`).

### Starten
```bash
uvicorn app.main:app --reload --port 8000
```
Open daarna http://localhost:8000:

- **Dashboard (`/`)** – toont rapporten, recente input/resources en onboarding-status van users.
- **Reports (`/reports`)** – overzicht van recente rapporten.
- **Report detail (`/reports/{id}`)** – onderwerp, periodes, e-mail body + JSON samenvattingen.
- **Acties** – `POST /run/daily`, `/run/weekly`, `/run/extraction` (via HTML forms of direct HTTP-calls).

Alle routes hergebruiken `DigestService`, `EmailService` en `ExtractionService`; er is geen duplicate logica.
Dashboard- en run-routes zijn beveiligd met Basic Auth; gebruik `DASHBOARD_USERNAME` en `DASHBOARD_PASSWORD`.
Als er geen activiteit is (0 berichten + 0 bronnen) wordt het rapport wel opgeslagen maar krijgt status `skipped_empty` en er wordt geen mail gestuurd.
Daily/weekly runs via dashboard of `/internal/run/*` verwerken alle users met `is_active=true`, `email_verified=true` en een ingevuld e-mailadres.

### Telegram webhook
Het endpoint `POST /telegram/webhook` accepteert reguliere Telegram update payloads. Om lokaal te testen:

```bash
# start de FastAPI server eerst
# expose lokaal via bijv. cloudflared of ngrok
cloudflared tunnel --url http://localhost:8000

# daarna
curl \"https://api.telegram.org/bot<token>/setWebhook?url=https://<tunnel-domain>/telegram/webhook\"
```

Nieuwe berichten worden via `TelegramService.store_from_payload` verwerkt (inclusief onboarding, e-mailverificatie en optionele contact-share), waardoor polling optioneel blijft.

### Config
- `DEFAULT_USER_ID` bepaalt de fallback user voor handmatige scripts.
- `DASHBOARD_USERNAME` / `DASHBOARD_PASSWORD` voor Basic Auth.
- `INTERNAL_TRIGGER_TOKEN` voor de beveiligde `/internal/run/*` endpoints (gebruik je ook in GitHub Actions).
- `EMAIL_VERIFICATION_CODE_TTL_MINUTES` bepaalt hoe lang onboardingcodes geldig zijn.
- SMTP/Telegram variabelen blijven verder hetzelfde.
- Datums/timestamps blijven in UTC opgeslagen in de database; weergave gebeurt in `APP_TIMEZONE`.

## Deployment via GitHub + Render
Gebruik desgewenst `render.yaml` (in deze repo) als Render Blueprint, of configureer handmatig:

1. **GitHub klaarzetten**
   - Fork/clone deze repo en push naar je eigen GitHub.
   - Zet in GitHub → *Settings → Secrets and variables → Actions*:
     - `APP_BASE_URL` = publieke Render URL (bijv. `https://knowledge-inbox.onrender.com`).
     - `INTERNAL_TRIGGER_TOKEN` = willekeurige secret, gelijk aan de Render env var.
2. **Render Postgres**
   - Maak in Render een gratis Postgres database (of laat `render.yaml` dit voor je doen).
   - Kopieer de connection string → gebruik als `DATABASE_URL`.
3. **Render Web Service**
   - Maak een Web Service vanuit je repo (of importeer `render.yaml`).
   - Build command: `pip install -r requirements.txt`
   - Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
   - Plan: Free.
   - Zet de env vars:
     - `DATABASE_URL`
     - `TELEGRAM_BOT_TOKEN`
     - `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`
     - `EMAIL_FROM`, `EMAIL_TO`
     - `EMAIL_VERIFICATION_CODE_TTL_MINUTES`
     - `APP_TIMEZONE` (bijv. `Europe/Amsterdam`)
     - `DEFAULT_USER_ID` (meestal `1`)
     - `DASHBOARD_USERNAME`, `DASHBOARD_PASSWORD`
     - `INTERNAL_TRIGGER_TOKEN`
4. **Eerste database init**
   - Open de Render Shell (of background job) en voer één keer uit:
     ```bash
     python scripts/init_db.py
     python scripts/migrate_user_onboarding.py
     ```
   - Hierdoor worden alle tabellen in Postgres aangemaakt. Doe dit alleen bij de eerste deploy of na schemawijzigingen.
5. **Telegram webhook koppelen**
   ```bash
   curl "https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/setWebhook?url=https://your-app.onrender.com/telegram/webhook"
   ```
6. **Smoke tests**
   - `/health` moet `{"ok": true}` teruggeven.
   - Bezoek `https://your-app.onrender.com/` en log in met Basic Auth.
   - Gebruik het dashboard om een test-daily run te starten (zorg dat er minstens één message is).
7. **GitHub Actions controleren**
   - In GitHub → Actions → kies `Trigger Daily Digest` of `Trigger Weekly Digest` → *Run workflow*.
   - Controleer dat de run een 2xx-response toont en dat er een report in Render/DB verschijnt.
8. **Dagelijkse/wekelijkse planning**
   - Workflows draaien automatisch volgens de gedefinieerde cron (UTC). Als de app een `skipped_empty` status teruggeeft, is dat normaal bij geen activiteit.

### GitHub Actions schedules & endpoints
Er staan twee workflows in `.github/workflows/`:

| Workflow | Endpoint | Schedule (UTC) | Doel |
| --- | --- | --- | --- |
| `daily-digest.yml` | `/internal/run/daily` | `0 20 * * *` | Dagelijkse digest (ongeveer 21:00 NL-tijd) |
| `weekly-digest.yml` | `/internal/run/weekly` | `0 9 * * 0` | Zondagmail (11:00 NL-tijd) |

Zet in GitHub → Settings → Secrets and variables → Actions:
- `APP_BASE_URL` → bv. `https://your-app.onrender.com`
- `INTERNAL_TRIGGER_TOKEN` → dezelfde waarde als in Render `.env`

De workflows posten naar de interne endpoints en falen automatisch als de status geen 2xx is. Je kunt ze ook handmatig runnen via *workflow_dispatch*. Voor een extraction-backfill kun je een kopie van de daily workflow maken en de URL veranderen naar `/internal/run/extraction`.

### Deployment (gratis opties)
De FastAPI-app is stateless en kan op iedere gratis container/Functions host draaien. Mogelijke routes:

1. **Cloudflare Pages Functions** – bundel met `uvicorn` en gebruik hun Python support (alpha) + SQLite file in KV/D1, of migreer naar Cloudflare D1.
2. **Railway / Render / Fly.io (free tier)** – draai `uvicorn app.main:app --host 0.0.0.0 --port $PORT` met een lichte container. Gebruik Cloudflare Tunnel of Worker als publiek HTTPS-endpoint voor de Telegram webhook.
3. **Deta Space / HuggingFace Spaces** – ook mogelijk zolang Python + uvicorn ondersteund wordt.

Omdat alles in één proces zit (FastAPI + services + SMTP), is deployment een kwestie van deze environment variables meegeven en `uvicorn` starten.

## Lokale ontwikkeling
- Laat `.env` op SQLite staan (`DATABASE_URL=sqlite:///./knowledge.db`) en gebruik `uvicorn app.main:app --reload`.
- Scripts (`python scripts/run_bot.py`, `process_extraction.py`, `run_weekly_digest.py`, enz.) blijven werken zoals eerder.
- Launchd/cron zijn optioneel zodra GitHub Actions & Render draaien, maar je kunt ze lokaal blijven gebruiken.
- Tests zoals `sqlite3 knowledge.db` blijven identiek; er zijn geen Postgres-only wijzigingen.

## Volgende stappen
- Voeg repositories en services toe voor het verwerken van binnenkomende data.
- Integreer Telegram, OpenAI en e-mail zodra de basis getest is.

## Enrichment draaien
Gebruik de AI verrijkingsstap om `knowledge_items` automatisch te vullen.

1. Zorg dat je `.env` de OpenAI variabelen bevat (`OPENAI_API_KEY`, `OPENAI_MODEL`).
2. Installeer de nieuwste requirements (zie boven).
3. Verwerk openstaande berichten:
   ```bash
   python scripts/process_enrichment.py
   ```
4. Het script meldt hoeveel items succesvol, gefaald of overgeslagen zijn. Nieuwe `knowledge_items` koppelen automatisch `user_id`, `raw_message_id`, categorie, samenvatting, inzichten, tags en prioriteit.

Gebruik daarna de SQL-checks uit de multi-user sectie om te bevestigen dat `knowledge_items` gevuld zijn.

### Extraction vs enrichment
- **Extraction** (dit hoofdstuk) is deterministisch, gebruikt alleen httpx/readability/BeautifulSoup en vult `resources` met technische metadata.
- **Enrichment** (dit hoofdstuk) gebruikt OpenAI om inhoudelijk te classificeren en `knowledge_items` te vullen op basis van de ge-normaliseerde data uit `resources`.

### Aanbevolen volgorde
1. Telegram intake (`python scripts/run_bot.py`) om `raw_messages` te vullen.
2. Link backlog verwerken (`python scripts/process_backlog.py`) zodat `resources` bestaan voor URL's.
3. Deterministische extraction (`python scripts/process_extraction.py`) om platform/content_format/titel/tekst vast te leggen.
4. AI enrichment (`python scripts/process_enrichment.py`) - deze stap gebruikt zowel de oorspronkelijke message-tekst als de ge-extraheerde resourcecontext.

Internal endpoints geven JSON terug:
- `status`: `sent`, `skipped_empty`, of `failed`
- `ok`: boolean
- `message`: samenvatting voor logs
- `report_id`, `start`, `end` indien relevant

GitHub Actions falen automatisch bij non-2xx responses (curl exit). De response body wordt kort gelogd.
