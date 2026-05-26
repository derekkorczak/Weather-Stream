# Weather Stream

Flask-based weather slideshow server. Supports running in Docker and auto-updates via Portainer GitOps.

## Docker & Portainer GitOps

To run as a container and use **Portainer’s GitOps updates** (auto-redeploy on git push):

1. **Push this repo to a Git host** (GitHub, GitLab, etc.) that Portainer can reach.

2. **In Portainer:** Stacks → Add stack → **Build and deploy from a Git repository**:
   - Repository URL: your repo clone URL (HTTPS or SSH).
   - Compose path: `docker-compose.yml`
   - Build the image: leave **Build the image** enabled so pushes trigger a rebuild.

3. **Set environment variables** for the stack (or use an env file):
   - `SQL_SERVER` – e.g. `hostname,port`
   - `SQL_DATABASE` – e.g. `weather`
   - `SQL_USERNAME`
   - `SQL_PASSWORD`

4. **Enable GitOps / auto-update** (if available in your Portainer version):
   - In the stack’s Git configuration, turn on **Webhook** or **Auto-update** so Portainer redeploys when you push.

5. **Deploy.** The app will be available on port **8080**.

## Database tables

The app requires two tables in the configured SQL Server database:

```sql
CREATE TABLE ExpiredImages (
    url NVARCHAR(450) PRIMARY KEY,
    image_hash NVARCHAR(450),
    expiration_date DATETIME NULL
);

CREATE TABLE SlideDurations (
    url NVARCHAR(450) PRIMARY KEY,
    duration_seconds INT NOT NULL
);
```

`SlideDurations` persists per-slide custom display durations set from the admin console.

## Admin console

Browse to `/admin` for the management UI: per-slide thumbnails, live countdown, and buttons for expire-now, set/clear expiration, set/clear duration, jump-to-slide, and skip-to-next. The slideshow at `/` is display-only.

### Local Docker

```bash
cp .env.example .env   # edit with your SQL_* values
docker compose up -d --build
```

