# Chicago Dig Bot 🚧

A Python bot that tracks and analyzes Chicago 811 dig tickets, providing daily insights about infrastructure work happening across the city. The bot posts daily summaries to Bluesky, highlighting emergency vs regular dig permits, contractor activity, and geographic patterns.

## Overview

The Chicago Dig Bot monitors the City of Chicago's 811 dig ticket data to:
- Track new dig permits issued across the city
- Analyze patterns in emergency vs regular dig work
- Identify active contractors and their work patterns
- Generate visualizations of dig activity across Chicago
- Share daily insights via Bluesky social network

## Features

### 🔍 Data Collection & Analysis
- Fetches dig ticket data from Chicago's data portal
- Tracks both emergency and regular dig permits
- Maintains historical data for trend analysis
- Generates daily statistics and comparisons
- Identifies patterns in contractor activity

### 📊 Visualizations
- Creates heatmaps showing dig activity across Chicago
- Differentiates between emergency and regular permits
- Generates daily statistical charts and comparisons

### 📱 Social Media Integration
- Posts daily summaries to Bluesky
- Includes permit statistics and comparisons
- Features top contractor leaderboards
- Highlights newcomer contractors
- Shares geographic visualizations

### 🎲 Hole Roulette
- Posts a random dig permit from yesterday every 3 hours
- Includes Google Street View image of the dig location
- Shows permit details like work type and emergency status
- Provides an engaging way to explore dig activity across Chicago

### 📈 Analytics Features
- Day-of-week comparisons
- Rolling averages
- Contractor leaderboards
- Geographic clustering analysis
- Emergency vs regular permit tracking

## Data Source

The data comes from the [Chicago Data Portal 811 Dig Tickets dataset](https://data.cityofchicago.org/Transportation/Chicago-811-Dig-Tickets/gptz-y9ub). To access the data programmatically, you'll need an API key from the Chicago Data Portal:

1. Create an account at [Chicago Data Portal](https://data.cityofchicago.org)
2. Go to your profile and click on "Developer Settings"
3. Create a new API key
4. Add the API key to your `.env` file as `CHICAGO_DATA_PORTAL_TOKEN`

## Data Schema

The bot uses parquet files to store dig ticket data with the following schema:

| Field Name | Type | Description |
|------------|------|-------------|
| ticket_number | string | Unique identifier for the dig ticket |
| dig_date | datetime | Date when digging is scheduled to begin |
| street_number_from | int | Starting street number of dig location |
| street_direction | string | Street direction (N, S, E, W) |
| street_name | string | Name of the street |
| street_suffix | string | Street suffix (ST, AVE, BLVD, etc.) |
| dig_location | string | Type of work being performed |
| is_emergency | boolean | Whether this is an emergency dig |
| contractor_name | string | Name of the contractor |
| request_date | datetime | When the permit was requested |
| expiration_date | datetime | When the permit expires |
| latitude | float | Location latitude |
| longitude | float | Location longitude |

## Configuration

The bot is configured via `config.yaml` with the following main sections:

```yaml
# Data Collection Settings
data:
  data_dir: "data"
  initial_csv_path: "https://data.cityofchicago.org/api/views/gptz-y9ub/rows.csv"
  soda_api:
    url: "https://data.cityofchicago.org/resource/gptz-y9ub.json"
    days_to_fetch: 30

# Analytics Settings
analytics:
  stats:
    emergency_threshold_hours: 2
    aggregation_period: "day"
    rolling_window_days: 7

# Visualization Settings
visualization:
  chart:
    filename: "daily_chart.png"
  heatmap:
    emergency_filename: "emergency_heatmap.html"

# Social Media Settings
social:
  bluesky:
    username: "${BLUESKY_HANDLE}"
    password: "${BLUESKY_PASSWORD}"
```

## Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/chicago-dig-bot.git
cd chicago-dig-bot
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your credentials:
# - CHICAGO_DATA_PORTAL_TOKEN
# - BLUESKY_HANDLE
# - BLUESKY_PASSWORD
```

4. Initialize the database:
```bash
python src/scripts/migrate_schema.py
```

5. Run initial data collection:
```bash
python src/scripts/refresh_data.py
```

## Usage

### Daily Updates
Run the daily update script to fetch new data and post updates:
```bash
python src/scripts/run_daily_update.py
```

This will:
1. Fetch the latest dig ticket data
2. Update statistics and analytics
3. Generate new visualizations
4. Post updates to Bluesky

### Test Mode
Enable test mode in `config.yaml` to run without posting to Bluesky:
```yaml
test_mode: true
```

## Production Deployment

The bot uses PM2 for process management in production. Here's how to set it up on a Lightsail instance:

1. Install PM2 globally:
```bash
npm install pm2 -g
```

2. Create PM2 ecosystem file (ecosystem.config.js):
```javascript
module.exports = {
  apps: [{
    name: "chicago-dig-daily",
    script: "src/scripts/run_daily_update.py",
    interpreter: "python3",
    cron_restart: "0 10 * * *",  // 10am daily
    autorestart: false
  },
  {
    name: "chicago-dig-roulette",
    script: "src/scripts/post_random_permit.py",
    interpreter: "python3",
    cron_restart: "0 */3 * * *",  // Every 3 hours
    autorestart: false
  }]
}
```

3. Start the processes:
```bash
pm2 start ecosystem.config.js
```

4. Save the PM2 process list:
```bash
pm2 save
```

5. Setup PM2 to start on system boot:
```bash
pm2 startup
```

6. Monitor the processes:
```bash
pm2 list  # View all processes
pm2 logs  # View logs
pm2 monit # Monitor CPU/Memory usage
```

## Data Pipeline

The bot follows this daily workflow:

1. **Data Collection**
   - Fetches new dig tickets from Chicago's data portal
   - Validates and processes new data
   - Updates local database

2. **Analysis**
   - Generates daily statistics
   - Compares to historical averages
   - Updates contractor leaderboards
   - Identifies geographic patterns

3. **Visualization**
   - Creates updated heatmaps
   - Generates statistical charts
   - Prepares visual comparisons

4. **Social Updates**
   - Composes daily summary thread
   - Uploads visualizations
   - Posts updates to Bluesky

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
