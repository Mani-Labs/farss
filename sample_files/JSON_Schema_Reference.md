## Farsiland JSON Schema Reference

### 🎬 Movies
| Field | Type | Description |
|-------|------|-------------|
| id | int | Unique movie ID |
| url | string | Movie page URL |
| sitemap_url | string | URL in sitemap |
| title_en | string | English title |
| title_fa | string | Persian title |
| poster | string | Poster image URL |
| description | string | Movie description |
| release_date | string (YYYY-MM-DD) | Release date |
| year | int | Year of release |
| rating | float | Average rating |
| rating_count | int | Number of ratings |
| genres | string[] | List of genres |
| directors | string[] | List of directors |
| cast | string[] | List of main cast members |
| social_shares | int | Social shares |
| comments_count | int | Comments on the movie |
| video_files | object[] | Video objects with `quality`, `size`, `url`, `mirror_url` |
| is_new | int (0/1) | Recently added flag |
| lastmod | string | Last modified datetime |
| last_scraped | string | Last time scraped |
| cached_at | string | Cache timestamp |
| source | string | Data source (e.g., api) |
| api_id | int | ID from API |
| api_source | string | API version used |
| modified_gmt | string | Last GMT-modified timestamp |

---

### 📺 Shows
| Field | Type | Description |
|-------|------|-------------|
| id | int | Unique show ID |
| url | string | Show page URL |
| sitemap_url | string | Sitemap origin URL |
| title_en | string | English title |
| title_fa | string | Persian title |
| poster | string | Poster image URL |
| description | string | Show description |
| first_air_date | string | Air date of first episode |
| rating | float | Average rating |
| rating_count | int | Number of ratings |
| season_count | int | Count of seasons |
| episode_count | int | Count of episodes |
| genres | string[] | List of genres |
| directors | string[] | List of directors |
| cast | string[] | Cast members |
| social_shares | int | Share count |
| comments_count | int | Number of comments |
| seasons | object[] | Season list, each with:
  - season_number, title, episode_count, episodes[] |
| episode_urls | string[] | Flat list of episode URLs |
| is_new | int | New content flag |
| lastmod | string | Last modified timestamp |
| last_scraped | string | Scraped time |
| cached_at | string | Cache time |
| source | string | Source of data |
| api_id | int | API-specific ID |
| api_source | string | API version |
| modified_gmt | string | GMT timestamp |
| full_episodes | object[] | Related episodes (optional) |

---

### 🎞 Episodes
| Field | Type | Description |
|-------|------|-------------|
| id | int | Episode ID |
| url | string | Episode URL |
| sitemap_url | string | Sitemap entry URL |
| show_url | string | Parent show URL |
| season_number | int | Season number |
| episode_number | int | Episode number in season |
| title | string | Episode title |
| air_date | string | Air date |
| thumbnail | string | Episode thumbnail image |
| video_files | object[] | Videos with: `quality`, `size`, `url`, `mirror_url` |
| is_new | int | New content flag |
| lastmod | string | Last modified timestamp |
| last_scraped | string | Scrape timestamp |
| cached_at | string | Cache timestamp |
| source | string | Data source label |
| api_id | int | API ID |
| api_source | string | API version |
| modified_gmt | string | GMT version of lastmod |
| show_id | int (optional) | Reference to show ID (may be inferred) |