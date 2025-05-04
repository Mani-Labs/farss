// File: types/farsiland.d.ts

export interface VideoFile {
  quality: string;
  size?: string;
  url: string;
  mirror_url?: string;
}

export interface Episode {
  id: number;
  url: string;
  sitemap_url?: string;
  show_url?: string;
  season_number: number;
  episode_number: number;
  title: string;
  air_date: string;
  thumbnail: string;
  video_files: VideoFile[];
  is_new?: number;
  lastmod?: string;
  last_scraped?: string;
  cached_at?: string;
  source?: string;
  api_id?: number;
  api_source?: string;
  modified_gmt?: string;
  show_id?: number;
}

export interface Season {
  season_number: number;
  title: string;
  episode_count: number;
  episodes: Episode[];
}

export interface Show {
  id: number;
  url: string;
  sitemap_url?: string;
  title_en: string;
  title_fa: string;
  poster?: string;
  description?: string;
  first_air_date?: string;
  rating?: number;
  rating_count?: number;
  season_count?: number;
  episode_count?: number;
  genres?: string[];
  directors?: string[];
  cast?: string[];
  social_shares?: number;
  comments_count?: number;
  seasons: Season[];
  episode_urls?: string[];
  is_new?: number;
  lastmod?: string;
  last_scraped?: string;
  cached_at?: string;
  source?: string;
  api_id?: number;
  api_source?: string;
  modified_gmt?: string;
  full_episodes?: Episode[];
}

export interface Movie {
  id: number;
  url: string;
  sitemap_url?: string;
  title_en: string;
  title_fa: string;
  poster?: string;
  description?: string;
  release_date?: string;
  year: number;
  rating?: number;
  rating_count?: number;
  genres?: string[];
  directors?: string[];
  cast?: string[];
  social_shares?: number;
  comments_count?: number;
  video_files: VideoFile[];
  is_new?: number;
  lastmod?: string;
  last_scraped?: string;
  cached_at?: string;
  source?: string;
  api_id?: number;
  api_source?: string;
  modified_gmt?: string;
}

export interface FarsilandData {
  metadata: {
    exported_at: string;
    version: string;
  };
  movies_count: number;
  shows_count: number;
  episodes_count: number;
  movies: Movie[];
  shows: Show[];
  episodes: Episode[];
}