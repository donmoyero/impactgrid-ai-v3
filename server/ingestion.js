// ================================================================
//  IMPACTGRID / DIJO — Data Ingestion Engine
//  ingestion.js
//
//  Handles:
//    - TikTok video ingestion (keyword-based, every 30 min)
//    - YouTube trending + keyword ingestion (every 30 min)
//    - Google Trends RSS ingestion (every 30 min)
//    - Hashtag velocity tracking
//    - Velocity-based trend scoring engine (every 35 min)
//    - Instagram prediction scoring (inferred, no API needed)
//    - API routes: /trends/live, /trends/rising, /trends/cross,
//                  /trends/instagram-predictions,
//                  /ingestion/status, /ingestion/trigger
//
//  Wired into server.js via:
//    import { startIngestion, addIngestionRoutes } from './ingestion.js';
//    addIngestionRoutes(app);
//    startIngestion();
//
//  Render env vars required:
//    SUPABASE_URL
//    SUPABASE_SERVICE_KEY
//    TIKTOK_CLIENT_KEY
//    TIKTOK_CLIENT_SECRET
//    YOUTUBE_API_KEY
// ================================================================

// ── Supabase — reuse existing client from config ──
import { supabase } from "../config/supabase-client.js";

// ── Config ──
const GEO        = "GB";
const YT_API_KEY = process.env.YOUTUBE_API_KEY;

// ── TikTok seed keywords ──
const TIKTOK_SEED_QUERIES = [
  "ai tools",
  "side hustle",
  "finance tips",
  "productivity",
  "content creator",
  "digital marketing",
  "entrepreneurship",
  "fitness motivation",
  "tech review",
  "life hack",
  "passive income",
  "investing",
  "chatgpt",
  "viral trend"
];

// ── YouTube categories (UK trending) ──
const YT_CATEGORIES = [
  { name: "tech",     categoryId: "28" },
  { name: "business", categoryId: "25" },
  { name: "howto",    categoryId: "26" },
  { name: "people",   categoryId: "22" }
];

// ── YouTube keyword searches ──
const YT_KEYWORDS = [
  "ai tools 2026",
  "side hustle uk",
  "content creator tips",
  "passive income"
];

// ── Topic grouping keywords ──
const TOPIC_KEYWORDS = [
  "ai", "chatgpt", "automation", "llm",
  "side hustle", "passive income", "make money",
  "finance", "investing", "stocks", "crypto",
  "fitness", "gym", "workout", "health",
  "content creator", "youtube growth", "tiktok growth",
  "productivity", "morning routine", "life hack",
  "entrepreneur", "business", "startup",
  "tech", "review", "unboxing",
  "digital marketing", "seo", "social media"
];

// ── Cron intervals ──
const INTERVALS = {
  TIKTOK_MS:  30 * 60 * 1000,
  YOUTUBE_MS: 30 * 60 * 1000,
  GOOGLE_MS:  30 * 60 * 1000,
  SCORING_MS: 35 * 60 * 1000
};


// ================================================================
//  UTILITIES
// ================================================================

function log(source, msg, data = "") {
  const ts = new Date().toISOString();
  console.log(`[${ts}] [Ingestion:${source}] ${msg}`, data || "");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function daysAgo(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d;
}

function hoursAgo(n) {
  return new Date(Date.now() - n * 60 * 60 * 1000);
}

function formatDateYYYYMMDD(date) {
  return date.toISOString().split("T")[0].replace(/-/g, "");
}

function extractHashtags(text = "") {
  const matches = text.match(/#[a-zA-Z][a-zA-Z0-9_]*/g) || [];
  return [...new Set(matches.map((h) => h.toLowerCase()))];
}

function parseTraffic(str) {
  if (!str) return 0;
  const s = str.replace(/[^0-9.KMBkmb+]/g, "").replace("+", "");
  if (/b/i.test(str)) return parseFloat(s) * 1_000_000_000;
  if (/m/i.test(str)) return parseFloat(s) * 1_000_000;
  if (/k/i.test(str)) return parseFloat(s) * 1_000;
  return parseFloat(s) || 0;
}

function googleTrafficToScore(traffic) {
  if (traffic >= 5_000_000) return 95;
  if (traffic >= 1_000_000) return 85;
  if (traffic >= 500_000)   return 75;
  if (traffic >= 100_000)   return 65;
  if (traffic >= 50_000)    return 55;
  if (traffic >= 10_000)    return 45;
  return 35;
}

function getStatus(score) {
  if (score >= 80) return "peak";
  if (score >= 60) return "rising";
  return "emerging";
}

function buildIGReason(hasTikTok, hasYouTube, velocityScore, prediction) {
  if (prediction >= 75) {
    if (hasTikTok && hasYouTube)
      return "Strong TikTok velocity + YouTube validation — high IG adoption likely";
    if (hasTikTok)
      return "High TikTok velocity detected — likely to reach Instagram within 24-48h";
    return "Cross-platform signals suggest Instagram traction incoming";
  }
  if (prediction >= 50) return "Moderate cross-platform signals — watch for Instagram growth";
  return "Early signals — Instagram adoption uncertain";
}


// ================================================================
//  INGESTION RUN TRACKING
// ================================================================

async function startRun(platform) {
  try {
    const { data, error } = await supabase
      .from("ingestion_runs")
      .insert({ platform, status: "running", started_at: new Date().toISOString() })
      .select()
      .single();
    if (error) log(platform, "Failed to create run record", error.message);
    return data?.id || null;
  } catch (e) {
    return null;
  }
}

async function completeRun(runId, platform, counts) {
  if (!runId) return;
  try {
    await supabase
      .from("ingestion_runs")
      .update({
        status:          "completed",
        videos_fetched:  counts.videos   || 0,
        trends_detected: counts.trends   || 0,
        completed_at:    new Date().toISOString(),
        duration_ms:     counts.duration || 0
      })
      .eq("id", runId);
  } catch (e) {}
}

async function failRun(runId, platform, errorMsg) {
  if (!runId) return;
  try {
    await supabase
      .from("ingestion_runs")
      .update({
        status:        "failed",
        error_message: errorMsg,
        completed_at:  new Date().toISOString()
      })
      .eq("id", runId);
  } catch (e) {}
}


// ================================================================
//  HASHTAG TRACKER
// ================================================================

async function updateHashtagCounts(hashtags, platform) {
  if (!hashtags || !hashtags.length) return;

  const counts = {};
  hashtags.forEach((h) => { if (h) counts[h] = (counts[h] || 0) + 1; });

  for (const [hashtag, count] of Object.entries(counts)) {
    try {
      const { data: existing } = await supabase
        .from("hashtags")
        .select("*")
        .eq("hashtag", hashtag)
        .eq("platform", platform)
        .single();

      if (existing) {
        const prevCount  = existing.usage_count || 0;
        const newCount   = prevCount + count;
        const growthRate = prevCount > 0 ? ((count / prevCount) * 100) : 100;
        const count2h    = (existing.count_2h  || 0) + count;
        const count24h   = (existing.count_24h || 0) + count;
        const velocity   = count24h > 0 ? count2h / count24h : 0;

        await supabase
          .from("hashtags")
          .update({
            prev_usage_count: prevCount,
            usage_count:      newCount,
            growth_rate:      parseFloat(growthRate.toFixed(2)),
            count_2h:         count2h,
            count_24h:        count24h,
            velocity_score:   parseFloat(velocity.toFixed(4)),
            last_seen_at:     new Date().toISOString(),
            updated_at:       new Date().toISOString()
          })
          .eq("hashtag", hashtag)
          .eq("platform", platform);
      } else {
        await supabase.from("hashtags").insert({
          hashtag,
          platform,
          usage_count:    count,
          growth_rate:    100,
          count_2h:       count,
          count_24h:      count,
          velocity_score: 1,
          last_seen_at:   new Date().toISOString()
        });
      }
    } catch (e) {}
  }
}


// ================================================================
//  1. TIKTOK INGESTION
//  Uses video.list (approved scope) — reads connected users' videos.
//  Runs per stored TikTok access token in Supabase.
//  Falls back to YouTube keyword matching for trend detection
//  until more TikTok users connect their accounts.
// ================================================================

async function ingestTikTok() {
  const start = Date.now();
  const runId = await startRun("tiktok");
  log("TikTok", "Starting ingestion run");

  let totalVideos = 0;

  try {
    // Get all stored TikTok access tokens from Supabase
    // (tokens saved when users connect their TikTok accounts)
    const { data: tokens } = await supabase
      .from("tiktok_tokens")
      .select("access_token, open_id, display_name")
      .gt("expires_at", new Date().toISOString());

    if (!tokens || !tokens.length) {
      log("TikTok", "No connected TikTok accounts yet — skipping video.list fetch");
      await completeRun(runId, "tiktok", { videos: 0, duration: Date.now() - start });
      return;
    }

    log("TikTok", `Found ${tokens.length} connected accounts — fetching videos`);

    for (const token of tokens) {
      try {
        // video.list — reads the connected user's own videos
        const res = await fetch(
          "https://open.tiktokapis.com/v2/video/list/?fields=id,title,video_description,duration,cover_image_url,like_count,comment_count,share_count,view_count,create_time",
          {
            method:  "POST",
            headers: {
              "Authorization": "Bearer " + token.access_token,
              "Content-Type":  "application/json"
            },
            body: JSON.stringify({ max_count: 20 })
          }
        );

        const data   = await res.json();
        const videos = data?.data?.videos || [];

        if (!videos.length) {
          log("TikTok", `No videos for ${token.display_name || token.open_id}`);
          continue;
        }

        const rows = videos.map((v) => ({
          platform:          "tiktok",
          platform_video_id: v.id,
          creator_id:        token.open_id   || null,
          creator_name:      token.display_name || null,
          title:             v.title || v.video_description || null,
          description:       v.video_description || null,
          hashtags:          extractHashtags(v.video_description || ""),
          duration_secs:     v.duration      || null,
          thumbnail_url:     v.cover_image_url || null,
          views:             parseInt(v.view_count    || 0),
          likes:             parseInt(v.like_count    || 0),
          comments:          parseInt(v.comment_count || 0),
          shares:            parseInt(v.share_count   || 0),
          published_at:      v.create_time
            ? new Date(v.create_time * 1000).toISOString()
            : null,
          fetched_at: new Date().toISOString()
        }));

        const { error } = await supabase.from("videos").insert(rows);
        if (error) log("TikTok", `Insert error — ${token.display_name}`, error.message);
        else totalVideos += rows.length;

        await updateHashtagCounts(
          videos.flatMap((v) => extractHashtags(v.video_description || "")),
          "tiktok"
        );

        log("TikTok", `Stored ${rows.length} videos for: ${token.display_name || token.open_id}`);
        await sleep(600);

      } catch (tokenErr) {
        log("TikTok", `Error for account ${token.open_id}`, tokenErr.message);
      }
    }

    await completeRun(runId, "tiktok", { videos: totalVideos, duration: Date.now() - start });
    log("TikTok", `Run complete — ${totalVideos} videos stored`);

  } catch (err) {
    log("TikTok", "Run FAILED", err.message);
    await failRun(runId, "tiktok", err.message);
  }
}


// ================================================================
//  2. YOUTUBE INGESTION
// ================================================================

async function ingestYouTube() {
  const start = Date.now();
  const runId = await startRun("youtube");
  log("YouTube", "Starting ingestion run");

  if (!YT_API_KEY) {
    log("YouTube", "YOUTUBE_API_KEY not set — skipping");
    await failRun(runId, "youtube", "YOUTUBE_API_KEY not configured");
    return;
  }

  let totalVideos = 0;

  try {
    // Part 1 — Trending by category
    for (const cat of YT_CATEGORIES) {
      try {
        const url    = `https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails&chart=mostPopular&regionCode=GB&videoCategoryId=${cat.categoryId}&maxResults=25&key=${YT_API_KEY}`;
        const res    = await fetch(url);
        const data   = await res.json();
        const videos = data.items || [];

        if (!videos.length) continue;

        const rows = videos.map((v) => ({
          platform:          "youtube",
          platform_video_id: v.id,
          creator_id:        v.snippet?.channelId    || null,
          creator_name:      v.snippet?.channelTitle  || null,
          title:             v.snippet?.title         || null,
          description:       (v.snippet?.description  || "").slice(0, 500),
          hashtags:          extractHashtags(
            (v.snippet?.tags || []).join(" ") + " " + (v.snippet?.description || "")
          ),
          thumbnail_url:     v.snippet?.thumbnails?.medium?.url || null,
          views:             parseInt(v.statistics?.viewCount    || 0),
          likes:             parseInt(v.statistics?.likeCount    || 0),
          comments:          parseInt(v.statistics?.commentCount || 0),
          shares:            0,
          published_at:      v.snippet?.publishedAt || null,
          fetched_at:        new Date().toISOString()
        }));

        const { error } = await supabase.from("videos").insert(rows);
        if (error) log("YouTube", `Insert error — category ${cat.name}`, error.message);
        else totalVideos += rows.length;

        await updateHashtagCounts(
          videos.flatMap((v) =>
            extractHashtags(
              (v.snippet?.tags || []).join(" ") + " " + (v.snippet?.description || "")
            )
          ),
          "youtube"
        );

        log("YouTube", `Stored ${rows.length} videos — category: ${cat.name}`);
        await sleep(400);

      } catch (catErr) {
        log("YouTube", `Error — category ${cat.name}`, catErr.message);
      }
    }

    // Part 2 — Keyword searches
    for (const kw of YT_KEYWORDS) {
      try {
        const searchUrl  = `https://www.googleapis.com/youtube/v3/search?part=snippet&q=${encodeURIComponent(kw)}&type=video&order=viewCount&publishedAfter=${daysAgo(1).toISOString()}&regionCode=GB&maxResults=15&key=${YT_API_KEY}`;
        const searchRes  = await fetch(searchUrl);
        const searchData = await searchRes.json();
        const items      = searchData.items || [];

        if (!items.length) continue;

        const ids       = items.map((i) => i.id.videoId).filter(Boolean).join(",");
        const statsRes  = await fetch(`https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&id=${ids}&key=${YT_API_KEY}`);
        const statsData = await statsRes.json();
        const videos    = statsData.items || [];

        const rows = videos.map((v) => ({
          platform:          "youtube",
          platform_video_id: v.id,
          creator_id:        v.snippet?.channelId    || null,
          creator_name:      v.snippet?.channelTitle  || null,
          title:             v.snippet?.title         || null,
          description:       (v.snippet?.description  || "").slice(0, 500),
          hashtags:          extractHashtags(v.snippet?.description || ""),
          thumbnail_url:     v.snippet?.thumbnails?.medium?.url || null,
          views:             parseInt(v.statistics?.viewCount    || 0),
          likes:             parseInt(v.statistics?.likeCount    || 0),
          comments:          parseInt(v.statistics?.commentCount || 0),
          shares:            0,
          published_at:      v.snippet?.publishedAt || null,
          fetched_at:        new Date().toISOString()
        }));

        const { error } = await supabase.from("videos").insert(rows);
        if (error) log("YouTube", `Insert error — kw "${kw}"`, error.message);
        else totalVideos += rows.length;

        log("YouTube", `Stored ${rows.length} videos for: ${kw}`);
        await sleep(400);

      } catch (kwErr) {
        log("YouTube", `Error — keyword "${kw}"`, kwErr.message);
      }
    }

    await completeRun(runId, "youtube", { videos: totalVideos, duration: Date.now() - start });
    log("YouTube", `Run complete — ${totalVideos} videos stored`);

  } catch (err) {
    log("YouTube", "Run FAILED", err.message);
    await failRun(runId, "youtube", err.message);
  }
}


// ================================================================
//  3. GOOGLE TRENDS INGESTION
// ================================================================

async function ingestGoogleTrends() {
  const start = Date.now();
  const runId = await startRun("google");
  log("Google", "Starting ingestion run");

  let titles   = [];
  let traffics = [];

  const RSS_URLS = [
    "https://trends.google.com/trends/trendingsearches/daily/rss?geo=GB",
    "https://trends.google.com/trends/trendingsearches/daily/rss?geo=US",
    "https://trends.google.com/trends/trendingsearches/daily/rss?geo=GB&hl=en-GB",
  ];
  const USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (compatible; ImpactGrid/1.0; +https://impactgridgroup.com)",
  ];

  outer: for (const url of RSS_URLS) {
    for (const ua of USER_AGENTS) {
      try {
        const res = await fetch(url, { headers: { "User-Agent": ua } });
        const xml = await res.text();
        if (!xml || xml.trim().startsWith("<html") || xml.length < 200) continue;

        const cdataRe   = /<title><!\[CDATA\[([^\]]+)\]\]><\/title>/g;
        const plainRe   = /<title>([^<]{2,80})<\/title>/g;
        const trafficRe = /<ht:approx_traffic>([^<]+)<\/ht:approx_traffic>/g;
        let m;

        while ((m = cdataRe.exec(xml)) !== null) {
          const t = m[1].trim();
          if (t && !["Google Trends","Daily Search Trends"].includes(t)) titles.push(t);
        }
        if (!titles.length) {
          while ((m = plainRe.exec(xml)) !== null) {
            const t = m[1].trim();
            if (t && !["Google Trends","Daily Search Trends","RSS"].includes(t)) titles.push(t);
          }
        }
        while ((m = trafficRe.exec(xml)) !== null) traffics.push(m[1]);

        if (titles.length > 0) { log("Google", `RSS success: ${titles.length} topics from ${url}`); break outer; }
      } catch (e) { /* try next */ }
      await sleep(300);
    }
  }

  // Fallback: Google Trends JSON API
  if (!titles.length) {
    try {
      log("Google", "RSS failed — trying JSON API");
      const res  = await fetch(
        "https://trends.google.com/trends/api/dailytrends?hl=en-GB&tz=-60&geo=GB&ns=15",
        { headers: { "User-Agent": USER_AGENTS[0] } }
      );
      let text = await res.text();
      text = text.replace(/^[^\[{]*/, "").trim();
      const json  = JSON.parse(text);
      const items = json?.default?.trendingSearchesDays?.[0]?.trendingSearches || [];
      items.forEach(item => {
        if (item?.title?.query) { titles.push(item.title.query); traffics.push(item.formattedTraffic || "0"); }
      });
      log("Google", `JSON API: ${titles.length} topics`);
    } catch (e) { log("Google", "JSON API failed", e.message); }
  }

  if (!titles.length) {
    log("Google", "All sources failed — no data this run");
    await completeRun(runId, "google", { trends: 0, duration: Date.now() - start });
    return;
  }

  const now  = new Date().toISOString();
  const rows = titles.slice(0, 20).map((topic, i) => {
    const traffic = parseTraffic(traffics[i] || "0");
    const score   = googleTrafficToScore(traffic);
    return {
      topic,
      platform_source:      "google",
      trend_score:          score,
      velocity_score:       score * 0.9,
      engagement_score:     50,
      cross_platform_boost: 0,
      instagram_prediction: score * 0.4,
      instagram_reason:     "Based on Google search demand — cross-platform signals pending",
      video_count:          0,
      total_views:          traffic,
      total_likes:          0,
      hashtags:             ["#" + topic.replace(/\s+/g, "").toLowerCase()],
      status:               i < 5 ? "peak" : i < 10 ? "rising" : "emerging",
      detected_at:          now,
      window_start:         daysAgo(1).toISOString(),
      window_end:           now
    };
  });

  const { error } = await supabase.from("trends").insert(rows);
  if (error) log("Google", "Insert error", error.message);

  await completeRun(runId, "google", { trends: rows.length, duration: Date.now() - start });
  log("Google", `Run complete — ${rows.length} trends stored`);
}

// ================================================================
//  4. TOPIC GROUPING
// ================================================================

function groupByTopic(videos) {
  const groups = {};

  for (const video of videos) {
    const text    = ((video.title || "") + " " + (video.description || "")).toLowerCase();
    let   matched = false;

    for (const keyword of TOPIC_KEYWORDS) {
      if (text.includes(keyword)) {
        if (!groups[keyword]) groups[keyword] = [];
        groups[keyword].push(video);
        matched = true;
        break;
      }
    }

    if (!matched && video.hashtags?.length) {
      const tag = video.hashtags[0].replace("#", "");
      if (!groups[tag]) groups[tag] = [];
      groups[tag].push(video);
    }
  }

  return groups;
}


// ================================================================
//  5. TREND SCORING ENGINE
// ================================================================

async function runTrendScoring() {
  log("Scoring", "Starting trend scoring run");

  try {
    const now    = new Date();
    const ago24h = hoursAgo(24).toISOString();
    const now2h  = hoursAgo(2);

    const { data: recentVideos, error: fetchError } = await supabase
      .from("videos")
      .select("*")
      .gte("fetched_at", ago24h)
      .order("fetched_at", { ascending: false });

    if (fetchError) {
      log("Scoring", "Fetch error", fetchError.message);
      return;
    }

    if (!recentVideos || !recentVideos.length) {
      log("Scoring", "No recent videos — skipping");
      return;
    }

    log("Scoring", `Processing ${recentVideos.length} videos`);

    const topicMap = groupByTopic(recentVideos);
    const scored   = [];

    for (const [topic, videos] of Object.entries(topicMap)) {
      if (videos.length < 2) continue;

      const videos2h  = videos.filter((v) => new Date(v.fetched_at) >= now2h);
      const videos24h = videos;

      const views2h     = videos2h.reduce((s, v)  => s + (v.views    || 0), 0);
      const views24h    = videos24h.reduce((s, v) => s + (v.views    || 0), 0);
      const likes24h    = videos24h.reduce((s, v) => s + (v.likes    || 0), 0);
      const comments24h = videos24h.reduce((s, v) => s + (v.comments || 0), 0);
      const shares24h   = videos24h.reduce((s, v) => s + (v.shares   || 0), 0);

      // Velocity (0-100)
      const velocityRaw   = views24h > 0 ? views2h / views24h : 0;
      const velocityScore = Math.min(100, velocityRaw * 200);

      // Engagement (0-100)
      const engagementRaw   = views24h > 0
        ? (likes24h + comments24h + shares24h) / views24h
        : 0;
      const engagementScore = Math.min(100, engagementRaw * 500);

      // Comments growth component (0-100)
      const commentsScore = views24h > 0
        ? Math.min(100, (comments24h / views24h) * 1000)
        : 0;

      // Recency (0-100, decays over 25h)
      const latestVideo  = [...videos].sort(
        (a, b) => new Date(b.published_at || 0) - new Date(a.published_at || 0)
      )[0];
      const hoursOld     = latestVideo?.published_at
        ? (now - new Date(latestVideo.published_at)) / (1000 * 60 * 60)
        : 24;
      const recencyScore = Math.max(0, 100 - hoursOld * 4);

      // Cross-platform boost
      const platforms          = [...new Set(videos.map((v) => v.platform))];
      const crossPlatformBoost = platforms.length >= 2 ? 25 : 0;
      const platformSource     =
        platforms.length >= 2    ? "cross"
        : platforms[0] === "tiktok"  ? "tiktok"
        : platforms[0] === "youtube" ? "youtube"
        : "google";

      // Final weighted score
      const finalScore = Math.min(100,
        (velocityScore   * 0.4) +
        (engagementScore * 0.3) +
        (commentsScore   * 0.2) +
        (recencyScore    * 0.1) +
        crossPlatformBoost
      );

      // Instagram prediction
      const hasTikTok  = platforms.includes("tiktok");
      const hasYouTube = platforms.includes("youtube");
      const instagramPrediction = Math.min(100,
        (hasTikTok  ? velocityScore   * 0.5 : 0) +
        (hasYouTube ? engagementScore * 0.3 : 0) +
        (crossPlatformBoost * 1.5)
      );

      scored.push({
        topic,
        platform_source:      platformSource,
        trend_score:          parseFloat(finalScore.toFixed(2)),
        velocity_score:       parseFloat(velocityScore.toFixed(2)),
        engagement_score:     parseFloat(engagementScore.toFixed(2)),
        cross_platform_boost: crossPlatformBoost,
        instagram_prediction: parseFloat(instagramPrediction.toFixed(2)),
        instagram_reason:     buildIGReason(hasTikTok, hasYouTube, velocityScore, instagramPrediction),
        video_count:          videos.length,
        total_views:          views24h,
        total_likes:          likes24h,
        hashtags:             [...new Set(videos.flatMap((v) => v.hashtags || []))].slice(0, 10),
        sample_video_ids:     videos.slice(0, 5).map((v) => v.platform_video_id),
        status:               getStatus(finalScore),
        detected_at:          now.toISOString(),
        window_start:         ago24h,
        window_end:           now.toISOString()
      });
    }

    if (!scored.length) {
      log("Scoring", "No topics scored — need more data");
      return;
    }

    scored.sort((a, b) => b.trend_score - a.trend_score);
    const top50 = scored.slice(0, 50);

    log("Scoring", `Top topic: "${top50[0].topic}" — score: ${top50[0].trend_score}`);

    const { data: insertedTrends, error: trendError } = await supabase
      .from("trends")
      .insert(top50)
      .select();

    if (trendError) {
      log("Scoring", "Trend insert error", trendError.message);
      return;
    }

    const scoreRows = insertedTrends.map((t) => ({
      trend_id:             t.id,
      topic:                t.topic,
      velocity_score:       t.velocity_score,
      engagement_score:     t.engagement_score,
      cross_platform_score: t.cross_platform_boost,
      recency_score:        50,
      final_score:          t.trend_score,
      video_count:          t.video_count,
      total_views:          t.total_views,
      views_last_2h:        0,
      views_last_24h:       t.total_views,
      scored_at:            now.toISOString()
    }));

    const { error: scoreError } = await supabase.from("trend_scores").insert(scoreRows);
    if (scoreError) log("Scoring", "Score rows error", scoreError.message);

    log("Scoring", `Complete — ${top50.length} trends written to Supabase`);

  } catch (err) {
    log("Scoring", "FAILED", err.message);
  }
}


// ================================================================
//  FULL INGESTION CYCLE
// ================================================================

async function runAllIngestion() {
  log("Scheduler", "Running full ingestion cycle");
  await Promise.allSettled([
    ingestTikTok(),
    ingestYouTube(),
    ingestGoogleTrends()
  ]);
  await runTrendScoring();
  log("Scheduler", "Full cycle complete");
}


// ================================================================
//  API ROUTES
// ================================================================

export function addIngestionRoutes(app) {

  // Top scored trends
  app.get("/trends/live", async (req, res) => {
    try {
      const limit  = parseInt(req.query.limit  || "20");
      const source = req.query.source || null;
      let query = supabase.from("v_top_trends").select("*").limit(limit);
      if (source) query = query.eq("platform_source", source);
      const { data, error } = await query;
      if (error) throw error;
      res.json({ trends: data || [], ts: new Date().toISOString() });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Rising hashtags
  app.get("/trends/rising", async (req, res) => {
    try {
      const { data, error } = await supabase
        .from("v_rising_hashtags")
        .select("*")
        .limit(30);
      if (error) throw error;
      res.json({ hashtags: data || [] });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Cross-platform trends
  app.get("/trends/cross", async (req, res) => {
    try {
      const { data, error } = await supabase
        .from("v_cross_platform_trends")
        .select("*")
        .limit(20);
      if (error) throw error;
      res.json({ trends: data || [] });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Instagram predictions
  app.get("/trends/instagram-predictions", async (req, res) => {
    try {
      const { data, error } = await supabase
        .from("trends")
        .select("topic,instagram_prediction,instagram_reason,trend_score,platform_source,detected_at")
        .gte("instagram_prediction", 50)
        .gte("detected_at", hoursAgo(24).toISOString())
        .order("instagram_prediction", { ascending: false })
        .limit(15);
      if (error) throw error;
      res.json({ predictions: data || [] });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Ingestion status per platform
  app.get("/ingestion/status", async (req, res) => {
    try {
      const platforms = ["tiktok", "youtube", "google"];
      const status    = {};
      for (const p of platforms) {
        const { data } = await supabase
          .from("ingestion_runs")
          .select("*")
          .eq("platform", p)
          .order("started_at", { ascending: false })
          .limit(1)
          .single();
        status[p] = data || null;
      }
      res.json(status);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Manual trigger (admin)
  app.post("/ingestion/trigger", async (req, res) => {
    const { platform } = req.body;
    res.json({ message: "Ingestion triggered", platform: platform || "all" });
    if      (platform === "tiktok")  ingestTikTok();
    else if (platform === "youtube") ingestYouTube();
    else if (platform === "google")  ingestGoogleTrends();
    else if (platform === "scoring") runTrendScoring();
    else                             runAllIngestion();
  });
}


// ================================================================
//  SCHEDULER — called once from server.js after app.listen()
// ================================================================

export async function startIngestion() {
  log("Scheduler", "ImpactGrid ingestion engine starting");

  // Run immediately on startup
  runAllIngestion().catch((e) => log("Scheduler", "Startup cycle error", e.message));

  // Schedule recurring runs
  setInterval(
    () => ingestTikTok().catch((e)       => log("TikTok",  "Scheduled run error", e.message)),
    INTERVALS.TIKTOK_MS
  );
  setInterval(
    () => ingestYouTube().catch((e)      => log("YouTube", "Scheduled run error", e.message)),
    INTERVALS.YOUTUBE_MS
  );
  setInterval(
    () => ingestGoogleTrends().catch((e) => log("Google",  "Scheduled run error", e.message)),
    INTERVALS.GOOGLE_MS
  );
  setInterval(
    () => runTrendScoring().catch((e)    => log("Scoring", "Scheduled run error", e.message)),
    INTERVALS.SCORING_MS
  );

  log("Scheduler", "All ingestion jobs scheduled — 30 min intervals");
}
