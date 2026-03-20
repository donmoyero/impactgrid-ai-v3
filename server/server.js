// ================================================================
//  IMPACTGRID / DIJO — Main Server
//  server.js
//
//  Render env vars required:
//    GROQ_API_KEY
//    SUPABASE_URL
//    SUPABASE_SERVICE_KEY
//    TIKTOK_CLIENT_KEY
//    TIKTOK_CLIENT_SECRET
//    TIKTOK_REDIRECT_URI
//    YOUTUBE_CLIENT_ID
//    YOUTUBE_CLIENT_SECRET
//    YOUTUBE_REDIRECT_URI
//    YOUTUBE_API_KEY          ← YouTube Data API v3 key (for ingestion)
//    INSTAGRAM_APP_ID
//    INSTAGRAM_APP_SECRET
//    INSTAGRAM_REDIRECT_URI
//    META_WEBHOOK_VERIFY_TOKEN
// ================================================================

import express from "express";
import cors    from "cors";
import dotenv  from "dotenv";
import Groq    from "groq-sdk";
import { startIngestion, addIngestionRoutes } from "./ingestion.js";

dotenv.config();

const app  = express();
const groq = new Groq({ apiKey: process.env.GROQ_API_KEY });

app.use(cors());
app.use(express.json());

/* ── Health check ── */
app.get("/health", (req, res) => {
  res.json({ status: "ok", service: "ImpactGrid Dijo", ts: new Date().toISOString() });
});

/* ── Keep-warm ping ── */
app.get("/ping", (req, res) => res.json({ pong: true }));


/* ================================================================
   CHAT ENDPOINT
================================================================ */
app.post("/chat", async (req, res) => {
  try {
    const { message, mode = "adviser" } = req.body;
    if (!message) return res.status(400).json({ error: "No message provided" });

    const systemPrompts = {
      adviser: `You are Dijo, ImpactGrid's AI financial adviser for small business owners.
You speak plainly and directly — no jargon, no waffle.
You give specific, actionable financial advice based on the data provided.
Always be honest even if the news is tough.
Keep responses concise and practical.
Format responses clearly with short paragraphs.
Use bullet points sparingly.`,

      dashboard: `You are Dijo, an AI financial analyst embedded in ImpactGrid.
Analyse the financial data provided and give clear, specific insights.
Focus on trends, risks, and opportunities.
Be direct and numbers-focused.`,

      group: `You are Dijo, ImpactGrid's AI adviser for the ImpactGrid Group platform.
You help business owners and investors understand financial performance.
Be professional, clear, and actionable.`,

      creator: `You are the ImpactGrid Creator Intelligence Engine — a data-driven AI that helps creators and businesses identify viral content opportunities.
You analyse trends and generate optimised content for TikTok, YouTube, and Instagram.
When asked to generate content, respond with a hook, caption, hashtags, and posting advice.
Be specific, direct, and data-driven. No generic advice.`
    };

    const systemPrompt = systemPrompts[mode] || systemPrompts.adviser;

    const completion = await groq.chat.completions.create({
      model:       "llama-3.3-70b-versatile",
      max_tokens:  1024,
      temperature: 0.7,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user",   content: message }
      ]
    });

    const reply = completion.choices[0]?.message?.content || "I couldn't generate a response.";
    res.json({ reply });

  } catch (err) {
    console.error("[Dijo] Error:", err.message);
    res.status(500).json({ error: "AI service error", details: err.message });
  }
});


/* ================================================================
   META WEBHOOK
   Render env: META_WEBHOOK_VERIFY_TOKEN = impactgrid_webhook_2026
   Meta Portal: Callback URL = https://impactgrid-dijo.onrender.com/webhook
================================================================ */
app.get("/webhook", (req, res) => {
  const VERIFY_TOKEN = process.env.META_WEBHOOK_VERIFY_TOKEN;
  const mode         = req.query["hub.mode"];
  const token        = req.query["hub.verify_token"];
  const challenge    = req.query["hub.challenge"];
  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    console.log("[Meta Webhook] Verified");
    res.status(200).send(challenge);
  } else {
    res.status(403).json({ error: "Verification failed" });
  }
});

app.post("/webhook", (req, res) => {
  console.log("[Meta Webhook] Event:", JSON.stringify(req.body));
  if (req.body.object === "instagram") {
    req.body.entry?.forEach((e) => e.changes?.forEach((c) =>
      console.log("[Meta Webhook] Instagram:", c.field, c.value)
    ));
  }
  res.status(200).send("EVENT_RECEIVED");
});


/* ================================================================
   TIKTOK ROUTES
   Render env: TIKTOK_CLIENT_KEY, TIKTOK_CLIENT_SECRET, TIKTOK_REDIRECT_URI
================================================================ */
app.post("/tiktok/token", async (req, res) => {
  const { code, redirect_uri, code_verifier } = req.body;
  if (!code || !code_verifier) return res.status(400).json({ error: "Missing code or code_verifier" });
  try {
    const response = await fetch("https://open.tiktokapis.com/v2/oauth/token/", {
      method:  "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_key:    process.env.TIKTOK_CLIENT_KEY,
        client_secret: process.env.TIKTOK_CLIENT_SECRET,
        code,
        grant_type:    "authorization_code",
        redirect_uri:  redirect_uri || process.env.TIKTOK_REDIRECT_URI,
        code_verifier
      }).toString()
    });
    const data = await response.json();
    if (!response.ok || data.error) return res.status(400).json({ error: data.error || "Token exchange failed", details: data });
    res.json({
      access_token:  data.access_token,
      open_id:       data.open_id,
      expires_in:    data.expires_in,
      refresh_token: data.refresh_token,
      scope:         data.scope
    });
  } catch (err) {
    res.status(500).json({ error: "Token exchange failed", details: err.message });
  }
});

app.post("/tiktok/profile", async (req, res) => {
  const { access_token } = req.body;
  if (!access_token) return res.status(400).json({ error: "Missing access_token" });
  try {
    const response = await fetch(
      "https://open.tiktokapis.com/v2/user/info/?fields=open_id,union_id,avatar_url,display_name,username,follower_count,following_count,likes_count,video_count",
      { method: "GET", headers: { "Authorization": "Bearer " + access_token } }
    );
    const data = await response.json();
    if (!response.ok || data.error) return res.status(400).json({ error: "Failed to fetch profile", details: data });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "Profile fetch failed", details: err.message });
  }
});

app.post("/tiktok/videos", async (req, res) => {
  const { access_token, max_count } = req.body;
  if (!access_token) return res.status(400).json({ error: "Missing access_token" });
  try {
    const response = await fetch(
      "https://open.tiktokapis.com/v2/video/list/?fields=id,title,cover_image_url,video_description,duration,like_count,comment_count,share_count,view_count",
      {
        method:  "POST",
        headers: { "Authorization": "Bearer " + access_token, "Content-Type": "application/json" },
        body:    JSON.stringify({ max_count: max_count || 10 })
      }
    );
    const data = await response.json();
    if (!response.ok || data.error) return res.status(400).json({ error: "Failed to fetch videos", details: data });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "Videos fetch failed", details: err.message });
  }
});

app.post("/tiktok/publish", async (req, res) => {
  const { access_token, video_url, caption, privacy_level } = req.body;
  if (!access_token || !video_url) return res.status(400).json({ error: "Missing access_token or video_url" });
  try {
    const response = await fetch("https://open.tiktokapis.com/v2/post/publish/video/init/", {
      method:  "POST",
      headers: { "Authorization": "Bearer " + access_token, "Content-Type": "application/json" },
      body: JSON.stringify({
        post_info:   { title: caption || "", privacy_level: privacy_level || "PUBLIC_TO_EVERYONE", disable_duet: false, disable_comment: false, disable_stitch: false },
        source_info: { source: "PULL_FROM_URL", video_url }
      })
    });
    const data = await response.json();
    if (!response.ok || (data.error && data.error.code !== "ok")) return res.status(400).json({ error: "Publish failed", details: data });
    res.json({ publish_id: data.data?.publish_id, status: "publishing" });
  } catch (err) {
    res.status(500).json({ error: "Publish failed", details: err.message });
  }
});

app.post("/tiktok/share", async (req, res) => {
  const { access_token, video_url, title } = req.body;
  if (!access_token || !video_url) return res.status(400).json({ error: "Missing access_token or video_url" });
  try {
    const verify = await fetch("https://open.tiktokapis.com/v2/user/info/?fields=open_id", {
      headers: { "Authorization": "Bearer " + access_token }
    });
    if (!verify.ok) return res.status(401).json({ error: "Invalid or expired access token" });
    res.json({ share_url: video_url, title: title || "", client_key: process.env.TIKTOK_CLIENT_KEY, status: "ready" });
  } catch (err) {
    res.status(500).json({ error: "Share failed", details: err.message });
  }
});

app.post("/tiktok/refresh", async (req, res) => {
  const { refresh_token } = req.body;
  if (!refresh_token) return res.status(400).json({ error: "Missing refresh_token" });
  try {
    const response = await fetch("https://open.tiktokapis.com/v2/oauth/token/", {
      method:  "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_key:    process.env.TIKTOK_CLIENT_KEY,
        client_secret: process.env.TIKTOK_CLIENT_SECRET,
        grant_type:    "refresh_token",
        refresh_token
      }).toString()
    });
    const data = await response.json();
    if (!response.ok || data.error) return res.status(400).json({ error: "Token refresh failed", details: data });
    res.json({ access_token: data.access_token, expires_in: data.expires_in, refresh_token: data.refresh_token });
  } catch (err) {
    res.status(500).json({ error: "Token refresh failed", details: err.message });
  }
});


/* ── Save TikTok token to Supabase (called from tiktok-callback.html) ── */
app.post("/tiktok/save-token", async (req, res) => {
  const { open_id, access_token, refresh_token, expires_in, scope, display_name, avatar_url } = req.body;
  if (!open_id || !access_token) return res.status(400).json({ error: "Missing open_id or access_token" });
  try {
    const { createClient } = await import("@supabase/supabase-js");
    const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
    const expiresAt = new Date(Date.now() + (expires_in || 86400) * 1000).toISOString();
    const { error } = await sb.from("tiktok_tokens").upsert({
      open_id,
      access_token,
      refresh_token: refresh_token || "",
      expires_at:    expiresAt,
      scope:         scope || "",
      display_name:  display_name || "",
      avatar_url:    avatar_url || "",
      updated_at:    new Date().toISOString()
    }, { onConflict: "open_id" });
    if (error) {
      console.error("[TikTok save-token] Supabase error:", error.message);
      return res.status(500).json({ error: "Failed to save token", details: error.message });
    }
    console.log("[TikTok save-token] Saved token for:", display_name || open_id);
    res.json({ saved: true, open_id });
  } catch (err) {
    console.error("[TikTok save-token] Exception:", err.message);
    res.status(500).json({ error: "Token save failed", details: err.message });
  }
});



/* ================================================================
   INSTAGRAM ROUTES
   Render env: INSTAGRAM_APP_ID, INSTAGRAM_APP_SECRET, INSTAGRAM_REDIRECT_URI
================================================================ */
app.post("/instagram/token", async (req, res) => {
  const { code, redirect_uri } = req.body;
  if (!code) return res.status(400).json({ error: "Missing code" });
  try {
    const shortRes  = await fetch("https://api.instagram.com/oauth/access_token", {
      method:  "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id:     process.env.INSTAGRAM_APP_ID,
        client_secret: process.env.INSTAGRAM_APP_SECRET,
        grant_type:    "authorization_code",
        redirect_uri:  redirect_uri || process.env.INSTAGRAM_REDIRECT_URI,
        code
      }).toString()
    });
    const shortData = await shortRes.json();
    if (!shortRes.ok || shortData.error) return res.status(400).json({ error: "Token exchange failed", details: shortData });
    const longRes  = await fetch(`https://graph.instagram.com/access_token?grant_type=ig_exchange_token&client_secret=${process.env.INSTAGRAM_APP_SECRET}&access_token=${shortData.access_token}`);
    const longData = await longRes.json();
    if (!longRes.ok || longData.error) return res.json({ access_token: shortData.access_token, user_id: shortData.user_id, expires_in: 3600 });
    res.json({ access_token: longData.access_token, user_id: shortData.user_id, expires_in: longData.expires_in || 5184000 });
  } catch (err) {
    res.status(500).json({ error: "Token exchange failed", details: err.message });
  }
});

app.post("/instagram/profile", async (req, res) => {
  const { access_token, user_id } = req.body;
  if (!access_token) return res.status(400).json({ error: "Missing access_token" });
  try {
    const uid  = user_id || "me";
    const data = await (await fetch(`https://graph.instagram.com/v19.0/${uid}?fields=id,username,name,biography,followers_count,follows_count,media_count,profile_picture_url,website&access_token=${access_token}`)).json();
    if (data.error) return res.status(400).json({ error: "Failed to fetch profile", details: data });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "Profile fetch failed", details: err.message });
  }
});

app.post("/instagram/media", async (req, res) => {
  const { access_token, user_id } = req.body;
  if (!access_token) return res.status(400).json({ error: "Missing access_token" });
  try {
    const uid  = user_id || "me";
    const data = await (await fetch(`https://graph.instagram.com/v19.0/${uid}/media?fields=id,caption,media_type,media_url,thumbnail_url,timestamp,like_count,comments_count,permalink&limit=12&access_token=${access_token}`)).json();
    if (data.error) return res.status(400).json({ error: "Failed to fetch media", details: data });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "Media fetch failed", details: err.message });
  }
});

app.post("/instagram/insights", async (req, res) => {
  const { access_token, user_id } = req.body;
  if (!access_token) return res.status(400).json({ error: "Missing access_token" });
  try {
    const uid  = user_id || "me";
    const data = await (await fetch(`https://graph.instagram.com/v19.0/${uid}/insights?metric=reach,impressions,profile_views,follower_count&period=day&access_token=${access_token}`)).json();
    if (data.error) return res.status(400).json({ error: "Failed to fetch insights", details: data });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "Insights fetch failed", details: err.message });
  }
});

app.post("/instagram/publish", async (req, res) => {
  const { access_token, user_id, image_url, caption } = req.body;
  if (!access_token || !image_url) return res.status(400).json({ error: "Missing access_token or image_url" });
  try {
    const uid           = user_id || "me";
    const containerData = await (await fetch(`https://graph.instagram.com/v19.0/${uid}/media`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ image_url, caption: caption || "", access_token })
    })).json();
    if (containerData.error) return res.status(400).json({ error: "Failed to create container", details: containerData });
    const publishData = await (await fetch(`https://graph.instagram.com/v19.0/${uid}/media_publish`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ creation_id: containerData.id, access_token })
    })).json();
    if (publishData.error) return res.status(400).json({ error: "Failed to publish", details: publishData });
    res.json({ media_id: publishData.id, status: "published" });
  } catch (err) {
    res.status(500).json({ error: "Publish failed", details: err.message });
  }
});


/* ================================================================
   YOUTUBE ROUTES
   Render env: YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, YOUTUBE_REDIRECT_URI
================================================================ */

/* ── Token Exchange ── */
app.post("/youtube/token", async (req, res) => {
  const { code, redirect_uri } = req.body;
  if (!code) return res.status(400).json({ error: "Missing code" });
  try {
    const response = await fetch("https://oauth2.googleapis.com/token", {
      method:  "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        code,
        client_id:     process.env.YOUTUBE_CLIENT_ID,
        client_secret: process.env.YOUTUBE_CLIENT_SECRET,
        redirect_uri:  redirect_uri || process.env.YOUTUBE_REDIRECT_URI,
        grant_type:    "authorization_code"
      }).toString()
    });
    const data = await response.json();
    if (!response.ok || data.error) {
      console.error("[YouTube /token] Error:", data);
      return res.status(400).json({ error: "Token exchange failed", details: data });
    }
    res.json({
      access_token:  data.access_token,
      refresh_token: data.refresh_token,
      expires_in:    data.expires_in,
      token_type:    data.token_type
    });
  } catch (err) {
    console.error("[YouTube /token] Exception:", err.message);
    res.status(500).json({ error: "Token exchange failed", details: err.message });
  }
});

/* ── Channel Stats ── */
app.post("/youtube/channel", async (req, res) => {
  const { access_token } = req.body;
  if (!access_token) return res.status(400).json({ error: "Missing access_token" });
  try {
    const response = await fetch(
      "https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics,brandingSettings&mine=true",
      { headers: { "Authorization": "Bearer " + access_token } }
    );
    const data = await response.json();
    if (!response.ok || data.error) {
      console.error("[YouTube /channel] Error:", data);
      return res.status(400).json({ error: "Failed to fetch channel", details: data });
    }
    res.json(data);
  } catch (err) {
    console.error("[YouTube /channel] Exception:", err.message);
    res.status(500).json({ error: "Channel fetch failed", details: err.message });
  }
});

/* ── Recent Videos ── */
app.post("/youtube/videos", async (req, res) => {
  const { access_token, max_results } = req.body;
  if (!access_token) return res.status(400).json({ error: "Missing access_token" });
  try {
    const channelRes  = await fetch(
      "https://www.googleapis.com/youtube/v3/channels?part=contentDetails&mine=true",
      { headers: { "Authorization": "Bearer " + access_token } }
    );
    const channelData = await channelRes.json();
    const uploadsId   = channelData.items?.[0]?.contentDetails?.relatedPlaylists?.uploads;
    if (!uploadsId) return res.status(404).json({ error: "No uploads playlist found" });

    const videosRes  = await fetch(
      `https://www.googleapis.com/youtube/v3/playlistItems?part=snippet,contentDetails&playlistId=${uploadsId}&maxResults=${max_results || 10}`,
      { headers: { "Authorization": "Bearer " + access_token } }
    );
    const videosData = await videosRes.json();
    if (!videosRes.ok || videosData.error) {
      console.error("[YouTube /videos] Error:", videosData);
      return res.status(400).json({ error: "Failed to fetch videos", details: videosData });
    }

    const videoIds = videosData.items?.map((v) => v.contentDetails.videoId).join(",");
    if (!videoIds) return res.json({ items: [] });

    const statsRes  = await fetch(
      `https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&id=${videoIds}`,
      { headers: { "Authorization": "Bearer " + access_token } }
    );
    const statsData = await statsRes.json();
    res.json(statsData);
  } catch (err) {
    console.error("[YouTube /videos] Exception:", err.message);
    res.status(500).json({ error: "Videos fetch failed", details: err.message });
  }
});

/* ── Analytics ── */
app.post("/youtube/analytics", async (req, res) => {
  const { access_token } = req.body;
  if (!access_token) return res.status(400).json({ error: "Missing access_token" });
  try {
    const endDate   = new Date().toISOString().split("T")[0];
    const startDate = new Date(Date.now() - 28 * 24 * 60 * 60 * 1000).toISOString().split("T")[0];
    const response  = await fetch(
      `https://youtubeanalytics.googleapis.com/v2/reports?ids=channel==MINE&startDate=${startDate}&endDate=${endDate}&metrics=views,estimatedMinutesWatched,averageViewDuration,subscribersGained,subscribersLost,likes,comments&dimensions=day&sort=day`,
      { headers: { "Authorization": "Bearer " + access_token } }
    );
    const data = await response.json();
    if (!response.ok || data.error) {
      console.error("[YouTube /analytics] Error:", data);
      return res.status(400).json({ error: "Failed to fetch analytics", details: data });
    }
    res.json(data);
  } catch (err) {
    console.error("[YouTube /analytics] Exception:", err.message);
    res.status(500).json({ error: "Analytics fetch failed", details: err.message });
  }
});

/* ── Refresh Token ── */
app.post("/youtube/refresh", async (req, res) => {
  const { refresh_token } = req.body;
  if (!refresh_token) return res.status(400).json({ error: "Missing refresh_token" });
  try {
    const response = await fetch("https://oauth2.googleapis.com/token", {
      method:  "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        refresh_token,
        client_id:     process.env.YOUTUBE_CLIENT_ID,
        client_secret: process.env.YOUTUBE_CLIENT_SECRET,
        grant_type:    "refresh_token"
      }).toString()
    });
    const data = await response.json();
    if (!response.ok || data.error) return res.status(400).json({ error: "Token refresh failed", details: data });
    res.json({ access_token: data.access_token, expires_in: data.expires_in });
  } catch (err) {
    console.error("[YouTube /refresh] Exception:", err.message);
    res.status(500).json({ error: "Token refresh failed", details: err.message });
  }
});


/* ================================================================
   GOOGLE TRENDS PROXY (legacy — kept for backwards compatibility)
   /trends/google still works, but /trends/live is now preferred
   as it serves scored data from Supabase.
================================================================ */
app.get("/trends/google", async (req, res) => {
  const geo = req.query.geo || "GB";
  try {
    const response = await fetch(
      `https://trends.google.com/trends/trendingsearches/daily/rss?geo=${geo}`,
      { headers: { "User-Agent": "Mozilla/5.0 (compatible; ImpactGrid/1.0)" } }
    );
    const xml    = await response.text();
    const titles = [];
    const regex  = /<title><!\[CDATA\[([^\]]+)\]\]><\/title>/g;
    let match;
    while ((match = regex.exec(xml)) !== null) {
      if (match[1] !== "Google Trends" && match[1] !== "Daily Search Trends") {
        titles.push(match[1]);
      }
    }
    res.json({ geo, trends: titles.slice(0, 20), ts: new Date().toISOString() });
  } catch (err) {
    console.error("[Google Trends] Exception:", err.message);
    res.status(500).json({ error: "Failed to fetch Google Trends", details: err.message });
  }
});


/* ================================================================
   START SERVER
================================================================ */
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`ImpactGrid Dijo running on port ${PORT}`);

  // ── Register ingestion API routes ──
  addIngestionRoutes(app);

  // ── Start ingestion cron jobs ──
  startIngestion();
});
